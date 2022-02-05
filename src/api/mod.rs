extern crate reqwest;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use super::data::SObjectType;
use super::errors::SalesforceError;

use crate::auth::Authentication;
use crate::rest::describe::{SObjectDescribe, SObjectDescribeRequest};

use anyhow::{Error, Result};
use async_trait::async_trait;
use reqwest::{header, Body, Client, Method, RequestBuilder, Response, StatusCode, Url};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

#[cfg(test)]
mod test;

pub trait SalesforceRequest {
    type ReturnValue;

    fn get_body(&self) -> Option<Value> {
        None
    }

    fn get_url(&self) -> String;
    fn get_method(&self) -> Method;

    fn get_query_parameters(&self) -> Option<Value> {
        None
    }

    fn get_result(&self, conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue>;
}

#[async_trait]
pub(crate) trait SalesforceRawRequest {
    type ReturnValue;

    fn get_body(&self) -> Option<Body> {
        None
    }
    fn get_mime_type(&self) -> String {
        "text/json".to_owned()
    }

    fn get_url(&self) -> String;
    fn get_method(&self) -> Method;

    fn get_query_parameters(&self) -> Option<Value> {
        None
    }

    async fn get_result(&self, conn: &Connection, response: Response) -> Result<Self::ReturnValue>;
}

pub trait CompositeFriendlyRequest: SalesforceRequest {}

pub struct ConnectionBody {
    pub(crate) api_version: String,
    sobject_types: RwLock<HashMap<String, SObjectType>>,
    auth: RwLock<Box<dyn Authentication>>,
    auth_refresh: Mutex<()>,
    auth_global_lock: Mutex<()>,
}

pub struct Connection(Arc<ConnectionBody>);

impl Deref for Connection {
    type Target = ConnectionBody;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for Connection {
    fn clone(&self) -> Self {
        Connection(Arc::clone(&self.0))
    }
}

impl Connection {
    pub fn new(auth: Box<dyn Authentication>, api_version: &str) -> Result<Connection> {
        Ok(Connection(Arc::new(ConnectionBody {
            api_version: api_version.to_string(),
            sobject_types: RwLock::new(HashMap::new()),
            auth: RwLock::new(auth),
            auth_refresh: Mutex::new(()),
            auth_global_lock: Mutex::new(()),
        })))
    }

    pub async fn get_instance_url(&self) -> Result<Url> {
        if self.get_current_access_token().await.is_none() {
            // We haven't done an initial token refresh yet, so we may not have
            // the right instance_url set.
            self.refresh_access_token().await?;
        }

        let lock = self.auth.read().await;

        Ok(lock.get_instance_url().await?.clone())
        // TODO: this is why the refresh is in this method (above )
    }

    pub async fn get_base_url(&self) -> Result<Url> {
        Ok(self
            .get_instance_url()
            .await?
            .join(&self.get_base_url_path())?)
    }

    pub fn get_base_url_path(&self) -> String {
        format!("/services/data/{}/", self.api_version)
    }

    pub async fn get_access_token(&self) -> Result<String> {
        let tok = self.get_current_access_token().await;

        if let Some(tok) = tok {
            Ok(tok)
        } else {
            self.refresh_access_token().await?;
            self.get_current_access_token()
                .await
                .ok_or_else(|| SalesforceError::CannotRefresh.into()) // Right error?
        }
    }

    async fn get_current_access_token(&self) -> Option<String> {
        let access_token = self.auth.read().await;

        access_token.get_access_token().cloned()
    }

    pub async fn refresh_access_token(&self) -> Result<()> {
        // First, obtain the global auth mutex so that our interactions
        // with the two subsidiary locks are atomic.
        let global_auth_handle = self.auth_global_lock.lock().await;

        // Attempt to obtain the Mutex that gates a refresh process.
        let auth_permission_handle = self.auth_refresh.try_lock();
        // If we got the mutex, also get a write lock on AuthDetails.
        let auth_lock = if auth_permission_handle.is_ok() {
            // We got the mutex lock, which means we should actually process the refresh.
            Some(self.auth.write().await)
        } else {
            None
        };

        // Now that we know our situation, drop the global auth handle.
        drop(global_auth_handle);

        // If we are the task that will be performing this refresh, do so.
        if auth_permission_handle.is_ok() {
            auth_lock.unwrap().refresh_access_token().await?;
        } else {
            // We didn't get the mutex lock, which means someone else is running the operation,
            // and we do not have a write lock on the auth details.
            // Await on a read lock on our AuthDetails. Via the mutex above,
            // we guarantee that the updating task also has a write lock on AuthDetails.
            self.auth.read().await;
        }

        Ok(())
    }

    pub async fn get_type(&self, type_name: &str) -> Result<SObjectType> {
        let mut sobject_types = self.sobject_types.write().await;

        if !sobject_types.contains_key(type_name) {
            // Pull the Describe information for this sObject
            let describe: SObjectDescribe = self
                .execute(&SObjectDescribeRequest::new(type_name))
                .await?;
            sobject_types.insert(
                type_name.to_string(),
                SObjectType::new(type_name.to_string(), describe),
            );
        }
        let sobject_types = sobject_types.downgrade();

        match sobject_types.get(type_name) {
            Some(rc) => Ok(rc.clone()), // TODO: Is this correct?
            None => Err(Error::new(SalesforceError::GeneralError(
                "sObject Type not found".to_string(),
            ))),
        }
    }

    pub async fn get_client(&self) -> Result<Client> {
        // TODO: it is more efficient to cache the client for connection pooling.
        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", self.get_access_token().await?))?,
        );

        Ok(Client::builder().default_headers(headers).build()?)
    }

    async fn build_request<K>(&self, request: &K) -> Result<RequestBuilder>
    where
        K: SalesforceRequest,
    {
        let url = self.get_base_url().await?.join(&request.get_url())?;

        let mut builder = self.get_client().await?.request(request.get_method(), url);

        let method = request.get_method();

        if method == Method::POST || method == Method::PUT || method == Method::PATCH {
            if let Some(body) = request.get_body() {
                builder = builder.json(&body);
            }
        }

        if let Some(params) = request.get_query_parameters() {
            builder = builder.query(&params);
        }

        Ok(builder)
    }

    // The following violates DRY but is challenging to express due to the two-trait structure.
    // TODO: figure out how to do a blanket impl of SalesforceRawRequest for SalesforceRequest
    // without impacting the external-facing API.

    async fn build_raw_request<K>(&self, request: &K) -> Result<RequestBuilder>
    where
        K: SalesforceRawRequest,
    {
        let url = self.get_base_url().await?.join(&request.get_url())?;

        let mut builder = self.get_client().await?.request(request.get_method(), url);

        let method = request.get_method();

        if method == Method::POST || method == Method::PUT || method == Method::PATCH {
            if let Some(body) = request.get_body() {
                builder = builder.body(body);
            }
        }

        builder = builder.header(reqwest::header::CONTENT_TYPE, request.get_mime_type());

        if let Some(params) = request.get_query_parameters() {
            builder = builder.query(&params);
        }

        Ok(builder)
    }

    pub(crate) async fn execute_raw_request<K, T>(&self, request: &K) -> Result<T>
    where
        K: SalesforceRawRequest<ReturnValue = T>,
    {
        let mut result = self.build_raw_request(request).await?.send().await?;

        // If the token is expired, refresh it and try again.
        if result.status().as_u16() == 401 {
            self.refresh_access_token().await?;
            result = self.build_raw_request(request).await?.send().await?
        }
        result = result.error_for_status()?;

        request.get_result(self, result).await
    }

    pub async fn execute<K, T>(&self, request: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
    {
        let mut result = self.build_request(request).await?.send().await?;

        // If the token is expired, refresh it and try again.
        if result.status().as_u16() == 401 {
            self.refresh_access_token().await?;
            result = self.build_request(request).await?.send().await?
        }

        // TODO: we don't consume any error details returned in the case of a 400.
        result = result.error_for_status()?;

        if result.status() == StatusCode::NO_CONTENT {
            Ok(request.get_result(self, None)?)
        } else {
            Ok(request.get_result(self, Some(&result.json().await?))?)
        }
    }
}
