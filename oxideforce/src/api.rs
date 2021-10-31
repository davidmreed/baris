extern crate reqwest;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use std::collections::HashMap;
use std::mem;
use std::ops::Deref;
use std::sync::Arc;

use super::data::{SObjectDescribe, SObjectType};
use super::errors::SalesforceError;

use crate::rest::SObjectDescribeRequest;

use anyhow::{Error, Result};
use reqwest::{header, Client, Method, Url};
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

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

    fn has_reference_parameters(&self) -> bool {
        false
    }

    fn get_result(&self, conn: &Connection, body: &Value) -> Result<Self::ReturnValue>;
}

pub trait CompositeFriendlyRequest: SalesforceRequest {}

struct ConnectedApp {
    consumer_key: String,
    client_secret: String,
    redirect_url: Url,
}

impl ConnectedApp {
    pub fn new(consumer_key: String, client_secret: String, redirect_url: Url) -> ConnectedApp {
        ConnectedApp {
            consumer_key,
            client_secret,
            redirect_url,
        }
    }
}

struct RefreshTokenAuth {
    refresh_token: String,
    access_token: Option<String>,
    app: ConnectedApp,
}

impl RefreshTokenAuth {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        Ok(())
    }
}

struct JwtAuth {
    access_token: Option<String>,
    app: ConnectedApp,
    cert: String,
}

impl JwtAuth {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        Ok(())
    }
}

enum AuthDetails {
    AccessToken(String),
    RefreshToken(RefreshTokenAuth),
    Jwt(JwtAuth),
}

impl AuthDetails {
    pub async fn refresh_access_token(&mut self) -> Result<()> {
        match self {
            AuthDetails::RefreshToken(tok) => tok.refresh_access_token(),
            AuthDetails::Jwt(tok) => tok.refresh_access_token(),
            AuthDetails::AccessToken(_) => Err(SalesforceError::CannotRefresh().into()),
        }
    }

    pub fn get_access_token(&self) -> Option<String> {
        match self {
            AuthDetails::RefreshToken(tok) => tok.access_token,
            AuthDetails::Jwt(tok) => tok.access_token,
            AuthDetails::AccessToken(tok) => Some(tok),
        }
    }
}

pub struct ConnectionBody {
    pub(crate) instance_url: String,
    pub(crate) api_version: String,
    sobject_types: RwLock<HashMap<String, SObjectType>>,
    pub(crate) client: Client,
    auth: RwLock<AuthDetails>,
    auth_refresh: RwLock<Option<JoinHandle<()>>>,
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
        Connection {
            0: Arc::clone(&self.0),
        }
    }
}

impl Connection {
    pub fn new(sid: &str, instance_url: &str, api_version: &str) -> Result<Connection> {
        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", sid))?,
        );

        Ok(Connection {
            0: Arc::new(ConnectionBody {
                api_version: api_version.to_string(),
                instance_url: instance_url.to_string(),
                sobject_types: RwLock::new(HashMap::new()),
                client: Client::builder().default_headers(headers).build()?,
            }),
        })
    }

    pub fn get_base_url(&self) -> String {
        format!("{}services/data/{}", self.instance_url, self.api_version)
    }

    pub async fn get_access_token(&mut self) -> Result<String> {
        // The sequence for auth token access goes like this:
        // Attempt to get a lock on the auth structure.
        // If this lock blocks, that means another thread is already
        // attempting to refresh the token.
        // If we obtain the lock and there is no token, attempt
        // to start a token refresh, then recurse.
        let access_token = self.auth.read().await;

        let tok = access_token.get_access_token();
        mem::drop(access_token);

        if let Some(tok) = tok {
            Ok(tok)
        } else {
            self.refresh_access_token().await?;
            self.get_access_token().await
        }
    }

    pub async fn refresh_access_token(&mut self) -> Result<()> {
        // First, try to get a write lock on the auth struct.
        // If we do not get the lock, get a read lock on the auth handle
        // so that we can await until the update is complete.
        // We don't want to do `write().await` because it would
        // build up a queue of write lock contenders when this
        // operation only needs to happen once.

        // If we do get the lock, populate the auth handle with
        // a Future.
        let auth = self.auth.try_write(); // ... but what if someone has a read lock?

        if let Some(handle) = auth {
            // Enqueue and await on the OAuth refresh.
            self.auth_handle = Some(spawn(async {
                self.auth.refresh_access_token().await
            }));

            self.auth_handle.await?;
            self.auth_handle = None;
        } else {
            // Someone else is running the refresh.
            let lock = self.auth_handle.read().await;

            self.auth_handle.await?;
        }
    }

    pub async fn get_type(&self, type_name: &str) -> Result<SObjectType> {
        // TODO: can we be clever here to reduce lock contention?
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

        match sobject_types.get(type_name) {
            Some(rc) => Ok(rc.clone()), // TODO: Is this correct?
            None => Err(Error::new(SalesforceError::GeneralError(
                "sObject Type not found".to_string(),
            ))),
        }
    }

    pub async fn execute<K, T>(&self, request: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
    {
        let url = format!("{}{}", self.get_base_url(), request.get_url());
        println!("I have URL {}", url);
        let mut builder = self.client.request(request.get_method(), &url);
        let method = request.get_method();

        if method == Method::POST || method == Method::PUT || method == Method::PATCH {
            if let Some(body) = request.get_body() {
                builder = builder.json(&body);
            }
        }

        if let Some(params) = request.get_query_parameters() {
            builder = builder.query(&params);
        }

        let result = builder.send().await?.json().await?;
        // TODO: interpret common errors here, such as not found and access token expired.

        println!("I received {:?}", result);

        Ok(request.get_result(&self, &result)?)
    }
}
