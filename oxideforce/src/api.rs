extern crate reqwest;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use super::data::{SObjectDescribe, SObjectType};
use super::errors::SalesforceError;

use crate::rest::SObjectDescribeRequest;

use anyhow::{Error, Result};
use reqwest::{header, Client, Method};
use serde_json::Value;
use tokio::sync::RwLock;

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

pub trait CompositeFriendlyRequest {}

pub struct ConnectionBody {
    pub(crate) instance_url: String,
    pub(crate) api_version: String,
    sobject_types: RwLock<HashMap<String, SObjectType>>,
    pub(crate) client: Client,
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
