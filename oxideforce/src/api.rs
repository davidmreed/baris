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

use crate::auth::AuthDetails;
use crate::rest::SObjectDescribeRequest;

use anyhow::{Error, Result};
use reqwest::{header, Client, Method};
use serde_json::Value;
use tokio::spawn;
use tokio::sync::{broadcast, Mutex, RwLock};

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

pub struct ConnectionBody {
    pub(crate) api_version: String,
    sobject_types: RwLock<HashMap<String, SObjectType>>,
    auth: RwLock<AuthDetails>,
    auth_refresh: Mutex<Option<Error>>,
    auth_refresh_channel: RwLock<Option<broadcast::Sender<Option<AuthDetails>>>>,
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
    pub fn new(auth: AuthDetails, api_version: &str) -> Result<Connection> {
        Ok(Connection {
            0: Arc::new(ConnectionBody {
                api_version: api_version.to_string(),
                sobject_types: RwLock::new(HashMap::new()),
                auth: RwLock::new(auth),
                auth_refresh: Mutex::new(None),
                auth_refresh_channel: RwLock::new(None),
            }),
        })
    }

    pub async fn get_base_url(&self) -> String {
        let lock = self.auth.read().await;
        format!(
            "{}services/data/{}",
            lock.get_instance_url(),
            self.api_version
        )
    }

    pub async fn get_access_token(&self) -> Result<String> {
        // Atomicity.
        // When we request an access token, we should either:

        // a. receive an access token that is currently believed to be valid.
        // b. await until an in-process token refresh completes, and then
        // receive the new token.

        // Calls to `refresh_access_token()` should not be _automatically_
        // accumulated if a refresh is already in progress, based on token-expired
        // API responses. If the user explicitly calls `refresh_access_token()` more
        // than once, that's okay.

        // This means we need two locks, and they need to be acquired and released
        // in a very specific order:
        // When we start a refresh operation, we need to get _BOTH_ a write lock
        // on the auth details structure (blocking reads),
        // _AND_ a lock on the mutex. If we fail to get a lock on the mutex,
        // that means someone else started a refresh while we weren't looking,
        // and we should just await a read lock on the auth details.

        // When we complete a refresh operation, we need to release the locks
        // in the opposite order in which we obtained them, which prevents a
        // race condition. First we drop the lock on the mutex, then on the
        // auth details. This ensures that no one else will obtain the mutex
        // lock while we are busy updating the auth and start another refresh -
        // other clients will awake on the auth details first.

        let tok = self.get_current_access_token().await;

        if let Some(tok) = tok {
            Ok(tok)
        } else {
            self.refresh_access_token().await?;
            self.get_current_access_token()
                .await
                .ok_or(SalesforceError::CannotRefresh.into()) // Right error?
        }
    }

    async fn get_current_access_token(&self) -> Option<String> {
        let access_token = self.auth.read().await;

        access_token
            .get_access_token()
            .and_then(|s| Some(s.clone()))
    }

    pub async fn refresh_access_token(&self) -> Result<()> {
        let auth_handle = self.auth_refresh.try_lock();

        if let Ok(_) = auth_handle {
            // We got the mutex lock, which means we should actually process the refresh.
            let auth = self.auth.read().await;
            let mut cloned_auth = auth.clone();

            let (tx, _) = broadcast::channel(1);
            let mut channel_lock = self.auth_refresh_channel.write().await;
            *channel_lock = Some(tx.clone());

            spawn(async move {
                let result = cloned_auth.refresh_access_token().await;
                if let Err(e) = result {
                    tx.send(None);
                } else {
                    tx.send(Some(cloned_auth));
                }
                // Don't care about sending errors if there is no client waiting. TODO: this is wrong.
                // TODO: what if we use a oneshot channel to THIS task,
                // and use the locks to have other tasks await?
            });
        }

        // Regardless of who got the lock, there should be a channel in `auth_refresh_channel`.
        // Await on it.
        let channel_lock = self.auth_refresh_channel.read().await;

        if let Some(handle) = &*channel_lock {
            let mut receiver = handle.subscribe();
            // TODO: is there a race condition if the refresh task completes before we establish _any_ subscriber?
            let outcome = receiver.recv().await;

            if let Ok(r) = outcome {
                if let Ok(_) = auth_handle {
                    let mut auth = self.auth.write().await;
                    *auth = r?;
                }
            }
            // If receiving failed, just fall through - that means that the refresh
            // completed before we subscribed, and the sender was dropped.
        }
        mem::drop(channel_lock);

        if let Ok(_) = auth_handle {
            let mut channel_lock = self.auth_refresh_channel.write().await;
            *channel_lock = None;
        }

        Ok(())
    }

    pub async fn get_type(&mut self, type_name: &str) -> Result<SObjectType> {
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

    pub async fn execute<K, T>(&self, request: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
    {
        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", self.get_access_token().await?))?,
        );

        let url = format!("{}{}", self.get_base_url().await, request.get_url());
        println!("I have URL {}", url);
        let mut builder = self
            .client
            .request(request.get_method(), &url)
            .headers(headers);

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
