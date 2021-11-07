use std::collections::HashMap;

use anyhow::Result;
use reqwest::Method;
use serde_json::{json, Value};

use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    Connection,
};

struct ReifiedRequest {
    url: String,
    body: Option<Value>,
    method: Method,
}

pub struct CompositeRequest {
    keys: Vec<String>,
    requests: HashMap<String, ReifiedRequest>,
    results: HashMap<String, Value>,
}

impl CompositeRequest {
    pub fn new() -> CompositeRequest {
        CompositeRequest {
            requests: HashMap::new(),
            results: HashMap::new(),
            keys: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &str, req: &(impl SalesforceRequest + CompositeFriendlyRequest)) {
        // TODO: support query parameters
        self.requests.insert(
            key.to_string(),
            ReifiedRequest {
                url: req.get_url(),
                body: req.get_body(),
                method: req.get_method(),
            },
        );
    }

    pub fn get_result_value(&self, key: &str) -> Option<&Value> {
        self.results.get(key)
    }

    pub fn get_result<K, T>(&self, conn: &Connection, key: &str, req: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
    {
        req.get_result(conn, self.get_result_value(key).ok_or(FooError)?)
    }
}

impl SalesforceRequest for CompositeRequest {
    type ReturnValue = Value;

    fn get_url(&self) -> String {
        "composite".to_string()
    }

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn get_body(&self) -> Option<Value> {
        json!()
    }

    fn get_result(&self, conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        todo!()
    }
}

struct CompositeRequestBody {
    all_or_none: Option<bool>,
    collate_subrequests: Option<bool>,
    composite_request: Vec<CompositeSubrequest>,
}

struct CompositeSubrequest {
    method: String,
    url: String,
    body: Value,
    reference_id: Option<String>,
    http_headers: HashMap<String, String>,
}
