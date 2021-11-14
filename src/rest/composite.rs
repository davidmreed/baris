use std::collections::HashMap;

use anyhow::Result;
use reqwest::Method;
use serde_derive::Serialize;
use serde_json::Value;

use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    Connection, SalesforceError,
};

pub struct CompositeRequest {
    // TODO: make this struct directly serialize.
    // TODO: use builder pattern.
    keys: Vec<String>,
    requests: HashMap<String, CompositeSubrequest>,
    results: HashMap<String, Value>,
    all_or_none: Option<bool>, // TODO: Option<Option<bool>>, to allow them to be unspecified?
    collate_subrequests: Option<bool>,
}

impl CompositeRequest {
    pub fn new(all_or_none: Option<bool>, collate_subrequests: Option<bool>) -> CompositeRequest {
        CompositeRequest {
            requests: HashMap::new(),
            results: HashMap::new(),
            keys: Vec::new(),
            all_or_none,
            collate_subrequests,
        }
    }

    pub fn add(&mut self, key: &str, req: &(impl SalesforceRequest + CompositeFriendlyRequest)) {
        // TODO: support query parameters
        self.keys.push(key.to_string());
        self.requests.insert(
            key.to_string(),
            CompositeSubrequest {
                url: req.get_url(),
                body: req.get_body(),
                method: req.get_method().to_string(),
                reference_id: Some(key.to_string()),
                http_headers: None,
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
        // TODO: what does the response body look like for a composite request that includes a 204-result subrequest?
        req.get_result(
            conn,
            Some(
                self.get_result_value(key)
                    .ok_or(SalesforceError::GeneralError(
                        "Key does not exist".to_string(),
                    ))?,
            ),
        )
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
        let mut body = CompositeRequestBody {
            all_or_none: self.all_or_none,
            collate_subrequests: self.collate_subrequests,
            composite_requests: Vec::with_capacity(self.keys.len()),
        };

        for k in self.keys.iter() {
            let req = self.requests.get(k).unwrap();
            body.composite_requests.push(req.clone()); // TODO: don't clone.
        }

        serde_json::to_value(body).ok()
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(body.clone()) // TODO: don't clone
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompositeRequestBody {
    all_or_none: Option<bool>,
    collate_subrequests: Option<bool>,
    composite_requests: Vec<CompositeSubrequest>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CompositeSubrequest {
    method: String,
    url: String,
    body: Option<Value>,
    reference_id: Option<String>,
    http_headers: Option<HashMap<String, String>>,
}
