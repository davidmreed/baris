use std::collections::HashMap;

use anyhow::Result;
use reqwest::Method;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    api::Connection,
    api::{CompositeFriendlyRequest, SalesforceRequest},
    errors::SalesforceError,
};

use super::ApiError;

#[cfg(test)]
mod test;

pub struct CompositeRequest {
    keys: Vec<String>,
    requests: HashMap<String, CompositeSubrequest>,
    all_or_none: Option<bool>, // TODO: Option<Option<bool>>, to allow them to be unspecified?
    collate_subrequests: Option<bool>,
    base_url: String,
}

impl CompositeRequest {
    pub fn new(
        base_url: String,
        all_or_none: Option<bool>,
        collate_subrequests: Option<bool>,
    ) -> CompositeRequest {
        CompositeRequest {
            requests: HashMap::new(),
            keys: Vec::new(),
            all_or_none,
            collate_subrequests,
            base_url,
        }
    }

    pub fn add(
        &mut self,
        key: &str,
        req: &(impl SalesforceRequest + CompositeFriendlyRequest),
    ) -> Result<()> {
        self.keys.push(key.to_string());

        let query_string = if let Some(params) = req.get_query_parameters() {
            format!("?{}", serde_urlencoded::to_string(&params)?)
        } else {
            "".to_owned()
        };

        self.requests.insert(
            key.to_string(),
            CompositeSubrequest {
                url: format!("{}{}{}", self.base_url, req.get_url(), query_string),
                body: req.get_body(),
                method: req.get_method().to_string(),
                reference_id: Some(key.to_string()),
                http_headers: None,
            },
        );

        Ok(())
    }
}

impl SalesforceRequest for CompositeRequest {
    type ReturnValue = CompositeResponse;

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
            composite_request: Vec::with_capacity(self.keys.len()),
        };

        for k in self.keys.iter() {
            let req = self.requests.get(k).unwrap();
            body.composite_request.push(req.clone()); // TODO: don't clone.
        }

        serde_json::to_value(body).ok()
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value(body.clone())?) // TODO: don't clone
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
    composite_request: Vec<CompositeSubrequest>,
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompositeResponse {
    pub composite_response: Vec<CompositeSubrequestResponse>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum CompositeSubrequestResponseBody {
    Error(Vec<ApiError>),
    Success(Option<Value>),
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompositeSubrequestResponse {
    body: CompositeSubrequestResponseBody,
    http_headers: HashMap<String, String>,
    http_status_code: u16,
    reference_id: String,
}

impl CompositeResponse {
    pub fn get_result_value(&self, key: &str) -> Option<&CompositeSubrequestResponse> {
        // TODO: cache a HashMap
        let matches: Vec<&CompositeSubrequestResponse> = self
            .composite_response
            .iter()
            .filter(|s| s.reference_id == key)
            .collect();

        if matches.len() > 0 {
            Some(matches[0])
        } else {
            None
        }
    }

    pub fn get_result<K, T>(&self, conn: &Connection, key: &str, req: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
    {
        let subrequest_response =
            self.get_result_value(key)
                .ok_or(SalesforceError::GeneralError(
                    "Subrequest key does not exist".into(),
                ))?;

        match &subrequest_response.body {
            // TODO: handle multiple errors returned.
            CompositeSubrequestResponseBody::Error(errs) => Err(errs[0].clone().into()),
            CompositeSubrequestResponseBody::Success(Some(body)) => {
                req.get_result(conn, Some(&body))
            }
            CompositeSubrequestResponseBody::Success(None) => req.get_result(conn, None),
        }

        // TODO: what does the response body look like for a composite request that includes a 201-result subrequest?
    }
}
