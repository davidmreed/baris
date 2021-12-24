use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::json;

use crate::{api::SalesforceRequest, SalesforceError};

#[cfg(test)]
mod test;

pub struct ExecuteAnonymousApexRequest {
    anonymous_body: String,
}

impl ExecuteAnonymousApexRequest {
    pub fn new(anonymous_body: String) -> ExecuteAnonymousApexRequest {
        ExecuteAnonymousApexRequest { anonymous_body }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteAnonymousApexResponse {
    pub line: i64,
    pub column: i64,
    pub compiled: bool,
    pub success: bool,
    pub compile_problem: Option<String>,
    pub exception_stack_trace: Option<String>,
    pub exception_message: Option<String>,
}

impl SalesforceRequest for ExecuteAnonymousApexRequest {
    type ReturnValue = ExecuteAnonymousApexResponse;

    fn get_url(&self) -> String {
        "tooling/executeAnonymous".to_owned()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(
        &self,
        _conn: &crate::Connection,
        body: Option<&serde_json::Value>,
    ) -> anyhow::Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }

    fn get_body(&self) -> Option<serde_json::Value> {
        None
    }

    fn get_query_parameters(&self) -> Option<serde_json::Value> {
        Some(json!({"anonymousBody": self.anonymous_body}))
    }
}
