use std::{error::Error, fmt::Display};

use anyhow::Result;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::json;

use crate::{api::SalesforceRequest, Connection, SalesforceError};

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

impl Display for ExecuteAnonymousApexResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.compiled && self.success {
            write!(f, "Anonymous Apex succeeded")?;
        } else if self.compiled && !self.success {
            write!(
                f,
                "Anonymous Apex failed: {}\n{}",
                self.exception_message.as_ref().unwrap_or(&"".to_owned()),
                self.exception_stack_trace
                    .as_ref()
                    .unwrap_or(&"".to_owned())
            )?;
        } else if !self.compiled {
            write!(
                f,
                "Anonymous Apex failed to compile: {}",
                self.compile_problem.as_ref().unwrap_or(&"".to_owned()),
            )?;
        }

        Ok(())
    }
}

impl Error for ExecuteAnonymousApexResponse {}

impl Into<Result<(), anyhow::Error>> for ExecuteAnonymousApexResponse {
    fn into(self) -> Result<(), anyhow::Error> {
        if self.compiled && self.success {
            Ok(())
        } else {
            Err(self.into())
        }
    }
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

impl Connection {
    pub async fn execute_anonymous(&self, anonymous_body: String) -> Result<()> {
        self.execute(&ExecuteAnonymousApexRequest::new(anonymous_body))
            .await?
            .into()
    }
}
