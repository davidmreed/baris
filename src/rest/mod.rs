use crate::{SalesforceError, SalesforceId};

use serde_derive::Deserialize;
use std::error::Error;
use std::fmt;

use anyhow::Result;

pub mod collections;
pub mod composite;
pub mod describe;
pub mod query;
pub mod rows;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ApiError {
    pub message: String,
    // The sObject Rows endpoints use errorCode:
    // https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_upsert.htm
    pub error_code: Option<String>,
    // The sObject Collections endpoints use statusCode:
    // https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
    pub status_code: Option<String>,
}

impl ApiError {
    pub fn get_error_code(&self) -> Option<&String> {
        if self.error_code.is_some() {
            self.error_code.as_ref()
        } else {
            self.status_code.as_ref()
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let error = &"Unknown error".to_string();
        write!(
            f,
            "{} ({})",
            self.get_error_code().unwrap_or_else(|| error),
            self.message,
        )
    }
}

impl Error for ApiError {}

// Result structures for DML operations, shared across various APIs.
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DmlError {
    pub fields: Vec<String>,
    #[serde(flatten)]
    pub error: ApiError,
}

impl DmlError {
    pub fn get_error_code(&self) -> Option<&String> {
        self.error.get_error_code()
    }
}

#[derive(Debug, Deserialize)]
pub struct DmlResult {
    pub id: Option<SalesforceId>,
    pub created: Option<bool>,
    pub success: bool,
    pub errors: Vec<DmlError>,
}

impl Into<Result<SalesforceId>> for DmlResult {
    fn into(self) -> Result<SalesforceId> {
        if !self.success {
            if self.errors.len() > 0 {
                // TODO: handle multiple errors, if this ever happens.
                let err = self.errors[0].clone();
                Err(err.into())
            } else {
                Err(SalesforceError::UnknownError.into())
            }
        } else {
            if let Some(id) = self.id {
                Ok(id)
            } else {
                Err(SalesforceError::UnknownError.into()) // TODO: specificity
            }
        }
    }
}

impl Into<Result<Option<SalesforceId>>> for DmlResult {
    fn into(self) -> Result<Option<SalesforceId>> {
        if !self.success {
            if self.errors.len() > 0 {
                // TODO: handle multiple errors, if this ever happens.
                let err = self.errors[0].clone();
                Err(err.into())
            } else {
                Err(SalesforceError::UnknownError.into())
            }
        } else {
            Ok(self.id)
        }
    }
}

impl Into<Result<()>> for DmlResult {
    fn into(self) -> Result<()> {
        if !self.success {
            if self.errors.len() > 0 {
                // TODO: handle multiple errors, if this ever happens.
                Err(self.errors[0].clone().into())
            } else {
                Err(SalesforceError::UnknownError.into())
            }
        } else {
            Ok(())
        }
    }
}

impl fmt::Display for DmlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} on fields {}", self.error, self.fields.join("\n"))
    }
}

impl Error for DmlError {}
