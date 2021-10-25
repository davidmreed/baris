use reqwest::Method;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::{api::SalesforceRequest, data::SObjectDescribe};
use crate::{Connection, SObject, SObjectType, SalesforceError, SalesforceId};

use serde_derive::Deserialize;
use std::error::Error;
use std::fmt;

use anyhow::Result;

pub mod composite;
pub mod query;
// SObject Describe Requests

pub struct SObjectDescribeRequest {
    sobject: String,
}

impl SObjectDescribeRequest {
    pub fn new(sobject: &str) -> SObjectDescribeRequest {
        SObjectDescribeRequest {
            sobject: sobject.to_owned(),
        }
    }
}

impl SalesforceRequest for SObjectDescribeRequest {
    type ReturnValue = SObjectDescribe;

    fn get_url(&self) -> String {
        format!("/sobjects/{}/describe", self.sobject)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

// SObject Create Requests

#[derive(Debug, Deserialize)]
pub struct CreateResult {
    pub id: Option<String>,
    pub errors: Option<Vec<String>>,
    pub success: bool,
    pub created: Option<bool>,
}

impl fmt::Display for CreateResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.success {
            write!(f, "Success ({})", self.id.as_ref().unwrap())
        } else {
            write!(f, "DML error: {}", self.errors.as_ref().unwrap().join("\n"))
        }
    }
}

impl Error for CreateResult {}

pub struct SObjectCreateRequest {
    sobject: SObject,
}

impl SObjectCreateRequest {
    pub fn new(sobject: SObject) -> Result<Self> {
        if sobject.get_id().is_some() {
            return Err(SalesforceError::RecordExistsError().into());
        }

        Ok(Self { sobject })
    }
}

impl SalesforceRequest for SObjectCreateRequest {
    type ReturnValue = CreateResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!("/sobjects/{}/", self.sobject.sobjecttype.get_api_name())
    }

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn has_reference_parameters(&self) -> bool {
        self.sobject.has_reference_parameters()
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

impl CompositeFriendlyRequest for SObjectCreateRequest {}

// Result structures for DML operations

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DmlError {
    pub fields: Vec<String>,
    pub message: String,
    pub error_code: String,
}

#[derive(Debug, Deserialize)]
pub enum DmlResult {
    Success,
    Error(DmlError),
}

impl fmt::Display for DmlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DML error: {} ({}) on fields {}",
            self.error_code,
            self.message,
            self.fields.join("\n")
        )
    }
}

impl Error for DmlError {}

// SObject Update Requests

pub struct SObjectUpdateRequest {
    sobject: SObject,
}

impl SObjectUpdateRequest {
    pub fn new(sobject: SObject) -> Result<SObjectUpdateRequest> {
        if sobject.get_id().is_none() {
            Err(SalesforceError::RecordDoesNotExistError().into())
        } else {
            Ok(SObjectUpdateRequest { sobject })
        }
    }
}

impl SalesforceRequest for SObjectUpdateRequest {
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!(
            "/sobjects/{}/{}",
            self.sobject.sobjecttype.get_api_name(),
            self.sobject.get_id().unwrap() // Cannot panic due to implementation of `new()`
        )
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

impl CompositeFriendlyRequest for SObjectUpdateRequest {}

// SObject Upsert Requests

pub struct SObjectUpsertRequest {
    sobject: SObject,
    external_id: String,
}

impl SObjectUpsertRequest {
    pub fn new(sobject: SObject, external_id: &str) -> Result<SObjectUpsertRequest> {
        if sobject
            .sobjecttype
            .get_describe()
            .get_field(external_id)
            .is_none()
        {
            return Err(SalesforceError::SchemaError(format!(
                "Field {} does not exist.",
                external_id
            ))
            .into());
        }

        let field_value = sobject.get(external_id);
        if field_value.is_none() {
            return Err(SalesforceError::GeneralError(format!(
                "Cannot upsert without a field value."
            ))
            .into());
        } else {
            Ok(SObjectUpsertRequest {
                sobject,
                external_id: external_id.to_owned(),
            })
        }
    }
}

impl SalesforceRequest for SObjectUpsertRequest {
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!(
            "/sobjects/{}/{}/{}",
            self.sobject.sobjecttype.get_api_name(),
            self.sobject
                .get(&self.external_id)
                .unwrap() // will not panic via implementation of `new()`
                .as_string(),
            self.external_id
        )
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

impl CompositeFriendlyRequest for SObjectUpsertRequest {}

// SObject Delete Requests

pub struct SObjectDeleteRequest {
    sobject: SObject,
}

impl SObjectDeleteRequest {
    pub fn new(sobject: SObject) -> Result<SObjectDeleteRequest> {
        if let Some(_) = sobject.get_id() {
            Ok(SObjectDeleteRequest { sobject })
        } else {
            Err(SalesforceError::RecordDoesNotExistError().into())
        }
    }
}

impl SalesforceRequest for SObjectDeleteRequest {
    type ReturnValue = DmlResult;

    fn get_url(&self) -> String {
        format!(
            "/sobjects/{}/{}",
            self.sobject.sobjecttype.get_api_name(),
            self.sobject.get_id().unwrap()
        )
    }

    fn get_method(&self) -> Method {
        Method::DELETE
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

impl CompositeFriendlyRequest for SObjectDeleteRequest {}

// SObject Retrieve Requests

pub struct SObjectRetrieveRequest {
    id: SalesforceId,
    sobject_type: SObjectType,
}

impl SObjectRetrieveRequest {
    pub fn new(id: SalesforceId, sobject_type: &SObjectType) -> SObjectRetrieveRequest {
        SObjectRetrieveRequest {
            id,
            sobject_type: sobject_type.clone(),
        }
    }
}

impl SalesforceRequest for SObjectRetrieveRequest {
    type ReturnValue = SObject;

    fn get_url(&self) -> String {
        format!(
            "/sobjects/{}/{}/",
            self.sobject_type.get_api_name(),
            self.id
        )
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(SObject::from_json(body, &self.sobject_type)?)
    }
}

impl CompositeFriendlyRequest for SObjectRetrieveRequest {}
