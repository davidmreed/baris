use reqwest::Method;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::{api::SalesforceRequest, rest::describe::SObjectDescribe};
use crate::{Connection, SObject, SObjectType, SalesforceError, SalesforceId};

use serde_derive::Deserialize;
use std::error::Error;
use std::fmt;

use anyhow::Result;

pub mod collections;
pub mod composite;
pub mod describe;
pub mod query;

// SObject Create Requests

#[derive(Debug, Deserialize)]
pub struct CreateResult {
    pub id: Option<SalesforceId>,
    pub errors: Option<Vec<String>>,
    pub success: bool,
    pub created: Option<bool>, // TODO: is this really part of upsert only?
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

pub struct SObjectCreateRequest<'a> {
    sobject: &'a mut SObject,
}

impl<'a> SObjectCreateRequest<'a> {
    pub fn new(sobject: &'a mut SObject) -> Result<Self> {
        if sobject.get_id().is_some() {
            return Err(SalesforceError::RecordExistsError.into());
        }

        Ok(Self { sobject })
    }
}

impl<'a> SalesforceRequest for SObjectCreateRequest<'a> {
    type ReturnValue = CreateResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!("sobjects/{}/", self.sobject.sobject_type.get_api_name())
    }

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn has_reference_parameters(&self) -> bool {
        self.sobject.has_reference_parameters()
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectCreateRequest<'a> {}

// Result structures for DML operations

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DmlError {
    pub fields: Vec<String>,
    pub message: String,
    pub error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum DmlResult {
    Success,
    Error(DmlError),
}

// TODO: can we implement `Try` instead?
impl Into<Result<()>> for DmlResult {
    fn into(self) -> Result<()> {
        if let DmlResult::Error(e) = self {
            Err(e.into())
        } else {
            Ok(())
        }
    }
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

pub struct SObjectUpdateRequest<'a> {
    sobject: &'a mut SObject,
}

impl<'a> SObjectUpdateRequest<'a> {
    pub fn new(sobject: &'a mut SObject) -> Result<SObjectUpdateRequest> {
        if sobject.get_id().is_none() {
            Err(SalesforceError::RecordDoesNotExistError.into())
        } else {
            Ok(SObjectUpdateRequest { sobject })
        }
    }
}

impl<'a> SalesforceRequest for SObjectUpdateRequest<'a> {
    type ReturnValue = ();

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}",
            self.sobject.sobject_type.get_api_name(),
            self.sobject.get_id().unwrap() // Cannot panic due to implementation of `new()`
        )
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        // This request returns 204 No Content on success.
        if let Some(body) = body {
            Err(serde_json::from_value::<DmlError>(body.clone())?.into())
        } else {
            Ok(())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectUpdateRequest<'a> {}

// SObject Upsert Requests

pub struct SObjectUpsertRequest<'a> {
    sobject: &'a mut SObject,
    external_id: String,
}

impl<'a> SObjectUpsertRequest<'a> {
    pub fn new(sobject: &'a mut SObject, external_id: &str) -> Result<SObjectUpsertRequest<'a>> {
        if sobject
            .sobject_type
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

impl<'a> SalesforceRequest for SObjectUpsertRequest<'a> {
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}/{}",
            self.sobject.sobject_type.get_api_name(),
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

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectUpsertRequest<'a> {}

// SObject Delete Requests

pub struct SObjectDeleteRequest<'a> {
    sobject: &'a mut SObject,
}

impl<'a> SObjectDeleteRequest<'a> {
    pub fn new(sobject: &'a mut SObject) -> Result<SObjectDeleteRequest> {
        if let Some(_) = sobject.get_id() {
            Ok(SObjectDeleteRequest { sobject })
        } else {
            Err(SalesforceError::RecordDoesNotExistError.into())
        }
    }
}

impl<'a> SalesforceRequest for SObjectDeleteRequest<'a> {
    type ReturnValue = ();

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}",
            self.sobject.sobject_type.get_api_name(),
            self.sobject.get_id().unwrap()
        )
    }

    fn get_method(&self) -> Method {
        Method::DELETE
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        // This request returns a 204 + empty body on success.
        if let Some(body) = body {
            Err(serde_json::from_value::<DmlError>(body.clone())?.into())
        } else {
            Ok(())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectDeleteRequest<'a> {}

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
        format!("sobjects/{}/{}/", self.sobject_type.get_api_name(), self.id)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(SObject::from_json(body, &self.sobject_type)?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl CompositeFriendlyRequest for SObjectRetrieveRequest {}
