use reqwest::Method;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::api::SalesforceRequest;
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

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DmlError {
    pub fields: Vec<String>,
    pub message: String,
    // The sObject Rows endpoints use errorCode:
    // https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_upsert.htm
    pub error_code: Option<String>,
    // The sObject Collections endpoints use statusCode:
    // https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
    pub status_code: Option<String>,
}

impl DmlError {
    pub fn get_error_code(&self) -> Option<&String> {
        if self.error_code.is_some() {
            self.error_code.as_ref()
        } else {
            self.status_code.as_ref()
        }
    }
}

// TODO: replace CreateResult with this struct
// TODO: review sObject Rows resources to see which really return this struct.
#[derive(Debug, Deserialize)]
pub struct DmlResultWithId {
    pub id: Option<SalesforceId>,
    pub created: Option<bool>,
    pub success: bool,
    pub errors: Vec<DmlError>,
}

impl Into<Result<Option<SalesforceId>>> for DmlResultWithId {
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

impl Into<Result<()>> for DmlResultWithId {
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
        let error = &"Unknown error".to_string();
        write!(
            f,
            "DML error: {} ({}) on fields {}",
            self.get_error_code().unwrap_or_else(|| error),
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
        Some(self.sobject.to_json_without_id()) // FIXME: including the Id is probably what's causing the 400 here.
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
// TODO: note unique return semantics at
// https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
// There is an API version change around response struct and HTTP code.
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

// TODO: support optional Fields query parameter
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
