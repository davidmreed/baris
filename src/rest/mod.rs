use reqwest::Method;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::{api::SalesforceRequest, data::SObjectDescribe};
use crate::{Connection, SObject, SObjectType, SalesforceError, SalesforceId};

use serde_derive::Deserialize;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;

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
}

// SObject Create Requests

#[derive(Debug, Deserialize)]
struct CreateResult {
    id: String,
    errors: Vec<String>,
    success: bool,
    created: Option<bool>,
}

impl fmt::Display for CreateResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.success {
            write!(f, "Success ({})", self.id)
        } else {
            write!(f, "DML error: {}", self.errors.join("\n"))
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
}

impl CompositeFriendlyRequest for SObjectCreateRequest {}

// Result structures for DML operations

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DmlError {
    fields: Vec<String>,
    message: String,
    error_code: String,
}

#[derive(Debug, Deserialize)]
enum DmlResult {
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

struct SObjectUpdateRequest {
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
                .unwrap()
                .into::<String>(), // will not panic via implementation of `new()`
            self.external_id
        )
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }
}

impl CompositeFriendlyRequest for SObjectUpsertRequest {}

// SObject Delete Requests

struct SObjectDeleteRequest {
    sobject: SObject,
}

impl SObjectDeleteRequest {
    fn new(sobject: SObject) -> Result<SObjectDeleteRequest> {
        if let Some(id) = sobject.get_id() {
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
}

impl CompositeFriendlyRequest for SObjectDeleteRequest {}

// SObject Retrieve Requests

pub struct SObjectRetrieveRequest {
    id: SalesforceId,
    sobject_type: Arc<SObjectType>,
}

impl SObjectRetrieveRequest {
    fn new(id: SalesforceId, sobject_type: &Arc<SObjectType>) -> SObjectRetrieveRequest {
        SObjectRetrieveRequest {
            id,
            sobject_type: Arc::clone(sobject_type),
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

    fn get_result<SObject>(&self, conn: &Connection, body: &Value) -> Result<SObject> {
        Ok(SObject::from_json(body, &self.sobject_type)?)
    }
}

impl CompositeFriendlyRequest for SObjectRetrieveRequest {}
