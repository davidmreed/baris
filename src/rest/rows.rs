use anyhow::Result;
use reqwest::Method;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::api::SalesforceRequest;
use crate::{Connection, FieldValue, SObject, SObjectType, SalesforceError, SalesforceId};

use super::DmlError;
use super::{DmlResult, DmlResultWithId};

// SObject class implementation

impl SObject {
    pub async fn create(&mut self, conn: &Connection) -> Result<()> {
        let request = SObjectCreateRequest::new(self)?;
        let result = conn.execute(&request).await?;

        if result.success {
            self.set_id(result.id.unwrap());
        }
        result.into()
    }

    pub async fn update(&mut self, conn: &Connection) -> Result<()> {
        conn.execute(&SObjectUpdateRequest::new(self)?).await
    }

    pub async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()> {
        conn.execute(&SObjectUpsertRequest::new(self, external_id)?)
            .await?
            .into()
    }

    pub async fn delete(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&SObjectDeleteRequest::new(self)?).await;

        if let Ok(_) = &result {
            self.put("id", FieldValue::Null);
        }

        result
    }

    pub async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
    ) -> Result<SObject> {
        conn.execute(&SObjectRetrieveRequest::new(id, sobject_type))
            .await
    }
}

// SObject Create Requests

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
    type ReturnValue = DmlResultWithId;

    fn get_body(&self) -> Option<Value> {
        Some(self.sobject.to_json())
    }

    fn get_url(&self) -> String {
        format!("sobjects/{}/", self.sobject.sobject_type.get_api_name())
    }

    fn get_method(&self) -> Method {
        Method::POST
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
