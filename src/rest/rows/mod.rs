use std::marker::PhantomData;
use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use reqwest::Method;
use reqwest::Response;
use serde_json::Map;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::api::SalesforceRawRequest;
use crate::api::SalesforceRequest;
use crate::data::FieldValue;
use crate::data::SObjectDeserialization;
use crate::data::SObjectRepresentation;
use crate::data::SObjectSerialization;
use crate::data::SObjectWithId;
use crate::data::TypedSObject;
use crate::{api::Connection, data::SObjectType, data::SalesforceId, errors::SalesforceError};

use super::DmlError;
use super::DmlResult;

pub mod traits;

#[cfg(test)]
mod test;

// SObject Create Requests

pub struct SObjectCreateRequest {
    body: Value,
    api_name: String,
}

impl SObjectCreateRequest {
    pub fn new_raw(body: Value, api_name: String) -> SObjectCreateRequest {
        SObjectCreateRequest { body, api_name }
    }

    pub fn new<T>(sobject: &T) -> Result<Self>
    where
        T: SObjectSerialization + SObjectWithId + TypedSObject,
    {
        match sobject.get_id() {
            FieldValue::Null => {}
            FieldValue::Id(_) | FieldValue::CompositeReference(_) => {
                return Err(SalesforceError::RecordExistsError.into())
            }
            _ => {
                return Err(SalesforceError::InvalidIdError(format!(
                    "{:?} is not a valid SObject Id",
                    sobject.get_id()
                ))
                .into())
            }
        }

        Ok(Self {
            body: sobject.to_value_with_options(false, false)?,
            api_name: sobject.get_api_name().to_owned(),
        })
    }
}

impl SalesforceRequest for SObjectCreateRequest {
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.body.clone()) // TODO: do not clone
    }

    fn get_url(&self) -> String {
        format!("sobjects/{}/", self.api_name)
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

impl CompositeFriendlyRequest for SObjectCreateRequest {}

// SObject Update Requests

pub struct SObjectUpdateRequest {
    body: Value,
    api_name: String,
    id: String,
}

impl SObjectUpdateRequest {
    pub fn new_raw(body: Value, api_name: String, id: String) -> SObjectUpdateRequest {
        SObjectUpdateRequest { body, api_name, id }
    }

    pub fn new<T>(sobject: &T) -> Result<Self>
    where
        T: SObjectSerialization + SObjectWithId + TypedSObject,
    {
        match sobject.get_id() {
            FieldValue::Null => return Err(SalesforceError::RecordDoesNotExistError.into()),
            FieldValue::Id(_) | FieldValue::CompositeReference(_) => {}
            _ => {
                return Err(SalesforceError::InvalidIdError(format!(
                    "{:?} is not a valid SObject Id",
                    sobject.get_id()
                ))
                .into())
            }
        }

        Ok(Self::new_raw(
            sobject.to_value_with_options(false, false)?,
            sobject.get_api_name().to_owned(),
            sobject.get_id().as_string(),
        ))
    }
}

impl SalesforceRequest for SObjectUpdateRequest {
    type ReturnValue = ();

    fn get_body(&self) -> Option<Value> {
        Some(self.body.clone()) // TODO: do not clone
    }

    fn get_url(&self) -> String {
        format!("sobjects/{}/{}", self.api_name, self.id)
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

impl CompositeFriendlyRequest for SObjectUpdateRequest {}

// SObject Upsert Requests
pub struct SObjectUpsertRequest {
    body: Value,
    api_name: String,
    external_id: String,
    external_id_value: String,
}

impl SObjectUpsertRequest {
    pub fn new_raw(
        body: Value,
        api_name: String,
        external_id: String,
        external_id_value: String,
    ) -> SObjectUpsertRequest {
        SObjectUpsertRequest {
            body,
            api_name,
            external_id,
            external_id_value,
        }
    }

    pub fn new<T>(sobject: &T, external_id: &str) -> Result<SObjectUpsertRequest>
    where
        T: SObjectSerialization + TypedSObject,
    {
        let s = sobject.to_value()?;
        if let Value::Object(ref map) = s {
            let field_value = map.get(external_id);
            if let Some(field_value) = field_value {
                let ext_id_value = field_value.to_string();
                Ok(Self::new_raw(
                    s,
                    sobject.get_api_name().to_owned(),
                    external_id.to_owned(),
                    ext_id_value, // TODO: does this yield the correct value for all ExtId-capable types?
                ))
            } else {
                Err(
                    SalesforceError::GeneralError("Cannot upsert without a field value.".to_string())
                        .into(),
                )
            }
        } else {
            Err(SalesforceError::UnknownError.into())
        }
    }
}

impl SalesforceRequest for SObjectUpsertRequest {
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        Some(self.body.clone()) // TODO: don't clone
    }

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}/{}",
            self.api_name, self.external_id, self.external_id_value
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

impl CompositeFriendlyRequest for SObjectUpsertRequest {}

// SObject Delete Requests

pub struct SObjectDeleteRequest {
    api_name: String,
    id: String,
}

impl SObjectDeleteRequest {
    pub fn new_raw(api_name: String, id: String) -> SObjectDeleteRequest {
        SObjectDeleteRequest { api_name, id }
    }

    pub fn new<T>(sobject: &T) -> Result<SObjectDeleteRequest>
    where
        T: TypedSObject + SObjectWithId,
    {
        match sobject.get_id() {
            FieldValue::Null => return Err(SalesforceError::RecordDoesNotExistError.into()),
            FieldValue::Id(_) | FieldValue::CompositeReference(_) => {}
            _ => {
                return Err(SalesforceError::InvalidIdError(format!(
                    "{:?} is not a valid SObject Id",
                    sobject.get_id()
                ))
                .into())
            }
        }

        Ok(Self::new_raw(
            sobject.get_api_name().to_owned(),
            sobject.get_id().as_string(),
        ))
    }
}

impl SalesforceRequest for SObjectDeleteRequest {
    type ReturnValue = ();

    fn get_url(&self) -> String {
        format!("sobjects/{}/{}", self.api_name, self.id)
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

impl CompositeFriendlyRequest for SObjectDeleteRequest {}

// SObject Retrieve Requests

pub struct SObjectRetrieveRequest<T>
where
    T: SObjectDeserialization,
{
    id: SalesforceId,
    sobject_type: SObjectType,
    fields: Option<Vec<String>>,
    phantom: PhantomData<T>,
}

impl<T> SObjectRetrieveRequest<T>
where
    T: SObjectDeserialization,
{
    pub fn new(
        id: SalesforceId,
        sobject_type: &SObjectType,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<T> {
        SObjectRetrieveRequest {
            id,
            sobject_type: sobject_type.clone(),
            fields,
            phantom: PhantomData,
        }
    }
}

impl<T> SalesforceRequest for SObjectRetrieveRequest<T>
where
    T: SObjectDeserialization,
{
    type ReturnValue = T;

    fn get_url(&self) -> String {
        format!("sobjects/{}/{}/", self.sobject_type.get_api_name(), self.id)
    }

    fn get_query_parameters(&self) -> Option<Value> {
        if let Some(fields) = &self.fields {
            let mut hm = Map::new();

            hm.insert("fields".to_string(), Value::String(fields.join(",")));

            Some(Value::Object(hm))
        } else {
            None
        }
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(T::from_value(body, &self.sobject_type)?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<T> CompositeFriendlyRequest for SObjectRetrieveRequest<T> where T: SObjectRepresentation {}

pub struct BlobRetrieveRequest {
    path: String,
}

impl BlobRetrieveRequest {
    pub fn new(path: String) -> BlobRetrieveRequest {
        BlobRetrieveRequest { path }
    }
}

#[async_trait]
impl SalesforceRawRequest for BlobRetrieveRequest {
    type ReturnValue = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>>>>;

    fn get_url(&self) -> String {
        self.path.clone()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    async fn get_result(
        &self,
        _conn: &Connection,
        response: Response,
    ) -> Result<Self::ReturnValue> {
        Ok(Box::pin(response.bytes_stream()))
    }
}
