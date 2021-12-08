use std::marker::PhantomData;

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Method;
use serde_json::Map;
use serde_json::Value;

use crate::api::CompositeFriendlyRequest;
use crate::api::SalesforceRequest;
use crate::data::SObjectRepresentation;
use crate::{Connection, SObjectType, SalesforceError, SalesforceId};

use super::DmlError;
use super::DmlResult;

// SObject class implementation

#[async_trait]
pub trait SObjectDML: Sized {
    async fn create(&mut self, conn: &Connection) -> Result<()>;
    async fn update(&mut self, conn: &Connection) -> Result<()>;
    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()>;
    async fn delete(&mut self, conn: &Connection) -> Result<()>;
    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>
    ) -> Result<Self>;
}

#[async_trait]
impl<T> SObjectDML for T
where
    T: SObjectRepresentation,
{
    async fn create(&mut self, conn: &Connection) -> Result<()> {
        let request = SObjectCreateRequest::new(self)?;
        let result = conn.execute(&request).await?;

        if result.success {
            self.set_id(Some(result.id.unwrap()));
        }
        result.into()
    }

    async fn update(&mut self, conn: &Connection) -> Result<()> {
        conn.execute(&SObjectUpdateRequest::new(self)?).await
    }

    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()> {
        let result = conn
            .execute(&SObjectUpsertRequest::new(self, external_id)?)
            .await?;

        if result.success {
            // In version 46.0 and earlier, the `created` return value
            // is not available for upsert requests.
            if let Some(id) = result.id {
                self.set_id(Some(id));
            }
        }

        result.into()
    }

    async fn delete(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&SObjectDeleteRequest::new(self)?).await;

        if let Ok(_) = &result {
            self.set_id(None);
        }

        result
    }

    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>
    ) -> Result<Self> {
        conn.execute(&SObjectRetrieveRequest::new(id, sobject_type, fields))
            .await
    }
}

// SObject Create Requests

pub struct SObjectCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    sobject: &'a mut T,
}

impl<'a, T> SObjectCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject: &'a mut T) -> Result<Self> {
        if sobject.get_id().is_some() {
            return Err(SalesforceError::RecordExistsError.into());
        }

        Ok(Self { sobject })
    }
}

impl<'a, T> SalesforceRequest for SObjectCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        self.sobject.to_value_with_options(false, false).ok()
    }

    fn get_url(&self) -> String {
        format!("sobjects/{}/", self.sobject.get_api_name())
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

impl<'a, T> CompositeFriendlyRequest for SObjectCreateRequest<'a, T> where T: SObjectRepresentation {}

// SObject Update Requests

pub struct SObjectUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    sobject: &'a mut T,
}

impl<'a, T> SObjectUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject: &'a mut T) -> Result<SObjectUpdateRequest<T>> {
        if sobject.get_id().is_none() {
            Err(SalesforceError::RecordDoesNotExistError.into())
        } else {
            Ok(SObjectUpdateRequest { sobject })
        }
    }
}

impl<'a, T> SalesforceRequest for SObjectUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = ();

    fn get_body(&self) -> Option<Value> {
        self.sobject.to_value_with_options(false, false).ok()
    }

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}",
            self.sobject.get_api_name(),
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

impl<'a, T> CompositeFriendlyRequest for SObjectUpdateRequest<'a, T> where T: SObjectRepresentation {}

// SObject Upsert Requests
pub struct SObjectUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    sobject: &'a mut T,
    external_id: String,
    external_id_value: String,
}

impl<'a, T> SObjectUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject: &'a mut T, external_id: &str) -> Result<SObjectUpsertRequest<'a, T>> {
        let s = sobject.to_value()?;
        if let Value::Object(map) = s {
            let field_value = map.get(external_id);
            if field_value.is_none() {
                Err(
                    SalesforceError::GeneralError(format!("Cannot upsert without a field value."))
                        .into(),
                )
            } else {
                Ok(SObjectUpsertRequest {
                    sobject,
                    external_id: external_id.to_owned(),
                    external_id_value: field_value.unwrap().to_string(), // TODO: does this yield the correct value for all ExtId-capable types?
                })
            }
        } else {
            Err(SalesforceError::UnknownError.into())
        }
    }
}

impl<'a, T> SalesforceRequest for SObjectUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = DmlResult;

    fn get_body(&self) -> Option<Value> {
        self.sobject.to_value().ok()
    }

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}/{}",
            self.sobject.get_api_name(),
            self.external_id,
            self.external_id_value
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

impl<'a, T> CompositeFriendlyRequest for SObjectUpsertRequest<'a, T> where T: SObjectRepresentation {}

// SObject Delete Requests

pub struct SObjectDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    sobject: &'a mut T,
}

impl<'a, T> SObjectDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject: &'a mut T) -> Result<SObjectDeleteRequest<T>> {
        if let Some(_) = sobject.get_id() {
            Ok(SObjectDeleteRequest { sobject })
        } else {
            Err(SalesforceError::RecordDoesNotExistError.into())
        }
    }
}

impl<'a, T> SalesforceRequest for SObjectDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = ();

    fn get_url(&self) -> String {
        format!(
            "sobjects/{}/{}",
            self.sobject.get_api_name(),
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

impl<'a, T> CompositeFriendlyRequest for SObjectDeleteRequest<'a, T> where T: SObjectRepresentation {}

// SObject Retrieve Requests

pub struct SObjectRetrieveRequest<T>
where
    T: SObjectRepresentation,
{
    id: SalesforceId,
    sobject_type: SObjectType,
    fields: Option<Vec<String>>,
    phantom: PhantomData<T>,
}

impl<T> SObjectRetrieveRequest<T>
where
    T: SObjectRepresentation,
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
    T: SObjectRepresentation,
{
    type ReturnValue = T;

    fn get_url(&self) -> String {
        format!("sobjects/{}/{}/", self.sobject_type.get_api_name(), self.id)
    }

    fn get_query_parameters(&self) -> Option<Value> {
        if let Some(fields) = &self.fields {
            let mut hm = Map::new();

            hm.insert(
                "fields".to_string(),
                Value::Array(
                    fields
                        .iter()
                        .map(|f| Value::String(f.to_string()))
                        .collect(),
                ),
            );

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
