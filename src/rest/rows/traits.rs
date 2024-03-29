use crate::data::{
    DynamicallyTypedSObject, SObjectDeserialization, SObjectSerialization, SObjectWithId,
    SingleTypedSObject, TypedSObject,
};
use crate::{api::Connection, data::FieldValue, data::SObjectType, data::SalesforceId};
use anyhow::Result;
use async_trait::async_trait;

use super::{
    SObjectCreateRequest, SObjectDeleteRequest, SObjectRetrieveRequest, SObjectUpdateRequest,
    SObjectUpsertRequest,
};

#[async_trait]
pub trait SObjectRowCreateable {
    fn create_request(&self) -> Result<SObjectCreateRequest>;
    async fn create(&mut self, conn: &Connection) -> Result<()>;
}

#[async_trait]
pub trait SObjectRowUpdateable {
    fn update_request(&self) -> Result<SObjectUpdateRequest>;
    async fn update(&mut self, conn: &Connection) -> Result<()>;
}

#[async_trait]
pub trait SObjectRowUpsertable {
    fn upsert_request(&self, external_id: &str) -> Result<SObjectUpsertRequest>;
    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()>;
}

#[async_trait]
pub trait SObjectRowDeletable {
    fn delete_request(&self) -> Result<SObjectDeleteRequest>;
    async fn delete(&mut self, conn: &Connection) -> Result<()>;
}

#[async_trait]
pub trait SObjectDynamicallyTypedRetrieval: SObjectDeserialization {
    fn retrieve_request(
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<Self>;

    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self>;
}

#[async_trait]
pub trait SObjectSingleTypedRetrieval: SObjectDeserialization {
    fn retrieve_request_t(
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<Self>;

    async fn retrieve_t(
        conn: &Connection,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self>;
}

#[async_trait]
impl<T> SObjectRowCreateable for T
where
    T: SObjectSerialization + SObjectWithId + TypedSObject,
{
    fn create_request(&self) -> Result<SObjectCreateRequest> {
        SObjectCreateRequest::new(self)
    }

    async fn create(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&self.create_request()?).await?;

        if result.success {
            self.set_id(FieldValue::Id(result.id.unwrap()))?;
        }
        result.into()
    }
}

#[async_trait]
impl<T> SObjectRowUpdateable for T
where
    T: SObjectSerialization + SObjectWithId + TypedSObject,
{
    fn update_request(&self) -> Result<SObjectUpdateRequest> {
        SObjectUpdateRequest::new(self)
    }

    async fn update(&mut self, conn: &Connection) -> Result<()> {
        conn.execute(&self.update_request()?).await
    }
}

#[async_trait]
impl<T> SObjectRowUpsertable for T
where
    T: SObjectSerialization + SObjectWithId + TypedSObject,
{
    fn upsert_request(&self, external_id: &str) -> Result<SObjectUpsertRequest> {
        SObjectUpsertRequest::new(self, external_id)
    }

    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()> {
        let result = conn.execute(&self.upsert_request(external_id)?).await?;

        if result.success {
            // In version 46.0 and earlier, the `created` return value
            // is not available for upsert requests.
            if let Some(id) = result.id {
                self.set_id(FieldValue::Id(id))?;
            }
        }

        result.into()
    }
}

#[async_trait]
impl<T> SObjectRowDeletable for T
where
    T: SObjectSerialization + SObjectWithId + TypedSObject,
{
    fn delete_request(&self) -> Result<SObjectDeleteRequest> {
        SObjectDeleteRequest::new(self)
    }

    async fn delete(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&self.delete_request()?).await;

        if result.is_ok() {
            self.set_id(FieldValue::Null)?;
        }

        result
    }
}

#[async_trait]
impl<T> SObjectDynamicallyTypedRetrieval for T
where
    T: SObjectDeserialization + DynamicallyTypedSObject,
{
    fn retrieve_request(
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<T> {
        SObjectRetrieveRequest::new(id, sobject_type, fields)
    }

    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self> {
        conn.execute(&Self::retrieve_request(sobject_type, id, fields))
            .await
    }
}

#[async_trait]
impl<T> SObjectSingleTypedRetrieval for T
where
    T: SObjectDeserialization + SingleTypedSObject,
{
    fn retrieve_request_t(
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<T> {
        SObjectRetrieveRequest::new(id, sobject_type, fields)
    }

    async fn retrieve_t(
        conn: &Connection,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self> {
        conn.execute(&SObjectRetrieveRequest::new(
            id,
            &conn.get_type(T::get_type_api_name()).await?,
            fields,
        ))
        .await
    }
}
