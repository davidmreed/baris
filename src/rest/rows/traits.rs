use crate::data::{
    DynamicallyTypedSObject, SObjectDeserialization, SObjectRepresentation, SingleTypedSObject,
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
pub trait SObjectDynamicallyTypedRetrieval: Sized + SObjectDeserialization {
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
pub trait SObjectSingleTypedRetrieval: Sized + SObjectDeserialization {
    fn retrieve_request(
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> SObjectRetrieveRequest<Self>;

    async fn retrieve(
        conn: &Connection,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self>;
}

#[async_trait]
impl<T> SObjectDML for T
where
    T: SObjectRepresentation,
{
    fn create_request(&self) -> Result<SObjectCreateRequest> {
        Ok(SObjectCreateRequest::new(self)?)
    }

    fn delete_request(&self) -> Result<SObjectDeleteRequest> {
        Ok(SObjectDeleteRequest::new(self)?)
    }

    fn update_request(&self) -> Result<SObjectUpdateRequest> {
        Ok(SObjectUpdateRequest::new(self)?)
    }

    fn upsert_request(&self, external_id: &str) -> Result<SObjectUpsertRequest> {
        Ok(SObjectUpsertRequest::new(self, external_id)?)
    }

    async fn create(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&self.create_request()?).await?;

        if result.success {
            self.set_id(FieldValue::Id(result.id.unwrap()));
        }
        result.into()
    }

    async fn update(&mut self, conn: &Connection) -> Result<()> {
        conn.execute(&self.update_request()?).await
    }

    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()> {
        let result = conn.execute(&self.upsert_request(external_id)?).await?;

        if result.success {
            // In version 46.0 and earlier, the `created` return value
            // is not available for upsert requests.
            if let Some(id) = result.id {
                self.set_id(FieldValue::Id(id));
            }
        }

        result.into()
    }

    async fn delete(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&self.delete_request()?).await;

        if let Ok(_) = &result {
            self.set_id(FieldValue::Null);
        }

        result
    }
}

#[async_trait]
impl<T> SObjectDynamicallyTypedRetrieval for T
where
    T: Sized + SObjectRepresentation + DynamicallyTypedSObject,
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
    T: Sized + SObjectRepresentation + SingleTypedSObject,
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
