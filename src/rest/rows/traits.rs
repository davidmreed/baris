use crate::data::{DynamicallyTypedSObject, SObjectRepresentation, SingleTypedSObject};
use crate::{Connection, SObjectType, SalesforceId};
use anyhow::Result;
use async_trait::async_trait;

use super::{
    SObjectCreateRequest, SObjectDeleteRequest, SObjectRetrieveRequest, SObjectUpdateRequest,
    SObjectUpsertRequest,
};

#[async_trait]
pub trait SObjectDML {
    async fn create(&mut self, conn: &Connection) -> Result<()>;
    async fn update(&mut self, conn: &Connection) -> Result<()>;
    async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()>;
    async fn delete(&mut self, conn: &Connection) -> Result<()>;
}

#[async_trait]
pub trait SObjectDynamicallyTypedRetrieval: Sized {
    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self>;
}

#[async_trait]
pub trait SObjectSingleTypedRetrieval: Sized {
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
}

#[async_trait]
impl<T> SObjectDynamicallyTypedRetrieval for T
where
    T: Sized + SObjectRepresentation + DynamicallyTypedSObject,
{
    async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
        fields: Option<Vec<String>>,
    ) -> Result<Self> {
        conn.execute(&SObjectRetrieveRequest::new(id, sobject_type, fields))
            .await
    }
}

#[async_trait]
impl<T> SObjectSingleTypedRetrieval for T
where
    T: Sized + SObjectRepresentation + SingleTypedSObject,
{
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
