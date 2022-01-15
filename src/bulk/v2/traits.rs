use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use serde::Serialize;

use crate::data::{
    DynamicallyTypedSObject, SObjectDeserialization, SObjectSerialization, SingleTypedSObject,
};
use crate::{api::Connection, data::SObjectType, streams::ResultStream};

use super::{BulkApiDmlOperation, BulkDmlJob, BulkDmlJobCreateRequest, BulkQueryJob};

#[async_trait]
pub trait BulkQueryable: DynamicallyTypedSObject + SObjectDeserialization + Unpin {
    async fn bulk_query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<ResultStream<Self>> {
        let job = BulkQueryJob::create(
            &conn.clone(), // TODO: correct?
            query,
            all,
        )
        .await?;

        let job = job.complete(conn).await?; //TODO: handle returned error statuses.

        Ok(job.get_results_stream(conn, sobject_type).await)
    }
}

impl<T> BulkQueryable for T where T: DynamicallyTypedSObject + SObjectDeserialization + Unpin {}

#[async_trait]
pub trait SingleTypeBulkQueryable: SingleTypedSObject + SObjectDeserialization + Unpin {
    async fn bulk_query(conn: &Connection, query: &str, all: bool) -> Result<ResultStream<Self>> {
        let job = BulkQueryJob::create(
            &conn.clone(), // TODO: correct?
            query,
            all,
        )
        .await?;

        let job = job.complete(conn).await?; //TODO: handle returned error statuses.

        Ok(job
            .get_results_stream(conn, &conn.get_type(Self::get_type_api_name()).await?)
            .await)
    }
}

impl<T> SingleTypeBulkQueryable for T where T: SingleTypedSObject + SObjectDeserialization + Unpin {}

#[async_trait]
pub trait BulkInsertable {
    async fn bulk_insert(self, conn: &Connection, object: String) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> BulkInsertable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + Unpin + Serialize, // FIXME: undesirable but supports CSV
{
    async fn bulk_insert(self, conn: &Connection, object: String) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(&conn, BulkApiDmlOperation::Insert, object).await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait SingleTypeBulkInsertable {
    async fn bulk_insert(self, conn: &Connection) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> SingleTypeBulkInsertable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + SingleTypedSObject + Unpin + Serialize,
{
    async fn bulk_insert(self, conn: &Connection) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(
            &conn,
            BulkApiDmlOperation::Insert,
            T::get_type_api_name().to_owned(),
        )
        .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait BulkUpdateable {
    async fn bulk_update(self, conn: &Connection, object: String) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> BulkUpdateable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + Unpin + Serialize, // FIXME: undesirable but supports CSV
{
    async fn bulk_update(self, conn: &Connection, object: String) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(&conn, BulkApiDmlOperation::Update, object).await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait SingleTypeBulkUpdateable {
    async fn bulk_update(self, conn: &Connection) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> SingleTypeBulkUpdateable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + SingleTypedSObject + Unpin + Serialize,
{
    async fn bulk_update(self, conn: &Connection) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(
            &conn,
            BulkApiDmlOperation::Update,
            T::get_type_api_name().to_owned(),
        )
        .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait BulkDeletable {
    async fn bulk_delete(
        self,
        conn: &Connection,
        object: String,
        hard_delete: bool,
    ) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> BulkDeletable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + Unpin + Serialize, // FIXME: undesirable but supports CSV
{
    async fn bulk_delete(
        self,
        conn: &Connection,
        object: String,
        hard_delete: bool,
    ) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(
            &conn,
            if hard_delete {
                BulkApiDmlOperation::HardDelete
            } else {
                BulkApiDmlOperation::Delete
            },
            object,
        )
        .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait SingleTypeBulkDeletable {
    async fn bulk_delete(self, conn: &Connection, hard_delete: bool) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> SingleTypeBulkDeletable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + SingleTypedSObject + Unpin + Serialize,
{
    async fn bulk_delete(self, conn: &Connection, hard_delete: bool) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = BulkDmlJob::create(
            &conn,
            if hard_delete {
                BulkApiDmlOperation::HardDelete
            } else {
                BulkApiDmlOperation::Delete
            },
            T::get_type_api_name().to_owned(),
        )
        .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait BulkUpsertable {
    async fn bulk_upsert(
        self,
        conn: &Connection,
        object: String,
        external_id: String,
    ) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> BulkUpsertable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + Unpin + Serialize,
{
    async fn bulk_upsert(
        self,
        conn: &Connection,
        object: String,
        external_id: String,
    ) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = conn
            .execute(&BulkDmlJobCreateRequest::new_with_options(
                BulkApiDmlOperation::Upsert,
                object,
                Some(external_id),
                None,
            ))
            .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}

#[async_trait]
pub trait SingleTypeBulkUpsertable {
    async fn bulk_upsert(self, conn: &Connection, external_id: String) -> Result<BulkDmlJob>;
}

#[async_trait]
impl<K, T> SingleTypeBulkUpsertable for K
where
    K: Stream<Item = T> + Send + Sync + 'static,
    T: SObjectSerialization + SingleTypedSObject + Unpin + Serialize,
{
    async fn bulk_upsert(self, conn: &Connection, external_id: String) -> Result<BulkDmlJob> {
        let conn = conn.clone();
        let job = conn
            .execute(&BulkDmlJobCreateRequest::new_with_options(
                BulkApiDmlOperation::Upsert,
                T::get_type_api_name().to_owned(),
                Some(external_id),
                None,
            ))
            .await?;
        job.ingest(&conn, self).await?;
        job.close(&conn).await?;

        let job = job.complete(&conn).await?;

        Ok(job)
    }
}
