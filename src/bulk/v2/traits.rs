use anyhow::Result;
use async_trait::async_trait;

use crate::data::{DynamicallyTypedSObject, SObjectDeserialization, SingleTypedSObject};
use crate::{streams::ResultStream, Connection, SObjectType};

use super::BulkQueryJob;

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
