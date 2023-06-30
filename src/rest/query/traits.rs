use anyhow::Result;
use async_trait::async_trait;
use tokio_stream::StreamExt;

use crate::{
    api::Connection,
    data::SObjectType,
    data::{DynamicallyTypedSObject, SObjectDeserialization, SingleTypedSObject},
    streams::ResultStream,
};

use super::{AggregateResult, QueryRequest};

#[async_trait]
pub trait Queryable: DynamicallyTypedSObject + SObjectDeserialization {
    // TODO: is a default implementation here the right approach, or a blanket impl?
    // TODO: we could get the sObject type from the `attributes` of the first result,
    // and unify the query traits.
    async fn query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<ResultStream<Self>> {
        let request = QueryRequest::new(query, all);

        Ok(conn
            .execute(&request)
            .await?
            .to_result_stream(conn, sobject_type)?)
    }

    async fn aggregate_query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<ResultStream<AggregateResult>> {
        let request = QueryRequest::new(query, all);

        Ok(conn
            .execute(&request)
            .await?
            .to_result_stream(conn, sobject_type)?)
    }

    async fn count_query(conn: &Connection, query: &str, all: bool) -> Result<usize> {
        let request = QueryRequest::new(query, all);

        Ok(conn.execute(&request).await?.total_size)
    }

    async fn query_vec(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<Vec<Self>> {
        Ok(Self::query(conn, sobject_type, query, all)
            .await?
            .collect::<Result<Vec<Self>>>()
            .await?)
    }
}

impl<T> Queryable for T where T: DynamicallyTypedSObject + SObjectDeserialization {}

#[async_trait]
pub trait QueryableSingleType: SingleTypedSObject + SObjectDeserialization {
    async fn query_t(conn: &Connection, query: &str, all: bool) -> Result<ResultStream<Self>> {
        let request = QueryRequest::new(query, all);

        Ok(conn
            .execute(&request)
            .await?
            .to_result_stream(conn, &conn.get_type(Self::get_type_api_name()).await?)?)
    }

    async fn aggregate_query_t(
        conn: &Connection,
        query: &str,
        all: bool,
    ) -> Result<ResultStream<AggregateResult>> {
        let request = QueryRequest::new(query, all);

        Ok(conn
            .execute(&request)
            .await?
            .to_result_stream(conn, &conn.get_type(Self::get_type_api_name()).await?)?)
    }

    async fn count_query_t(conn: &Connection, query: &str, all: bool) -> Result<usize> {
        let request = QueryRequest::new(query, all);

        Ok(conn.execute(&request).await?.total_size)
    }

    async fn query_vec_t(conn: &Connection, query: &str, all: bool) -> Result<Vec<Self>> {
        Ok(Self::query_t(conn, query, all)
            .await?
            .collect::<Result<Vec<Self>>>()
            .await?)
    }
}

impl<T> QueryableSingleType for T where T: SingleTypedSObject + SObjectDeserialization {}
