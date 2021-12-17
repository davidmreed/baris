use std::{collections::VecDeque, marker::PhantomData};

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::{Map, Value};
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

use crate::{
    api::SalesforceRequest,
    data::{SObjectDeserialization, SingleTypedSObjectRepresentation},
    streams::{ResultStream, ResultStreamManager, ResultStreamState},
    Connection, SObjectType, SalesforceError,
};

#[cfg(test)]
mod test;

pub struct AggregateResult(Map<String, Value>);

impl SObjectDeserialization for AggregateResult {
    fn from_value(value: &Value, _sobjecttype: &SObjectType) -> Result<Self> {
        if let Value::Object(map) = value {
            Ok(AggregateResult { 0: map.clone() }) // TODO: don't clone.
        } else {
            Err(SalesforceError::UnknownError.into()) // TODO
        }
    }
}

#[async_trait]
pub trait Queryable: SObjectDeserialization + Unpin {
    // TODO: is a default implementation here the right approach, or a blanket impl?
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

impl<T> Queryable for T where T: SObjectDeserialization + Unpin {}

#[async_trait]
pub trait QueryableSingleType:
    SingleTypedSObjectRepresentation + SObjectDeserialization + Unpin
{
    async fn query(conn: &Connection, query: &str, all: bool) -> Result<ResultStream<Self>> {
        let request = QueryRequest::new(query, all);

        Ok(conn
            .execute(&request)
            .await?
            .to_result_stream(conn, &conn.get_type(Self::get_type_api_name()).await?)?)
    }

    async fn aggregate_query(
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

    async fn count_query(conn: &Connection, query: &str, all: bool) -> Result<usize> {
        let request = QueryRequest::new(query, all);

        Ok(conn.execute(&request).await?.total_size)
    }

    async fn query_vec(conn: &Connection, query: &str, all: bool) -> Result<Vec<Self>> {
        Ok(Self::query(conn, query, all)
            .await?
            .collect::<Result<Vec<Self>>>()
            .await?)
    }
}

impl<T> QueryableSingleType for T where
    T: SingleTypedSObjectRepresentation + SObjectDeserialization + Unpin
{
}

pub struct QueryRequest {
    query: String,
    all: bool,
}

impl QueryRequest {
    pub fn new(query: &str, all: bool) -> QueryRequest {
        QueryRequest {
            query: query.to_owned(),
            all,
        }
    }
}

impl SalesforceRequest for QueryRequest {
    type ReturnValue = QueryResult;

    fn get_query_parameters(&self) -> Option<Value> {
        let mut hm = Map::new();

        hm.insert("q".to_string(), Value::String(self.query.clone()));

        Some(Value::Object(hm))
    }

    fn get_url(&self) -> String {
        if self.all {
            "queryAll".to_string()
        } else {
            "query".to_string()
        }
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<QueryResult>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    total_size: usize,
    done: bool,
    records: Vec<serde_json::Value>,
    next_records_url: Option<String>,
}

impl QueryResult {
    pub fn to_result_stream<T>(
        self,
        conn: &Connection,
        sobject_type: &SObjectType,
    ) -> Result<ResultStream<T>>
    where
        T: SObjectDeserialization + Sync + Send + Unpin + 'static,
    {
        Ok(ResultStream::new(
            Some(self.to_result_stream_state(sobject_type)?),
            Box::new(QueryStreamLocatorManager {
                conn: conn.clone(),
                sobject_type: sobject_type.clone(),
                phantom: PhantomData,
            }),
        ))
    }

    pub(crate) fn to_result_stream_state<T>(
        self,
        sobject_type: &SObjectType,
    ) -> Result<ResultStreamState<T>>
    where
        T: SObjectDeserialization + Sync + Send + Unpin + 'static,
    {
        Ok(ResultStreamState::new(
            self.records
                .iter()
                .map(|r| T::from_value(r, sobject_type))
                .collect::<Result<VecDeque<T>>>()?,
            self.next_records_url,
            Some(self.total_size),
            self.done,
        ))
    }
}

struct QueryStreamLocatorManager<T: SObjectDeserialization + Unpin> {
    conn: Connection,
    sobject_type: SObjectType,
    phantom: PhantomData<T>,
}

impl<T> ResultStreamManager for QueryStreamLocatorManager<T>
where
    T: SObjectDeserialization + Unpin, // TODO: why is this lifetime required?
{
    type Output = T;

    fn get_next_future(
        &mut self,
        state: Option<ResultStreamState<T>>,
    ) -> JoinHandle<Result<ResultStreamState<T>>> {
        let conn = self.conn.clone();
        let sobject_type = self.sobject_type.clone();

        spawn(async move {
            let result: QueryResult = conn
                .get_client()
                .await?
                .get(state.unwrap().locator.unwrap())
                .send()
                .await?
                .json()
                .await?;

            Ok(result.to_result_stream_state(&sobject_type)?)
        })
    }
}
