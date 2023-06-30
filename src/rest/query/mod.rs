use std::{collections::VecDeque, marker::PhantomData};

use anyhow::Result;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::{Map, Value};
use tokio::{spawn, task::JoinHandle};

use crate::{
    api::Connection,
    api::SalesforceRequest,
    data::traits::{SObjectBase, SObjectDeserialization},
    data::SObjectType,
    errors::SalesforceError,
    streams::{ResultStream, ResultStreamManager, ResultStreamState},
};

pub mod traits;

#[cfg(test)]
mod test;

pub struct AggregateResult(Map<String, Value>);
impl SObjectBase for AggregateResult {}

impl SObjectDeserialization for AggregateResult {
    fn from_value(value: &Value, _sobjecttype: &SObjectType) -> Result<Self> {
        if let Value::Object(map) = value {
            Ok(AggregateResult(map.clone())) // TODO: don't clone.
        } else {
            Err(SalesforceError::UnknownError.into()) // TODO
        }
    }
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
        sobject_type: &Option<SObjectType>,
    ) -> Result<ResultStreamState<T>>
    where
        T: SObjectDeserialization + Sync + Send + Unpin + 'static,
    {
        let mut sobject_type = *sobject_type;

        if sobject_type.is_none() && self.records.len() > 0 {
            // Infer the sObject type from the results.
            let result_type = self.records[0].get("attributes").get("type");

            sobject_type = Some(conn.get_type(result_type).await?);
        }
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
    // We may need to populate sobject_type from the `attributes` of our first result.
    sobject_type: Option<SObjectType>,
    phantom: PhantomData<T>,
}

impl<T> ResultStreamManager for QueryStreamLocatorManager<T>
where
    T: SObjectDeserialization + Unpin,
{
    type Output = T;

    fn get_next_future(
        &mut self,
        state: Option<ResultStreamState<T>>,
    ) -> JoinHandle<Result<ResultStreamState<T>>> {
        let conn = self.conn.clone();
        let sobject_type = self.sobject_type.clone();
        spawn(async move {
            let locator = state.unwrap().locator.unwrap();
            let result: QueryResult = conn
                .get_client()
                .await?
                .get(conn.get_instance_url().await?.join(&locator)?)
                .send()
                .await?
                .json()
                .await?;

            result.to_result_stream_state(&sobject_type)
        })
    }
}
