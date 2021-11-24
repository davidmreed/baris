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
    data::SObjectRepresentation,
    streams::{BufferedLocatorManager, BufferedLocatorStream, BufferedLocatorStreamState},
    Connection, SObjectType, SalesforceError,
};

#[async_trait]
pub trait Queryable: SObjectRepresentation + Unpin {
    async fn query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<BufferedLocatorStream<Self>> {
        let request = QueryRequest::new(sobject_type, query, all);

        Ok(conn.execute(&request).await?)
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

impl<T> Queryable for T where T: SObjectRepresentation + Unpin {}

pub struct QueryRequest<T: SObjectRepresentation> {
    query: String,
    sobject_type: SObjectType,
    all: bool,
    phantom: PhantomData<T>,
}

impl<T> QueryRequest<T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject_type: &SObjectType, query: &str, all: bool) -> QueryRequest<T> {
        QueryRequest {
            query: query.to_owned(),
            sobject_type: sobject_type.clone(),
            all,
            phantom: PhantomData,
        }
    }
}

impl<T> SalesforceRequest for QueryRequest<T>
where
    T: SObjectRepresentation + Unpin,
{
    type ReturnValue = BufferedLocatorStream<T>;

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

    fn get_result(&self, conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(BufferedLocatorStream::new(
                Some(
                    serde_json::from_value::<QueryResult>(body.clone())?
                        .to_locator_stream_state(&self.sobject_type)?,
                ),
                Box::new(QueryStreamLocatorManager {
                    conn: conn.clone(),
                    sobject_type: self.sobject_type.clone(),
                    phantom: PhantomData,
                }),
            ))
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResult {
    total_size: usize,
    done: bool,
    records: Vec<serde_json::Value>,
    next_records_url: Option<String>,
}

impl QueryResult {
    pub fn to_locator_stream_state<T>(
        self,
        sobject_type: &SObjectType,
    ) -> Result<BufferedLocatorStreamState<T>>
    where
        T: SObjectRepresentation,
    {
        Ok(BufferedLocatorStreamState::new(
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

struct QueryStreamLocatorManager<T: SObjectRepresentation> {
    conn: Connection,
    sobject_type: SObjectType,
    phantom: PhantomData<T>,
}

impl<T> BufferedLocatorManager for QueryStreamLocatorManager<T>
where
    T: SObjectRepresentation + 'static, // TODO: why is this lifetime required?
{
    type Output = T;

    fn get_next_future(
        &mut self,
        state: Option<BufferedLocatorStreamState<T>>,
    ) -> JoinHandle<Result<BufferedLocatorStreamState<T>>> {
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

            Ok(result.to_locator_stream_state(&sobject_type)?)
        })
    }
}
