use std::collections::VecDeque;

use anyhow::Result;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::{Map, Value};
use tokio::{spawn, task::JoinHandle};
use tokio_stream::StreamExt;

use crate::{
    api::SalesforceRequest,
    data::SObjectCreation,
    streams::{BufferedLocatorManager, BufferedLocatorStream, BufferedLocatorStreamState},
    Connection, SObject, SObjectType, SalesforceError,
};

impl SObject {
    pub async fn query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<BufferedLocatorStream> {
        let request = QueryRequest::new(sobject_type, query, all);

        Ok(conn.execute(&request).await?)
    }

    pub async fn query_vec(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<Vec<SObject>> {
        Ok(Self::query(conn, sobject_type, query, all)
            .await?
            .collect::<Result<Vec<SObject>>>()
            .await?)
    }
}

pub struct QueryRequest {
    query: String,
    sobject_type: SObjectType,
    all: bool,
}

impl QueryRequest {
    pub fn new(sobject_type: &SObjectType, query: &str, all: bool) -> QueryRequest {
        QueryRequest {
            query: query.to_owned(),
            sobject_type: sobject_type.clone(),
            all,
        }
    }
}

impl SalesforceRequest for QueryRequest {
    type ReturnValue = BufferedLocatorStream;

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
    pub fn to_locator_stream_state(
        self,
        sobject_type: &SObjectType,
    ) -> Result<BufferedLocatorStreamState> {
        Ok(BufferedLocatorStreamState::new(
            self.records
                .iter()
                .map(|r| SObject::from_value(r, sobject_type))
                .collect::<Result<VecDeque<SObject>>>()?,
            self.next_records_url,
            Some(self.total_size),
            self.done,
        ))
    }
}

struct QueryStreamLocatorManager {
    conn: Connection,
    sobject_type: SObjectType,
}

impl BufferedLocatorManager for QueryStreamLocatorManager {
    fn get_next_future(
        &mut self,
        state: Option<BufferedLocatorStreamState>,
    ) -> JoinHandle<Result<BufferedLocatorStreamState>> {
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
