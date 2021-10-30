use std::{
    collections::VecDeque,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Result;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::{Map, Value};
use std::future::Future;
use tokio::{spawn, task::JoinHandle};
use tokio_stream::Stream;

use crate::{api::SalesforceRequest, Connection, SObject, SObjectType};

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
    type ReturnValue = QueryStream;

    fn get_query_parameters(&self) -> Option<Value> {
        let mut hm = Map::new();

        hm.insert("q".to_string(), Value::String(self.query.clone()));

        Some(Value::Object(hm))
    }

    fn get_url(&self) -> String {
        if self.all {
            "/queryAll".to_string()
        } else {
            "/query".to_string()
        }
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, conn: &Connection, body: &Value) -> Result<Self::ReturnValue> {
        Ok(QueryStream::new(
            serde_json::from_value::<QueryResult>(body.clone())?,
            conn,
            &self.sobject_type,
        )?)
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

pub struct QueryStream {
    conn: Connection,
    sobject_type: SObjectType,
    stream: BufferedLocatorStream
}

impl BufferedLocatorManager for QueryStream {
    fn get_next_future(&mut self, state: Option<&BufferedLocatorStreamState>) -> JoinHandle<Result<BufferedLocatorStreamState>> {
        todo!();
    }
}

impl QueryStream {
    fn new(result: QueryResult, conn: &Connection, sobject_type: &SObjectType) -> Result<Self> {
    }

    fn process_query_result(&mut self, result: QueryResult) -> Result<()> {
        self.buffer = Some(
            result
                .records
                .iter()
                .map(|r| SObject::from_json(r, &self.sobject_type))
                .collect::<Result<VecDeque<SObject>>>()?,
        );
        self.done = result.done;
        self.next_records_url = result.next_records_url;

        Ok(())
    }
}

