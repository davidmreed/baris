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
    buffer: Option<VecDeque<SObject>>,
    retrieve_task: Option<JoinHandle<Result<QueryResult>>>,
    next_records_url: Option<String>,
    total_size: usize,
    yielded: usize,
    done: bool,
}

impl QueryStream {
    fn new(result: QueryResult, conn: &Connection, sobject_type: &SObjectType) -> Result<Self> {
        Ok(QueryStream {
            buffer: Some(
                result
                    .records
                    .iter()
                    .map(|r| SObject::from_json(r, sobject_type))
                    .collect::<Result<VecDeque<SObject>>>()?,
            ),
            retrieve_task: None,
            next_records_url: result.next_records_url,
            done: result.done,
            total_size: result.total_size,
            conn: conn.clone(),
            sobject_type: sobject_type.clone(),
            yielded: 0,
        })
    }
}

impl Stream for QueryStream {
    type Item = Result<SObject>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(buffer) = &mut self.buffer {
            if let Some(item) = buffer.pop_front() {
                self.yielded += 1;
                Poll::Ready(Some(Ok(item)))
            } else {
                self.buffer = None;
                if self.retrieve_task.is_none() {
                    let next_url = mem::take(&mut self.next_records_url);
                    let connection = self.conn.clone();

                    if let Some(next_url) = next_url {
                        // TODO: how do we poll and retrieve the result of this task?
                        self.retrieve_task = Some(spawn(async move {
                            let request_url = format!("{}/{}", connection.instance_url, next_url);

                            Ok(connection
                                .client
                                .get(&request_url)
                                .send()
                                .await?
                                .json()
                                .await?)
                        }));
                    }
                } else {
                    // We have a task waiting already.
                    //let fut = Pin::new(&self.retrieve_task.unwrap());
                    //let poll = fut.poll();
                }

                Poll::Pending
            }
        } else if self.done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.total_size - self.yielded,
            Some(self.total_size - self.yielded),
        )
    }
}
