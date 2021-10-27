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

    fn try_to_yield(&mut self) -> Option<SObject> {
        if let Some(buffer) = &mut self.buffer {
            if let Some(item) = buffer.pop_front() {
                self.yielded += 1;
                Some(item)
            } else {
                self.buffer = None;
                None
            }
        } else {
            None
        }
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

impl Stream for QueryStream {
    type Item = Result<SObject>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // First, check if we have sObjects ready to yield.
        if let Some(sobject) = self.try_to_yield() {
            return Poll::Ready(Some(Ok(sobject)));
        }
        // Check if we have a running task that is ready to yield a new buffer.
        if let Some(task) = &mut self.retrieve_task {
            // We have a task waiting already.
            let fut = unsafe { Pin::new_unchecked(task) };
            let poll = fut.poll(cx);
            if let Poll::Ready(result) = poll {
                self.process_query_result(result??)?;

                self.retrieve_task = None;

                if let Some(sobject) = self.try_to_yield() {
                    return Poll::Ready(Some(Ok(sobject)));
                } // TODO: could this buffer ever be empty?
            }
        }

        // Do we have a next records URL?
        if let Some(next_url) = mem::take(&mut self.next_records_url) {
            let connection = self.conn.clone();

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
            return Poll::Pending;
        }

        // If we are done, return a sigil.
        if self.done {
            return Poll::Ready(None);
        }

        // TODO: we should never reach this point.
        return Poll::Pending;
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.total_size - self.yielded,
            Some(self.total_size - self.yielded),
        )
    }
}
