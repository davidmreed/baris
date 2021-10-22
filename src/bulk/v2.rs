use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    mem,
    ops::Deref,
    pin::Pin,
    stream::Stream,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use serde_json::json;
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;

use crate::{Connection, SObject, SObjectType, SalesforceId};

const POLL_INTERVAL: u64 = 10;

type Timestamp = chrono::DateTime<chrono::Utc>;

#[derive(Copy, Clone)]
struct BulkQueryJob(SalesforceId);

impl Deref for BulkQueryJob {
    type Target = SalesforceId;

    fn deref(&self) -> &Self::Target {
        return &self.0;
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
enum BulkJobStatus {
    UploadComplete,
    InProgress,
    Aborted,
    JobComplete,
    Failed,
}

impl BulkJobStatus {
    pub fn is_completed_state(&self) -> bool {
        self != &Self::UploadComplete && self != &Self::InProgress
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum BulkQueryOperation {
    Query,
    QueryAll,
}

#[derive(Serialize, Deserialize, PartialEq)]
enum BulkApiLineEnding {
    LF,
    CRLF,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum BulkApiColumnDelimiter {
    Backquote,
    Caret,
    Comma,
    Pipe,
    Semicolon,
    Tab,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum BulkApiConcurrencyMode {
    Parallel,
}

#[derive(Serialize, Deserialize, PartialEq)]
enum BulkApiContentType {
    CSV,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkQueryJobDetail {
    id: SalesforceId,
    operation: BulkQueryOperation,
    object: String,
    created_by_id: SalesforceId,
    created_date: Timestamp,
    system_modstamp: Timestamp,
    state: BulkJobStatus,
    concurrency_mode: BulkApiConcurrencyMode,
    content_type: BulkApiContentType,
    api_version: f32,
    line_ending: BulkApiLineEnding,
    column_delimiter: BulkApiColumnDelimiter,
}

const RESULTS_CHUNK_SIZE: u32 = 2000;

pub struct BulkQueryResultStream<'a> {
    locator: Option<String>,
    job: BulkQueryJob,
    conn: &'a Connection,
    sobject_type: Arc<SObjectType>,
    buffer: Option<VecDeque<SObject>>,
    retrieve_task: Option<JoinHandle<Result<()>>>,
    done: bool,
}

impl<'a> BulkQueryResultStream<'a> {
    async fn new(
        job: BulkQueryJob,
        sobject_type: &'a Arc<SObjectType>,
        conn: &'a Connection,
    ) -> Result<BulkQueryResultStream<'a>> {
        let mut new_iterator = BulkQueryResultStream {
            locator: None,
            job,
            conn,
            sobject_type: Arc::clone(sobject_type),
            buffer: None,
            retrieve_task: None,
            done: false,
        };

        new_iterator.get_next_result_set().await?; // TODO

        Ok(new_iterator)
    }

    async fn get_next_result_set(&mut self) -> Result<()> {
        let url = format!(
            "{}/jobs/query/{}/results",
            self.conn.get_base_url(),
            *self.job
        );
        let mut query = HashMap::new();

        query.insert("maxRecords", format!("{}", RESULTS_CHUNK_SIZE));
        let locator = mem::take(&mut self.locator);

        if let Some(locator) = locator {
            query.insert("locator", locator.clone());
        }

        let result = self
            .conn
            .client
            .get(&url)
            .query(&query)
            .send()
            .await?
            .error_for_status()?;
        let headers = result.headers();

        // Ingest the headers that contain our next locator.
        if let Some(locator) = headers.get("Sforce-Locator") {
            if locator == "null" {
                // The literal string "null" means that we've consumed all of the results.
                self.done = true;
                self.locator = None;
            } else {
                self.locator = Some(locator.to_str()?.to_string());
            }
        }

        // Ingest the CSV records
        let content = result.bytes().await?;
        // TODO: respect this job's settings for delimiter.
        let reader = csv::Reader::from_reader(&*content);

        // TODO: this might need to be a `spawn_blocking()`
        self.buffer = Some(
            reader
                .into_deserialize::<HashMap<String, String>>()
                .map(|r| Ok(SObject::from_csv(&r?, &self.sobject_type)?))
                .collect::<Result<VecDeque<SObject>>>()?,
        );

        self.retrieve_task = None;
        Ok(())
    }
}

impl<'a> Stream for BulkQueryResultStream<'a> {
    type Item = Result<SObject>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(buffer) = self.buffer {
            if let Some(item) = buffer.pop_front() {
                Poll::Ready(Some(Ok(item)))
            } else {
                self.buffer = None;
                if self.retrieve_task.is_none() {
                    self.retrieve_task = Some(spawn(self.get_next_result_set()));
                }

                Poll::Pending
            }
        } else if self.done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl BulkQueryJob {
    pub async fn new(
        conn: &Connection,
        sobject: &SObjectType,
        query: &str,
        operation: BulkQueryOperation,
    ) -> Result<Self> {
        let url = format!("{}/jobs/query", conn.get_base_url());

        let result: BulkQueryJobDetail = conn
            .client
            .post(&url)
            .json(&json!({
                "operation": operation,
                "query": query,
                "object": sobject.get_api_name()
            }))
            .send()
            .await?
            .json()
            .await?;

        return Ok(BulkQueryJob(result.id));
    }

    pub async fn abort(&self, conn: &Connection) -> Result<()> {
        todo!();
    }

    pub async fn check_status(&self, conn: &Connection) -> Result<BulkQueryJobDetail> {
        conn.client
            .get(&format!("{}/jobs/query/{}", conn.get_base_url(), &self.0))
            .send()
            .await?
            .json()
            .await?
    }

    pub async fn complete(self, conn: &Arc<Connection>) -> Result<BulkQueryJobDetail> {
        let conn = Arc::clone(conn);
        spawn(async move {
            loop {
                let status: BulkQueryJobDetail = self.check_status(&conn).await?;

                if status.state.is_completed_state() {
                    return Ok(status);
                }

                sleep(Duration::from_secs(POLL_INTERVAL)).await;
            }
        })
        .await?
    }

    pub async fn get_results_stream<'a>(
        &self,
        conn: &'a Connection,
        sobject_type: &'a Arc<SObjectType>,
    ) -> Result<BulkQueryResultStream<'a>> {
        BulkQueryResultStream::new(*self, sobject_type, conn).await
    }
}
