use serde_derive::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    ops::Deref,
    pin::Pin,
    stream::Stream,
    task::{Context, Poll},
    time::Duration,
};

use anyhow::Result;
use serde_json::json;
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;

use crate::{Connection, SObject, SObjectType, SalesforceError, SalesforceId};

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

pub struct BulkQueryResultStream {
    locator: Option<String>,
    job: BulkQueryJob,
    conn: Connection,
    sobject_type: SObjectType,
    buffer: Option<VecDeque<SObject>>,
    retrieve_task: Option<JoinHandle<Result<(VecDeque<SObject>, bool, Option<String>)>>>,
    done: bool,
}

impl BulkQueryResultStream {
    fn new(
        job: BulkQueryJob,
        sobject_type: &SObjectType,
        conn: &Connection,
    ) -> Result<BulkQueryResultStream> {
        Ok(BulkQueryResultStream {
            locator: None,
            job,
            conn: conn.clone(),
            sobject_type: sobject_type.clone(),
            buffer: None,
            retrieve_task: None,
            done: false,
        })
    }

    async fn get_next_result_set(
        conn: Connection,
        sobject_type: SObjectType,
        job_id: BulkQueryJob,
        locator: Option<String>,
    ) -> Result<(VecDeque<SObject>, bool, Option<String>)> {
        let url = format!("{}/jobs/query/{}/results", conn.get_base_url(), *job_id);
        let mut query = HashMap::new();

        query.insert("maxRecords", format!("{}", RESULTS_CHUNK_SIZE));

        if let Some(locator) = locator {
            query.insert("locator", locator);
        }

        let result = conn
            .client
            .get(&url)
            .query(&query)
            .send()
            .await?
            .error_for_status()?;

        let headers = result.headers();

        // Ingest the headers that contain our next locator.
        let locator_header = headers
            .get("Sforce-Locator")
            .ok_or(SalesforceError::GeneralError(
                "No record set locator returned".into(),
            ))?
            .to_str()?;

        let (done, next_locator) = if locator_header == "null" {
            // The literal string "null" means that we've consumed all of the results.
            (true, None)
        } else {
            (false, Some(locator_header.to_string()))
        };

        // Ingest the CSV records
        let content = result.bytes().await?;
        // TODO: respect this job's settings for delimiter.
        let result = csv::Reader::from_reader(&*content)
            .into_deserialize::<HashMap<String, String>>()
            .map(|r| Ok(SObject::from_csv(&r?, &sobject_type)?))
            .collect::<Result<VecDeque<SObject>>>()?;

        Ok((result, done, next_locator))
    }
}

impl Stream for BulkQueryResultStream {
    type Item = Result<SObject>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(buffer) = &mut self.buffer {
            if let Some(item) = buffer.pop_front() {
                Poll::Ready(Some(Ok(item)))
            } else {
                self.buffer = None;
                if self.retrieve_task.is_none() {
                    self.retrieve_task = Some(spawn(BulkQueryResultStream::get_next_result_set(
                        self.conn.clone(),
                        self.sobject_type.clone(),
                        self.job.clone(),
                        self.locator.clone(),
                    )));
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

    pub async fn abort(&self, _conn: &Connection) -> Result<()> {
        todo!();
    }

    pub async fn check_status(&self, conn: &Connection) -> Result<BulkQueryJobDetail> {
        Ok(conn
            .client
            .get(&format!("{}/jobs/query/{}", conn.get_base_url(), &self.0))
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn complete(self, conn: &Connection) -> Result<BulkQueryJobDetail> {
        let conn = conn.clone();

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

    pub async fn get_results_stream(
        &self,
        conn: &Connection,
        sobject_type: &SObjectType,
    ) -> Result<BulkQueryResultStream> {
        BulkQueryResultStream::new(*self, sobject_type, conn)
    }
}
