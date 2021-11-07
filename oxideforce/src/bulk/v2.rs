use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, time::Duration};

use anyhow::Result;
use serde_json::json;
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;

use crate::{
    streams::{BufferedLocatorManager, BufferedLocatorStream, BufferedLocatorStreamState},
    Connection, DateTime, SObject, SObjectType, SalesforceError, SalesforceId,
};

const POLL_INTERVAL: u64 = 10;

#[derive(Copy, Clone)]
pub struct BulkQueryJob(SalesforceId);

impl Deref for BulkQueryJob {
    type Target = SalesforceId;

    fn deref(&self) -> &Self::Target {
        return &self.0;
    }
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum BulkJobStatus {
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
pub enum BulkQueryOperation {
    Query,
    QueryAll,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum BulkApiLineEnding {
    LF,
    CRLF,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum BulkApiColumnDelimiter {
    Backquote,
    Caret,
    Comma,
    Pipe,
    Semicolon,
    Tab,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum BulkApiConcurrencyMode {
    Parallel, // This type uses uppercase
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum BulkApiContentType {
    CSV,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BulkQueryJobDetail {
    id: SalesforceId,
    operation: BulkQueryOperation,
    object: String,
    created_by_id: SalesforceId,
    created_date: DateTime,
    system_modstamp: DateTime,
    state: BulkJobStatus,
    concurrency_mode: BulkApiConcurrencyMode,
    content_type: BulkApiContentType,
    api_version: f32,
    line_ending: BulkApiLineEnding,
    column_delimiter: BulkApiColumnDelimiter,
}

const RESULTS_CHUNK_SIZE: u32 = 2000;

struct BulkQueryLocatorManager {
    job: BulkQueryJob,
    conn: Connection,
    sobject_type: SObjectType,
}

impl BufferedLocatorManager for BulkQueryLocatorManager {
    fn get_next_future(
        &mut self,
        state: Option<BufferedLocatorStreamState>,
    ) -> JoinHandle<Result<BufferedLocatorStreamState>> {
        let conn = self.conn.clone();
        let sobject_type = self.sobject_type.clone();
        let job_id = *self.job;

        spawn(async move {
            let url = conn
                .get_base_url()
                .await?
                .join(&format!("jobs/query/{}/results", job_id))?;
            let mut query = HashMap::new();

            query.insert("maxRecords", format!("{}", RESULTS_CHUNK_SIZE));

            if let Some(state) = state {
                if let Some(current_locator) = state.locator {
                    // TODO errors
                    query.insert("locator", current_locator);
                }
            }

            let result = conn
                .get_client()
                .await?
                .get(url)
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

            let (done, locator) = if locator_header == "null" {
                // The literal string "null" means that we've consumed all of the results.
                (true, None)
            } else {
                (false, Some(locator_header.to_string()))
            };

            // Ingest the CSV records
            let content = result.bytes().await?;
            // TODO: respect this job's settings for delimiter.
            let buffer = csv::Reader::from_reader(&*content)
                .into_deserialize::<HashMap<String, String>>()
                .map(|r| Ok(SObject::from_csv(&r?, &sobject_type)?))
                .collect::<Result<VecDeque<SObject>>>()?;

            Ok(BufferedLocatorStreamState {
                buffer,
                locator,
                total_size: None, // TODO
                done,
            })
        })
    }
}

impl BulkQueryJob {
    pub async fn new(
        conn: &Connection,
        query: &str,
        operation: BulkQueryOperation,
    ) -> Result<Self> {
        let url = conn.get_base_url().await?.join("/jobs/query")?;

        let result = conn
            .get_client()
            .await?
            .post(url)
            .json(&json!({
                "operation": operation,
                "query": query,
            }))
            .send()
            .await?; // TODO need to handle HTTP status here and elsewhere.

        let val: BulkQueryJobDetail = result.json().await?;

        return Ok(BulkQueryJob(val.id));
    }

    pub async fn abort(&self, _conn: &Connection) -> Result<()> {
        todo!();
    }

    pub async fn check_status(&self, conn: &Connection) -> Result<BulkQueryJobDetail> {
        let url = conn
            .get_base_url()
            .await?
            .join(&format!("/jobs/query/{}", self.0))?;

        Ok(conn
            .get_client()
            .await?
            .get(url)
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
    ) -> BufferedLocatorStream {
        BufferedLocatorStream::new(
            None,
            Box::new(BulkQueryLocatorManager {
                job: *self,
                sobject_type: sobject_type.clone(),
                conn: conn.clone(),
            }),
        )
    }
}
