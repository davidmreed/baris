use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use serde_json::json;
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;

use crate::data::{SObjectDeserialization, SingleTypedSObjectRepresentation};
use crate::streams::value_from_csv;
use crate::{
    data::DateTime,
    streams::{ResultStream, ResultStreamManager, ResultStreamState},
    Connection, SObjectType, SalesforceError, SalesforceId,
};

#[cfg(test)]
mod test;

const POLL_INTERVAL: u64 = 10;

#[async_trait]
pub trait BulkQueryable: SObjectDeserialization + Unpin {
    async fn bulk_query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<ResultStream<Self>> {
        let job = BulkQueryJob::new(
            &conn.clone(), // TODO: correct?
            query,
            if all {
                BulkQueryOperation::QueryAll
            } else {
                BulkQueryOperation::Query
            },
        )
        .await?;

        let job = job.complete(conn).await?; //TODO: handle returned error statuses.

        Ok(job.get_results_stream(conn, sobject_type).await)
    }
}

impl<T> BulkQueryable for T where T: SObjectDeserialization + Unpin {}

#[async_trait]
pub trait SingleTypeBulkQueryable:
    SingleTypedSObjectRepresentation + SObjectDeserialization + Unpin
{
    async fn bulk_query(conn: &Connection, query: &str, all: bool) -> Result<ResultStream<Self>> {
        let job = BulkQueryJob::new(
            &conn.clone(), // TODO: correct?
            query,
            if all {
                BulkQueryOperation::QueryAll
            } else {
                BulkQueryOperation::Query
            },
        )
        .await?;

        let job = job.complete(conn).await?; //TODO: handle returned error statuses.

        Ok(job
            .get_results_stream(conn, &conn.get_type(Self::get_type_api_name()).await?)
            .await)
    }
}

impl<T> SingleTypeBulkQueryable for T where
    T: SingleTypedSObjectRepresentation + SObjectDeserialization + Unpin
{
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
pub struct BulkQueryJob {
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

struct BulkQueryLocatorManager<T: SObjectDeserialization> {
    job_id: SalesforceId,
    conn: Connection,
    sobject_type: SObjectType,
    phantom: PhantomData<T>,
}

impl<T> ResultStreamManager for BulkQueryLocatorManager<T>
where
    T: SObjectDeserialization + Send + Sync + 'static,
{
    type Output = T;

    fn get_next_future(
        &mut self,
        state: Option<ResultStreamState<T>>,
    ) -> JoinHandle<Result<ResultStreamState<T>>> {
        let conn = self.conn.clone();
        let sobject_type = self.sobject_type.clone();
        let job_id = self.job_id.clone();

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
                .map(|r| {
                    Ok(T::from_value(
                        &value_from_csv(&r?, &sobject_type)?,
                        &sobject_type,
                    )?)
                })
                .collect::<Result<VecDeque<T>>>()?;

            Ok(ResultStreamState {
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
        let url = conn.get_base_url().await?.join("jobs/query")?;

        Ok(conn
            .get_client()
            .await?
            .post(url)
            .json(&json!({
                "operation": operation,
                "query": query,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
        // TODO: handle token refresh.
    }

    pub async fn abort(&self, _conn: &Connection) -> Result<()> {
        todo!();
    }

    pub async fn check_status(&self, conn: &Connection) -> Result<BulkQueryJob> {
        let url = conn
            .get_base_url()
            .await?
            .join(&format!("jobs/query/{}", self.id))?;

        Ok(conn
            .get_client()
            .await?
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn complete(self, conn: &Connection) -> Result<BulkQueryJob> {
        loop {
            let status: BulkQueryJob = self.check_status(&conn).await?;

            if status.state.is_completed_state() {
                return Ok(status);
            }

            sleep(Duration::from_secs(POLL_INTERVAL)).await;
        }
    }

    pub async fn get_results_stream<T: 'static>(
        // TODO: why is the lifetime required?
        &self,
        conn: &Connection,
        sobject_type: &SObjectType,
    ) -> ResultStream<T>
    where
        T: SObjectDeserialization + Unpin + Send + Sync,
    {
        ResultStream::new(
            None,
            Box::new(BulkQueryLocatorManager {
                job_id: self.id,
                sobject_type: sobject_type.clone(),
                conn: conn.clone(),
                phantom: PhantomData,
            }),
        )
    }
}
