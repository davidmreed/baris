use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{Method, Response};
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use serde_json::{Map, Value};
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;

use crate::api::{SalesforceRawRequest, SalesforceRequest};
use crate::data::SObjectDeserialization;
use crate::streams::value_from_csv;
use crate::{
    data::DateTime,
    streams::{ResultStream, ResultStreamManager, ResultStreamState},
    Connection, SObjectType, SalesforceError, SalesforceId,
};

mod traits;

#[cfg(test)]
mod test;

const POLL_INTERVAL: u64 = 10;

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
    // This type uses uppercase, so no serde-renaming required.
    Parallel,
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

const RESULTS_CHUNK_SIZE: usize = 2000;

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
        let mut locator = None;

        if let Some(state) = state {
            if let Some(current_locator) = state.locator {
                locator = Some(current_locator);
            }
        } // TODO: error handling

        spawn(async move {
            let result = conn
                .execute_raw_request(&BulkQueryJobResultsRequest::new(
                    job_id,
                    locator,
                    RESULTS_CHUNK_SIZE,
                ))
                .await?;

            // Ingest the CSV records
            // TODO: respect this job's settings for delimiter.
            let buffer = csv::Reader::from_reader(&*result.content)
                .into_deserialize::<HashMap<String, String>>()
                .map(|r| {
                    Ok(T::from_value(
                        &value_from_csv(&r?, &sobject_type)?,
                        &sobject_type,
                    )?)
                })
                .collect::<Result<VecDeque<T>>>()?;

            let done = result.locator.is_none();
            Ok(ResultStreamState {
                buffer,
                locator: result.locator,
                total_size: None, // TODO
                done,
            })
        })
    }
}

#[derive(Serialize)]
struct BulkQueryJobCreateRequest {
    operation: BulkQueryOperation,
    query: String,
}

impl BulkQueryJobCreateRequest {
    pub fn new(query: String, query_all: bool) -> Self {
        Self {
            query,
            operation: if query_all {
                BulkQueryOperation::QueryAll
            } else {
                BulkQueryOperation::Query
            },
        }
    }
}

impl SalesforceRequest for BulkQueryJobCreateRequest {
    type ReturnValue = BulkQueryJob;

    fn get_url(&self) -> String {
        "jobs/query".to_owned()
    }

    fn get_method(&self) -> reqwest::Method {
        reqwest::Method::POST
    }

    fn get_body(&self) -> Option<Value> {
        serde_json::to_value(&self).ok()
    }

    fn get_result(
        &self,
        _conn: &Connection,
        body: Option<&serde_json::Value>,
    ) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Serialize)]
struct BulkQueryJobStatusRequest {
    id: SalesforceId,
}

impl BulkQueryJobStatusRequest {
    pub fn new(id: SalesforceId) -> Self {
        Self { id }
    }
}

impl SalesforceRequest for BulkQueryJobStatusRequest {
    type ReturnValue = BulkQueryJob;

    fn get_url(&self) -> String {
        format!("jobs/query/{}", self.id)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(
        &self,
        _conn: &Connection,
        body: Option<&serde_json::Value>,
    ) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

struct BulkQueryJobResultsResponse {
    locator: Option<String>,
    content: Bytes,
}

struct BulkQueryJobResultsRequest {
    id: SalesforceId,
    locator: Option<String>,
    max_records: usize,
}

impl BulkQueryJobResultsRequest {
    pub fn new(id: SalesforceId, locator: Option<String>, max_records: usize) -> Self {
        Self {
            id,
            locator,
            max_records,
        }
    }
}

#[async_trait]
impl SalesforceRawRequest for BulkQueryJobResultsRequest {
    type ReturnValue = BulkQueryJobResultsResponse;

    fn get_url(&self) -> String {
        format!("jobs/query/{}/results", self.id)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_query_parameters(&self) -> Option<Value> {
        let mut query = Map::new();

        query.insert(
            "maxRecords".to_owned(),
            Value::String(format!("{}", self.max_records)),
        );

        if let Some(current_locator) = &self.locator {
            // TODO errors
            query.insert("locator".to_owned(), Value::String(current_locator.clone()));
        }

        Some(Value::Object(query))
    }

    async fn get_result(&self, _conn: &Connection, response: Response) -> Result<Self::ReturnValue> {
        let headers = response.headers();

        // Ingest the headers that contain our next locator.
        let locator_header = headers
            .get("Sforce-Locator")
            .ok_or(SalesforceError::GeneralError(
                "No record set locator returned".into(),
            ))?
            .to_str()?;

        Ok(BulkQueryJobResultsResponse {
            locator: if locator_header == "null" {
                // The literal string "null" means that we've consumed all of the results.
                None
            } else {
                Some(locator_header.to_string())
            },
            content: response.bytes().await?,
        })
    }
}

impl BulkQueryJob {
    pub async fn create(conn: &Connection, query: &str, query_all: bool) -> Result<Self> {
        Ok(conn
            .execute(&BulkQueryJobCreateRequest::new(query.to_owned(), query_all))
            .await?)
    }

    pub async fn abort(&self, _conn: &Connection) -> Result<()> {
        todo!();
    }

    // TODO: should this take `&mut self` and replace self, returning Result<()>?
    pub async fn check_status(&self, conn: &Connection) -> Result<BulkQueryJob> {
        Ok(conn
            .execute(&BulkQueryJobStatusRequest::new(self.id))
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

    pub async fn get_results_stream<T>(
        &self,
        conn: &Connection,
        sobject_type: &SObjectType,
    ) -> ResultStream<T>
    where
        T: SObjectDeserialization + Unpin + Send + Sync + 'static,
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

struct BulkDmlJobIngestRequest {}
struct BulkDmlJobStatusRequest {}
struct BulkDmlJobSuccessfulRecordsRequest {}
struct BulkDmlJobFailedRecordsRequest {}
struct BulkDmlJobUnprocessedRecordsRequest {}
struct BulkDmlJobSetStatusRequest {}
struct BulkDmlJobDeleteRequest {}
struct BulkDmlJobListRequest {}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BulkApiDmlOperation {
    Insert,
    Delete,
    HardDelete,
    Update,
    Upsert
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BulkApiJobType {
    BigObjectIngest,
    Classic,
    V2Ingest // Need a serde override?
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkDmlJob {
    id: SalesforceId,
    assignment_rule_id: SalesforceId,
    column_delimiter: BulkApiColumnDelimiter,
    content_type: BulkApiContentType,
    external_id_field_name: String,
    line_ending: BulkApiLineEnding,
    object: String,
    operation: BulkApiDmlOperation
    api_version: String,
    concurrency_mode: BulkApiConcurrencyMode,
    content_url: Url,
    created_by_id: SalesforceId,
    created_date: DateTime,
    job_type: BulkApiJobType,
    state: BulkJobStatus,
    system_modstamp: DateTime,
}

impl BulkDmlJob {
    async fn complete(&self) -> Result<()> {

    }

    async fn status(&mut self, conn: &Connection) -> Result<()> {

    }
    
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BulkDmlJobCreateRequest {
    assignment_rule_id: Option<SalesforceId>,
    column_delimiter: BulkApiColumnDelimiter,
    content_type: BulkApiContentType,
    external_id_field_name: String,
    line_ending: BulkApiLineEnding,
    object: String,
    operation: BulkApiDmlOperation
}

impl BulkDmlJobCreateRequest {
    pub fn new_raw() -> Self {

    }

    pub fn new(operation: BulkApiDmlOperation, object: &SObjectType) -> Self {
        Self { operation, object: object.get_api_name() }
    }
}

impl SalesforceRequest for BulkDmlJobCreateRequest {
    type ReturnValue = BulkDmlJob;

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn get_body(&self) -> Option<Value> {
        Some(serde_json::to_value(&self))
    }

    fn get_url(&self) -> String {
        "jobs/ingest".to_owned()
    }
}

struct BulkDmlJobIngestRequest {

}

impl BulkDmlJobIngestRequest {
    pub fn new_raw() -> Self {

    }

    pub fn new<T>(id: SalesforceId, sobject_type: &SObjectType, records: impl Stream<Item = T>) -> Self
        where T: SObjectSerialization {

    }
}

impl SalesforceRawRequest for BulkDmlJobIngestRequest {
    type ReturnValue = ();

    fn get_method(&self) -> Method {
        Method::PUT
    }

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}/batches", self.id)
    }

    fn get_body(&self) -> Body {

    }

    fn get_result(&self, response: Response) -> Result<Self::ReturnValue> {
        // HTTP errors are handled by the Connection.
        Ok(())
    }
}
