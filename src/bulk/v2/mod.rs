use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::Stream;
use reqwest::{Body, Method, Response};
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::RwLock;
use std::{collections::HashMap, time::Duration};
use tokio_stream::StreamExt;

use anyhow::Result;
use csv_async::AsyncDeserializer;
use serde_json::{json, Map, Value};
use std::collections::VecDeque;
use tokio::task::{spawn, JoinHandle};
use tokio::time::sleep;
use tokio_util::io::StreamReader;

use crate::api::{SalesforceRawRequest, SalesforceRequest};
use crate::data::{SObjectDeserialization, SObjectSerialization};
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

    async fn get_result(
        &self,
        _conn: &Connection,
        response: Response,
    ) -> Result<Self::ReturnValue> {
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

// Bulk API DML support

struct BulkDmlJobStatusRequest {
    id: SalesforceId,
}

impl BulkDmlJobStatusRequest {
    pub fn new(id: SalesforceId) -> Self {
        Self { id }
    }
}

impl SalesforceRequest for BulkDmlJobStatusRequest {
    type ReturnValue = BulkDmlJob;

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}", self.id)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Deserialize)]
struct BulkDmlResult<T>
where
    T: SObjectDeserialization,
{
    #[serde(rename = "sf__Created")]
    pub created: bool,
    #[serde(rename = "sf__Id")]
    pub id: SalesforceId,
    #[serde(flatten)]
    data: Value,
    phantom: PhantomData<T>,
}

impl<T> BulkDmlResult<T>
where
    T: SObjectDeserialization,
{
    fn get_sobject(&self, sobject_type: &SObjectType) -> Result<T> {
        T::from_value(&self.data, sobject_type)
    }
}
struct BulkDmlJobSuccessfulRecordsRequest<T>
where
    T: SObjectDeserialization,
{
    id: SalesforceId,
    phantom: PhantomData<T>,
}

#[async_trait]
impl<T> SalesforceRawRequest for BulkDmlJobSuccessfulRecordsRequest<T>
where
    T: SObjectDeserialization,
{
    type ReturnValue = Pin<Box<dyn Stream<Item = Result<BulkDmlResult<T>>>>>;

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}/successfulResults", self.id)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    // TODO: delimiter settings?
    async fn get_result(
        &self,
        _conn: &Connection,
        response: Response,
    ) -> Result<Self::ReturnValue> {
        Ok(Box::pin(
            AsyncDeserializer::from_reader(StreamReader::new(
                response
                    .bytes_stream()
                    .map(|b| b.map_err(|e| tokio::io::Error::new(tokio::io::ErrorKind::Other, e))),
            ))
            .into_deserialize::<BulkDmlResult<T>>()
            .map(|r| r.map_err(|e| e.into())),
        ))
    }
}
struct BulkDmlJobFailedRecordsRequest {}
struct BulkDmlJobUnprocessedRecordsRequest {}
struct BulkDmlJobSetStatusRequest {
    id: SalesforceId,
    status: BulkJobStatus,
}

impl BulkDmlJobSetStatusRequest {
    pub fn new(id: SalesforceId, status: BulkJobStatus) -> Self {
        Self { id, status }
    }
}

impl SalesforceRequest for BulkDmlJobSetStatusRequest {
    type ReturnValue = BulkDmlJob;

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}", self.id)
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }

    fn get_body(&self) -> Option<Value> {
        Some(json!({"state": self.status}))
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

struct BulkDmlJobDeleteRequest {
    id: SalesforceId,
}
impl BulkDmlJobDeleteRequest {
    pub fn new(id: SalesforceId) -> Self {
        Self { id }
    }
}

impl SalesforceRequest for BulkDmlJobDeleteRequest {
    type ReturnValue = ();

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}", self.id)
    }

    fn get_method(&self) -> Method {
        Method::DELETE
    }

    fn get_result(&self, _conn: &Connection, _body: Option<&Value>) -> Result<Self::ReturnValue> {
        // HTTP errors handled by the Connection; no body.
        Ok(())
    }
}

// TODO: implement query stream interface.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BulkDmlJobListResponse {
    done: bool,
    records: Vec<BulkDmlJob>,
    next_records_url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BulkDmlJobListRequest {
    is_pk_chunking_enabled: Option<bool>,
    job_type: Option<BulkApiJobType>,
    query_locator: Option<String>,
}

impl BulkDmlJobListRequest {
    pub fn new(
        is_pk_chunking_enabled: Option<bool>,
        job_type: Option<BulkApiJobType>,
        query_locator: Option<String>,
    ) -> Self {
        Self {
            is_pk_chunking_enabled,
            job_type,
            query_locator,
        }
    }
}

impl SalesforceRequest for BulkDmlJobListRequest {
    type ReturnValue = BulkDmlJobListResponse;

    fn get_url(&self) -> String {
        format!("jobs/ingest")
    }

    fn get_query_parameters(&self) -> Option<Value> {
        serde_json::to_value(&self).ok()
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum BulkApiDmlOperation {
    Insert,
    Delete,
    HardDelete,
    Update,
    Upsert,
}

#[derive(Serialize, Deserialize)]
enum BulkApiJobType {
    // serde rename is not required; this are the actual API values
    BigObjectIngest,
    Classic,
    V2Ingest,
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
    operation: BulkApiDmlOperation,
    api_version: String,
    concurrency_mode: BulkApiConcurrencyMode,
    content_url: String,
    created_by_id: SalesforceId,
    created_date: DateTime,
    job_type: BulkApiJobType,
    state: BulkJobStatus,
    system_modstamp: DateTime,
    // These properties appear to only be returned on a Get Job Info, not a Create Job. TODO
    apex_processing_time: Option<u64>,
    api_active_processing_time: Option<u64>,
    number_records_failed: Option<u64>,
    number_records_processed: Option<u64>,
    retries: Option<u32>,
    total_processing_time: Option<u64>,
}

impl BulkDmlJob {
    async fn query(
        conn: &Connection,
        is_pk_chunking_enabled: Option<bool>,
        job_type: Option<BulkApiJobType>,
        query_locator: Option<String>,
    ) -> Result<BulkDmlJobListResponse> {
        Ok(conn
            .execute(&BulkDmlJobListRequest::new(
                is_pk_chunking_enabled,
                job_type,
                query_locator,
            ))
            .await?)
    }

    async fn complete(&self, conn: &Connection) -> Result<Self> {
        loop {
            let status = self.check_status(&conn).await?;

            if status.state.is_completed_state() {
                return Ok(status);
            }

            sleep(Duration::from_secs(POLL_INTERVAL)).await;
        }
    }

    async fn check_status(&self, conn: &Connection) -> Result<Self> {
        Ok(conn.execute(&BulkDmlJobStatusRequest::new(self.id)).await?)
    }

    async fn abort(&self, conn: &Connection) -> Result<Self> {
        Ok(conn
            .execute(&BulkDmlJobSetStatusRequest::new(
                self.id,
                BulkJobStatus::Aborted,
            ))
            .await?)
    }

    async fn close(&self, conn: &Connection) -> Result<Self> {
        Ok(conn
            .execute(&BulkDmlJobSetStatusRequest::new(
                self.id,
                BulkJobStatus::UploadComplete,
            ))
            .await?)
    }

    async fn delete(&self, conn: &Connection) -> Result<()> {
        Ok(conn.execute(&BulkDmlJobDeleteRequest::new(self.id)).await?)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BulkDmlJobCreateRequest {
    assignment_rule_id: Option<SalesforceId>,
    column_delimiter: BulkApiColumnDelimiter,
    content_type: BulkApiContentType,
    external_id_field_name: Option<String>,
    line_ending: BulkApiLineEnding,
    object: String,
    operation: BulkApiDmlOperation,
}

impl BulkDmlJobCreateRequest {
    pub fn new(operation: BulkApiDmlOperation, object: String) -> Self {
        Self::new_with_options(operation, object, None, None)
    }

    pub fn new_with_options(
        operation: BulkApiDmlOperation,
        object: String,
        external_id_field_name: Option<String>,
        assignment_rule_id: Option<SalesforceId>,
    ) -> Self {
        // TODO: validation combination of operation and external Id
        Self {
            operation,
            object,
            external_id_field_name,
            assignment_rule_id,
            content_type: BulkApiContentType::CSV,
            line_ending: BulkApiLineEnding::LF,
            column_delimiter: BulkApiColumnDelimiter::Comma, // TODO: allow configuration of these two parameters
        }
    }
}

impl SalesforceRequest for BulkDmlJobCreateRequest {
    type ReturnValue = BulkDmlJob;

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn get_body(&self) -> Option<Value> {
        serde_json::to_value(&self).ok()
    }

    fn get_url(&self) -> String {
        "jobs/ingest".to_owned()
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

// The end point is that we need to provide a Stream<Item = Bytes> to Reqwest for uploading
// Ideally, we want to avoid consuming our entire input stream of records (which could be very large)
// and storing in memory or on disk.
// So what we want is essentially a stream adapter from Stream<Item = T: SObjectSerialization> to
// Stream<Item = Bytes>.
// We'd implement a struct that implements Write and Stream. When polled, it polls the SObject stream,
// serializes a returned SObject into its Writer (which uses a single, growing, mutable buffer),
// and then yields a Bytes with the written data.
// NTH: parameterize how many records it consumes at a time. One at a time is probably not efficient.
// TODO: figure out how to set "#N/A" for nulls, and make it configurable.

type BytesStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send + Sync>>;
pub fn new_bytes_stream<T>(source: Pin<Box<dyn Stream<Item = T> + Send + Sync>>) -> BytesStream
where
    T: SObjectSerialization,
{
    let mut has_headers = true;
    Box::pin(source.map(move |s| {
        let buf = BytesMut::new();
        let mut writer = csv::WriterBuilder::new()
            .has_headers(has_headers)
            .from_writer(buf.writer());
        has_headers = false;

        writer.serialize(s.to_value().unwrap()).unwrap(); // TODO: can panic
        writer.flush().unwrap(); // TODO
        Ok(writer.into_inner()?.into_inner().freeze())
    }))
}

struct BulkDmlJobIngestRequest {
    id: SalesforceId,
    body: RwLock<Option<BytesStream>>,
}

impl BulkDmlJobIngestRequest {
    pub fn new<T>(id: SalesforceId, records: impl Stream<Item = T> + 'static + Send + Sync) -> Self
    where
        T: SObjectSerialization,
    {
        Self {
            id,
            body: RwLock::new(Some(new_bytes_stream(Box::pin(records)))),
        }
    }
}

#[async_trait]
impl SalesforceRawRequest for BulkDmlJobIngestRequest {
    type ReturnValue = ();

    fn get_method(&self) -> Method {
        Method::PUT
    }

    fn get_url(&self) -> String {
        format!("jobs/ingest/{}/batches", self.id)
    }

    fn get_body(&self) -> Option<Body> {
        // This is not a good implementation. Panics are possible
        // and this results in only one possible call to get_body().
        // TODO: should get_body() consume self?
        let body = self.body.write().unwrap().take().unwrap();

        Some(Body::wrap_stream(body))
    }

    async fn get_result(
        &self,
        _conn: &Connection,
        _response: Response,
    ) -> Result<Self::ReturnValue> {
        // HTTP errors are handled by the Connection.
        Ok(())
    }
}
