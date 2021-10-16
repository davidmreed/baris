use std::{
    pin::Pin,
    stream::Stream,
    task::{Context, Poll},
    time::Duration,
};


struct QueryRequest {
    query: String,
    sobject_type: &'a Rc<SObjectType>,
    all: bool,
    connection: &'a Connection
}

impl QueryRequest {
    pub fn new(connection: &'a Connection, sobject_type: &'a Rc<SObjectType>, query: &str, all: bool) -> QueryRequest {
        QueryRequest {connection, query.to_owned(), sobject_type: Rc::clone(sobject_type), all}
    }
}

impl SalesforceRequest for QueryRequest {
    type ReturnValue = Result<QueryStream>;

    fn validate(&self) -> Result<()> {
        Ok(())
    }

    fn finalize_request(&self, request: &RequestBuilder) { // TODO: this structure is probably nonoptimal for composite requests
        request.query(&[("q", query)])
    }

    fn get_body(&self) -> Result<Option<Value>> {
        Ok(None)
    }

    fn get_url(&self) -> Result<String> {
        if self.all {
            "queryAll"
        } else {
            "query"
        }
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, body: &Value) -> Self::ReturnValue {
        Ok(QueryStream::new(
            body.from_value::<QueryResult>()?,
            self.sobject_type,
            self.connection
        ))
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

pub struct QueryStream<'a> {
    conn: &'a Connection,
    sobject_type: &'a Rc<SObjectType>,
    buffer: Option<VecDeque<SObject>>,
    retrieve_task: Option<JoinHandle<Result<QueryResult>>>,
    next_records_url: Option<String>,
    total_size: usize,
    done: bool,    
}

impl QueryStream<'_> {
    fn new<'a>(
        result: QueryResult,
        conn: &'a Connection,
        sobject_type: &'a Rc<SObjectType>,
    ) -> Result<Self> {
        QueryStream {
            buffer: result.records.map(|r| SObject::from_json(
                r,
                self.sobject_type,
            )?).collect::<VecDeque>(),
            retrieve_task: None,
            next_records_url: result.next_records_url,
            done: result.done,
            total_size: result.total_size,
            conn,
            sobject_type,
        }
    }
}

impl<'a> Stream for QueryStream<'a> {
    type Item = Result<SObject>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(buffer) = self.buffer {
            if let Some(item) = buffer.pop_front() {
                Poll::Ready(Ok(Some(item)))
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

    async fn get_next_result_set() {
        if let Some(next_url) = &self.result.next_records_url {
            let request_url = format!("{}/{}", self.conn.instance_url, next_url);
            self.result = self
                .conn
                .client
                .get(&request_url)
                .send()
                .await?
                .json()
                .await?;
            self.index = 0;
        }

    }

    fn len(&self) -> usize {
        self.total_size
    }
}
