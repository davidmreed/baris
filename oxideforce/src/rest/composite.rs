use serde_json::Value;

use crate::api::SalesforceRequest;

pub struct CompositeRequest {
    requests: Vec<Box<dyn SalesforceRequest>>,
}

impl CompositeRequest {
    pub fn add(&mut self, req: impl SalesforceRequest) {}
}

impl SalesforceRequest for CompositeRequest {
    type ReturnValue = Value;
}
