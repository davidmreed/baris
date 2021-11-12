use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    Connection, SObject, SObjectType, SalesforceError, SalesforceId,
};

use anyhow::Result;
use reqwest::Method;
use serde_json::{json, Value};

use super::CreateResult;

struct SObjectCollectionCreateRequest<'a> {
    objects: &'a mut Vec<SObject>,
    all_or_none: bool,
}

impl<'a> SObjectCollectionCreateRequest<'a> {
    pub fn new(objects: &'a mut Vec<SObject>, all_or_none: bool) -> Result<Self> {
        if !objects
            .iter()
            .map(|s| s.get_id().is_none())
            .fold(true, |a, x| a && x)
        {
            return Err(SalesforceError::RecordExistsError.into());
        }
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // NTH: validate that there are up to 10 chunks.

        Ok(SObjectCollectionCreateRequest {
            objects,
            all_or_none,
        })
    }
}

impl<'a> SalesforceRequest for SObjectCollectionCreateRequest<'a> {
    type ReturnValue = Vec<CreateResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.objects.iter().map(|s| s.to_json_with_type()).collect::<Vec<Value>>()
        }))
    }

    fn get_url(&self) -> String {
        "composite/sobjects".to_owned()
    }

    fn get_method(&self) -> Method {
        Method::POST
    }

    fn get_result(
        &self,
        conn: &crate::Connection,
        body: Option<&Value>,
    ) -> Result<Self::ReturnValue> {
        todo!()
    }
}

impl<'a> CompositeFriendlyRequest for SObjectCollectionCreateRequest<'a> {}

struct SObjectCollectionRetrieveRequest {
    sobject_type: SObjectType,
    ids: Vec<SalesforceId>,
    fields: Vec<String>,
}

impl SObjectCollectionRetrieveRequest {
    pub fn new(sobject_type: &SObjectType, ids: Vec<SalesforceId>, fields: Vec<String>) -> Self {
        SObjectCollectionRetrieveRequest {
            sobject_type: sobject_type.clone(),
            ids,
            fields,
        }
    }
}

impl SalesforceRequest for SObjectCollectionRetrieveRequest {
    type ReturnValue = Vec<Option<SObject>>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "ids": self.ids,
            "fields": self.fields,
        }))
    }

    fn get_url(&self) -> String {
        format!("composite/sobjects/{}", self.sobject_type.get_api_name())
    }

    fn get_method(&self) -> Method {
        // GET and POST are both legal, depending on size of request.
        // We will always use POST.
        Method::POST
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        todo!()
    }
}

impl CompositeFriendlyRequest for SObjectCollectionRetrieveRequest {}
