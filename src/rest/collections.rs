use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    Connection, SObject, SObjectType, SalesforceError, SalesforceId,
};

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Value};

use super::DmlResultWithId;

// Traits

#[async_trait]
pub trait SObjectCollection {
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>>;
    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

#[async_trait]
impl SObjectCollection for Vec<SObject> {
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionCreateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i).unwrap().set_id(r.id.unwrap());
                }

                r.into()
            })
            .collect())
    }

    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionUpdateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .map(|r| r.into())
            .collect())
    }

    async fn upsert(
        &mut self,
        conn: &Connection,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>> {
        todo!()
    }

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        todo!()
    }
}

// Requests

pub struct SObjectCollectionCreateRequest<'a> {
    objects: &'a mut Vec<SObject>,
    all_or_none: bool,
}

impl<'a> SObjectCollectionCreateRequest<'a> {
    pub fn new(objects: &'a mut Vec<SObject>, all_or_none: bool) -> Result<Self> {
        if !objects.iter().all(|s| s.get_id().is_none()) {
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
    type ReturnValue = Vec<DmlResultWithId>;

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
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectCollectionCreateRequest<'a> {}

pub struct SObjectCollectionRetrieveRequest {
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
        if let Some(body) = body {
            if let Value::Array(list) = body {
                Ok(list
                    .iter()
                    .map(|sobj| {
                        if let Value::Object(_) = sobj {
                            SObject::from_json(sobj, &self.sobject_type).ok()
                        } else {
                            None
                        }
                    })
                    .collect())
            } else {
                Err(SalesforceError::UnknownError.into()) // TODO: can we be more specific here?
            }
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl CompositeFriendlyRequest for SObjectCollectionRetrieveRequest {}

pub struct SObjectCollectionUpdateRequest<'a> {
    objects: &'a mut Vec<SObject>,
    all_or_none: bool,
}

impl<'a> SObjectCollectionUpdateRequest<'a> {
    pub fn new(objects: &'a mut Vec<SObject>, all_or_none: bool) -> Result<Self> {
        if !objects.iter().all(|s| s.get_id().is_some()) {
            return Err(SalesforceError::RecordDoesNotExistError.into());
        }
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // NTH: validate that there are up to 10 chunks.

        Ok(SObjectCollectionUpdateRequest {
            objects,
            all_or_none,
        })
    }
}

impl<'a> SalesforceRequest for SObjectCollectionUpdateRequest<'a> {
    type ReturnValue = Vec<DmlResultWithId>;

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
        Method::PATCH
    }

    fn get_result(
        &self,
        conn: &crate::Connection,
        body: Option<&Value>,
    ) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a> CompositeFriendlyRequest for SObjectCollectionUpdateRequest<'a> {}
