use std::marker::PhantomData;

use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    data::SObjectRepresentation,
    Connection, SObjectType, SalesforceError, SalesforceId,
};

use anyhow::Result;
use async_trait::async_trait;
use reqwest::Method;
use serde_json::{json, Map, Value};

use super::DmlResult;

// Traits

#[async_trait]
pub trait SObjectCollection {
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn update(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
    async fn upsert(
        &mut self,
        conn: &Connection,
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>>;
    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>>;
}

#[async_trait]
impl<T> SObjectCollection for Vec<T>
where
    T: SObjectRepresentation,
{
    async fn create(&mut self, conn: Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionCreateRequest::new(self, all_or_none)?;

        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i).unwrap().set_id(r.id);
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
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Vec<Result<()>>> {
        let request =
            SObjectCollectionUpsertRequest::new(self, sobject_type, external_id, all_or_none)?;
        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    if let Some(true) = r.created {
                        self.get_mut(i).unwrap().set_id(r.id);
                    }
                }

                r.into()
            })
            .collect())
    }

    async fn delete(&mut self, conn: &Connection, all_or_none: bool) -> Result<Vec<Result<()>>> {
        let request = SObjectCollectionDeleteRequest::new(self, all_or_none)?;
        Ok(conn
            .execute(&request)
            .await?
            .into_iter()
            .enumerate()
            .map(|(i, r)| {
                if r.success {
                    self.get_mut(i).unwrap().set_id(None);
                }

                r.into()
            })
            .collect())
    }
}

// Requests

pub struct SObjectCollectionCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    objects: &'a mut Vec<T>,
    all_or_none: bool,
}

impl<'a, T> SObjectCollectionCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(objects: &'a mut Vec<T>, all_or_none: bool) -> Result<Self> {
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

impl<'a, T> SalesforceRequest for SObjectCollectionCreateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = Vec<DmlResult>;

    // TODO: this should return a Result<Option<Value>>
    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.objects.iter().map(|s| s.to_value_with_options(true, false)).collect::<Result<Vec<Value>>>().ok()
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
        _conn: &crate::Connection,
        body: Option<&Value>,
    ) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a, T> CompositeFriendlyRequest for SObjectCollectionCreateRequest<'a, T> where
    T: SObjectRepresentation
{
}

pub struct SObjectCollectionRetrieveRequest<T>
where
    T: SObjectRepresentation,
{
    sobject_type: SObjectType,
    ids: Vec<SalesforceId>,
    fields: Vec<String>,
    phantom: PhantomData<T>,
}

impl<T> SObjectCollectionRetrieveRequest<T>
where
    T: SObjectRepresentation,
{
    pub fn new(sobject_type: &SObjectType, ids: Vec<SalesforceId>, fields: Vec<String>) -> Self {
        SObjectCollectionRetrieveRequest {
            sobject_type: sobject_type.clone(),
            ids,
            fields,
            phantom: PhantomData,
        }
    }
}

impl<T> SalesforceRequest for SObjectCollectionRetrieveRequest<T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = Vec<Option<T>>;

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
                            T::from_value(sobj, &self.sobject_type).ok()
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

impl<T> CompositeFriendlyRequest for SObjectCollectionRetrieveRequest<T> where
    T: SObjectRepresentation
{
}

pub struct SObjectCollectionUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    objects: &'a mut Vec<T>,
    all_or_none: bool,
}

impl<'a, T> SObjectCollectionUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(objects: &'a mut Vec<T>, all_or_none: bool) -> Result<Self> {
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

impl<'a, T> SalesforceRequest for SObjectCollectionUpdateRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = Vec<DmlResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.objects.iter().map(|s| s.to_value_with_options(true, false)).collect::<Result<Vec<Value>>>().ok()
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
        _conn: &crate::Connection,
        body: Option<&Value>,
    ) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a, T> CompositeFriendlyRequest for SObjectCollectionUpdateRequest<'a, T> where
    T: SObjectRepresentation
{
}

pub struct SObjectCollectionUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    objects: &'a mut Vec<T>,
    external_id: String,
    sobject_type: SObjectType,
    all_or_none: bool,
}

impl<'a, T> SObjectCollectionUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(
        objects: &'a mut Vec<T>,
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Self> {
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // TODO: validate that all provided objects are of type sobject_type

        Ok(SObjectCollectionUpsertRequest {
            objects,
            external_id: external_id.to_owned(),
            sobject_type: sobject_type.clone(),
            all_or_none,
        })
    }
}

impl<'a, T> SalesforceRequest for SObjectCollectionUpsertRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = Vec<DmlResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.objects.iter().map(|s| s.to_value_with_options(true, false)).collect::<Result<Vec<Value>>>().ok()
        }))
    }

    fn get_url(&self) -> String {
        format!(
            "composite/sobjects/{}/{}",
            self.sobject_type.get_api_name(),
            self.external_id
        )
    }

    fn get_method(&self) -> Method {
        Method::PATCH
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a, T> CompositeFriendlyRequest for SObjectCollectionUpsertRequest<'a, T> where
    T: SObjectRepresentation
{
}

pub struct SObjectCollectionDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    objects: &'a mut Vec<T>,
    all_or_none: bool,
}

impl<'a, T> SObjectCollectionDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    pub fn new(objects: &'a mut Vec<T>, all_or_none: bool) -> Result<Self> {
        if !objects.iter().all(|s| s.get_id().is_some()) {
            return Err(SalesforceError::RecordDoesNotExistError.into());
        }

        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }

        Ok(SObjectCollectionDeleteRequest {
            objects,
            all_or_none,
        })
    }
}

impl<'a, T> SalesforceRequest for SObjectCollectionDeleteRequest<'a, T>
where
    T: SObjectRepresentation,
{
    type ReturnValue = Vec<DmlResult>;

    fn get_url(&self) -> String {
        format!("composite/sobjects",)
    }

    fn get_query_parameters(&self) -> Option<Value> {
        let mut hm = Map::new();

        // Will not panic by implementation of new().
        hm.insert(
            "ids".to_string(),
            self.objects
                .iter()
                .map(|o| o.get_id().unwrap().to_string())
                .collect(),
        );
        hm.insert("allOrNone".to_string(), Value::Bool(self.all_or_none));

        Some(Value::Object(hm))
    }

    fn get_method(&self) -> Method {
        Method::DELETE
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

impl<'a, T> CompositeFriendlyRequest for SObjectCollectionDeleteRequest<'a, T> where
    T: SObjectRepresentation
{
}
