use std::marker::PhantomData;

use crate::{
    api::{CompositeFriendlyRequest, SalesforceRequest},
    data::{SObjectDeserialization, SObjectSerialization, SObjectWithId},
    Connection, SObjectType, SalesforceError, SalesforceId,
};

use anyhow::Result;
use itertools::Itertools;
use reqwest::Method;
use serde_json::{json, Value};

use super::DmlResult;

pub mod traits;

#[cfg(test)]
mod test;

pub struct SObjectCollectionCreateRequest {
    records: Vec<Value>,
    all_or_none: bool,
}

impl SObjectCollectionCreateRequest {
    pub fn new_raw(records: Vec<Value>, all_or_none: bool) -> Self {
        Self {
            records,
            all_or_none,
        }
    }
    pub fn new<T>(objects: &Vec<T>, all_or_none: bool) -> Result<Self>
    where
        T: SObjectSerialization + SObjectWithId,
    {
        if !objects.iter().all(|s| s.get_id().is_null()) {
            return Err(SalesforceError::RecordExistsError.into());
        }
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // NTH: validate that there are up to 10 chunks.

        Ok(Self::new_raw(
            objects
                .iter()
                .map(|s| s.to_value_with_options(true, false))
                .collect::<Result<Vec<Value>>>()?,
            all_or_none,
        ))
    }
}

impl SalesforceRequest for SObjectCollectionCreateRequest {
    type ReturnValue = Vec<DmlResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.records
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

impl CompositeFriendlyRequest for SObjectCollectionCreateRequest {}

pub struct SObjectCollectionRetrieveRequest<T>
where
    T: SObjectDeserialization,
{
    sobject_type: SObjectType,
    ids: Vec<SalesforceId>,
    fields: Vec<String>,
    phantom: PhantomData<T>,
}

impl<T> SObjectCollectionRetrieveRequest<T>
where
    T: SObjectDeserialization,
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
    T: SObjectDeserialization,
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
    T: SObjectDeserialization
{
}

pub struct SObjectCollectionUpdateRequest {
    records: Vec<Value>,
    all_or_none: bool,
}

impl SObjectCollectionUpdateRequest {
    pub fn new_raw(records: Vec<Value>, all_or_none: bool) -> Self {
        Self {
            records,
            all_or_none,
        }
    }
    pub fn new<T>(objects: &Vec<T>, all_or_none: bool) -> Result<Self>
    where
        T: SObjectSerialization + SObjectWithId,
    {
        if !objects.iter().all(|s| s.get_id().is_null()) {
            return Err(SalesforceError::RecordDoesNotExistError.into());
        }
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // NTH: validate that there are up to 10 chunks.

        Ok(Self::new_raw(
            objects
                .iter()
                .map(|s| s.to_value_with_options(true, false))
                .collect::<Result<Vec<Value>>>()?,
            all_or_none,
        ))
    }
}

impl SalesforceRequest for SObjectCollectionUpdateRequest {
    type ReturnValue = Vec<DmlResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.records
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

impl CompositeFriendlyRequest for SObjectCollectionUpdateRequest {}

pub struct SObjectCollectionUpsertRequest {
    objects: Vec<Value>,
    external_id: String,
    sobject_type: SObjectType,
    all_or_none: bool,
}

impl SObjectCollectionUpsertRequest {
    pub fn new_raw(
        objects: Vec<Value>,
        external_id: String,
        sobject_type: SObjectType,
        all_or_none: bool,
    ) -> Self {
        Self {
            objects,
            external_id,
            sobject_type,
            all_or_none,
        }
    }
    pub fn new<T>(
        objects: &Vec<T>,
        sobject_type: &SObjectType,
        external_id: &str,
        all_or_none: bool,
    ) -> Result<Self>
    where
        T: SObjectSerialization,
    {
        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }
        // TODO: validate that all provided objects are of type sobject_type

        Ok(Self::new_raw(
            objects
                .iter()
                .map(|s| s.to_value_with_options(true, false))
                .collect::<Result<Vec<Value>>>()?,
            external_id.to_owned(),
            sobject_type.clone(),
            all_or_none,
        ))
    }
}

impl SalesforceRequest for SObjectCollectionUpsertRequest {
    type ReturnValue = Vec<DmlResult>;

    fn get_body(&self) -> Option<Value> {
        Some(json! ({
            "allOrNone": self.all_or_none,
            "records": self.objects
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

impl CompositeFriendlyRequest for SObjectCollectionUpsertRequest {}

pub struct SObjectCollectionDeleteRequest {
    ids: Vec<String>,
    all_or_none: bool,
}

impl SObjectCollectionDeleteRequest {
    pub fn new_raw(ids: Vec<String>, all_or_none: bool) -> Self {
        Self { ids, all_or_none }
    }
    pub fn new<T>(objects: &Vec<T>, all_or_none: bool) -> Result<Self>
    where
        T: SObjectWithId,
    {
        if !objects.iter().all(|s| !s.get_id().is_null()) {
            return Err(SalesforceError::RecordDoesNotExistError.into());
        }

        if objects.len() > 200 {
            return Err(SalesforceError::SObjectCollectionError.into());
        }

        Ok(Self::new_raw(
            objects.iter().map(|o| o.get_id().as_string()).collect(),
            all_or_none,
        ))
    }
}

impl SalesforceRequest for SObjectCollectionDeleteRequest {
    type ReturnValue = Vec<DmlResult>;

    fn get_url(&self) -> String {
        format!("composite/sobjects",)
    }

    fn get_query_parameters(&self) -> Option<Value> {
        Some(json!({
            "ids": self.ids.iter().join(","),
            "allOrNone": self.all_or_none
        }))
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

impl CompositeFriendlyRequest for SObjectCollectionDeleteRequest {}
