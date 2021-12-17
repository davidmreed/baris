use anyhow::Result;
use serde_json::{json, Value};

use crate::SalesforceError;

use super::sobjects::SObjectType;
use super::types::SalesforceId;

pub trait SObjectRepresentation:
    SObjectDeserialization + SObjectSerialization + SObjectWithId + InstanceTypedSObjectRepresentation
{
}

impl<T> SObjectRepresentation for T where
    T: SObjectDeserialization
        + SObjectSerialization
        + SObjectWithId
        + InstanceTypedSObjectRepresentation
{
}

pub trait SObjectWithId {
    fn get_id(&self) -> Option<SalesforceId>;
    fn set_id(&mut self, id: Option<SalesforceId>);
}

pub trait SingleTypedSObjectRepresentation {
    fn get_type_api_name() -> &'static str;
}

pub trait InstanceTypedSObjectRepresentation {
    fn get_api_name(&self) -> &str;
}

impl<T> InstanceTypedSObjectRepresentation for T
where
    T: SingleTypedSObjectRepresentation,
{
    fn get_api_name(&self) -> &str {
        Self::get_type_api_name()
    }
}

pub trait SObjectDeserialization: Sized + Send + Sync + 'static {
    fn from_value(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<Self>;
}

pub trait SObjectSerialization: Sized + Send + Sync + 'static {
    fn to_value(&self) -> Result<Value>;
    fn to_value_with_options(&self, include_type: bool, include_id: bool) -> Result<Value>;
}

impl<'a, T> SObjectDeserialization for T
where
    T: for<'de> serde::Deserialize<'de> + SObjectRepresentation,
{
    fn from_value(value: &serde_json::Value, _sobjecttype: &SObjectType) -> Result<Self> {
        Ok(serde_json::from_value::<Self>(value.clone())?) // TODO: make this not clone.
    }
}

impl<T> SObjectSerialization for T
where
    T: serde::Serialize + SObjectRepresentation,
{
    fn to_value(&self) -> Result<Value> {
        Ok(serde_json::to_value(self)?)
    }

    fn to_value_with_options(&self, include_type: bool, include_id: bool) -> Result<Value> {
        let mut value = self.to_value()?;

        if let Value::Object(ref mut map) = value {
            if include_type {
                map.insert(
                    "attributes".to_string(),
                    json!({"type": self.get_api_name() }),
                );
            }
            if include_id && self.get_id().is_some() {
                map.insert(
                    "id".to_string(),
                    Value::String(self.get_id().unwrap().to_string()),
                );
            } else {
                // TODO: handle case-insensitivity

                if map.contains_key("id") {
                    map.remove("id");
                }
                if map.contains_key("Id") {
                    map.remove("Id");
                }
                if map.contains_key("iD") {
                    map.remove("iD");
                }
                if map.contains_key("ID") {
                    map.remove("ID");
                }
            }
            Ok(value)
        } else {
            Err(SalesforceError::UnknownError.into())
        }
    }
}
