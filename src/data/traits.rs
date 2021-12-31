//! Traits representing SObject behaviors.
//!
//! Baris uses a variety of traits to express different SObject capabilities.
//! Different APIs (and trait implementations that provide access to those APIs)
//! require different SObject traits. This granular exposure of functionality
//! both allows Baris to provide seamless access to quasi-SObjects like `AggregateResult`
//! and allows clients to make their own choices about the functionality they need
//! and the costs they're willing to pay.

use anyhow::Result;
use serde_json::{json, Value};

use crate::{FieldValue, SalesforceError};

use super::sobjects::SObjectType;
use super::types::SalesforceId;

/// Represents an SObject struct that has all capabilities: can be sent to an API
/// (`SObjectSerialization`), consumed from an API (`SObjectDeserialization`),
/// has an Id, and can provide its own type.
///
/// This trait has a blanket implementation for any struct that satisfies its substraits.
pub trait SObjectRepresentation:
    SObjectDeserialization + SObjectSerialization + SObjectWithId + TypedSObject
{
}

impl<T> SObjectRepresentation for T where
    T: SObjectDeserialization + SObjectSerialization + SObjectWithId + TypedSObject
{
}

/// Represents an SObject struct that can provide its own type.
/// Required for use of many APIs that need the type.
pub trait TypedSObject {
    fn get_api_name(&self) -> &str;
}

/// Represents an SObject that can provide its own, optional Id.
/// This capability is required for APIs that perform updates, deletes,
/// and upserts. The FieldValue for the Id may hold a `SalesforceId`,
/// a `Null`, or a `CompositeReference`. Other enum values may result
/// in a panic.
pub trait SObjectWithId {
    fn get_id(&self) -> FieldValue;
    fn set_id(&mut self, id: FieldValue);

    fn get_opt_id(&self) -> Option<SalesforceId> {
        let id = self.get_id();
        match id {
            FieldValue::Id(id) => Some(id),
            _ => None,
        }
    }

    fn set_opt_id(&mut self, id: Option<SalesforceId>) {
        self.set_id(if let Some(id) = id {
            FieldValue::Id(id)
        } else {
            FieldValue::Null
        });
    }
}

/// Represents an SObject where every instance of the struct has the same
/// SObject type.
pub trait SingleTypedSObject: TypedSObject {
    fn get_type_api_name() -> &'static str;
}

impl<T> TypedSObject for T
where
    T: SingleTypedSObject,
{
    fn get_api_name(&self) -> &str {
        Self::get_type_api_name()
    }
}

/// Represents an SObject where every instance of the struct may have
/// a different SObject type.
pub trait DynamicallyTypedSObject: TypedSObject {}

/// Represents an SObject that can be deserialized from an API response.
/// A blanket implementation is provided for any struct that implements
/// `serde::Deserialize`.
///
/// Implement this trait if you need to provide
/// dynamic deserialization based on the SObject type.
pub trait SObjectDeserialization: Sized + Send + Sync + 'static {
    fn from_value(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<Self>;
}

/// Represents an SObject that can be serialized and sent to an API.
/// A blanket implementation is provided for any struct that implements
/// `serde::Serialize`.
///
/// Implement this trait if you need to provide
/// dynamic serialization behavior or if your struct does not directly
/// map to API-compatible SObject representations.
pub trait SObjectSerialization: Sized + Send + Sync + 'static {
    fn to_value(&self) -> Result<Value>;
    fn to_value_with_options(&self, include_type: bool, include_id: bool) -> Result<Value>;
}

// TODO: How can we scope down this blanket impl?
impl<'a, T> SObjectDeserialization for T
where
    T: for<'de> serde::Deserialize<'de> + Sized + Send + Sync + 'static,
{
    fn from_value(value: &serde_json::Value, _sobjecttype: &SObjectType) -> Result<Self> {
        Ok(serde_json::from_value::<Self>(value.clone())?) // TODO: make this not clone.
    }
}

impl<T> SObjectSerialization for T
where
    T: serde::Serialize + SObjectWithId + TypedSObject + Sized + Send + Sync + 'static,
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
            if include_id && self.get_opt_id().is_some() {
                map.insert(
                    "id".to_string(),
                    Value::String(self.get_opt_id().unwrap().to_string()),
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
