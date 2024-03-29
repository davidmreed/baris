use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Error, Result};
use serde_json::{json, Value};

use super::{
    traits::{
        DynamicallyTypedSObject, SObjectBase, SObjectDeserialization, SObjectSerialization,
        SObjectWithId, TypedSObject,
    },
    types::*,
};
use crate::errors::SalesforceError;
use crate::rest::describe::SObjectDescribe;

#[derive(Debug)]
pub struct SObjectTypeBody {
    api_name: String,
    describe: SObjectDescribe,
}

impl PartialEq for SObjectTypeBody {
    fn eq(&self, other: &Self) -> bool {
        self.api_name == other.api_name
    }
}

#[derive(Debug, PartialEq)] // TODO: is the derive of PartialEq OK here?
pub struct SObjectType(Arc<SObjectTypeBody>);

impl Deref for SObjectType {
    type Target = Arc<SObjectTypeBody>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for SObjectType {
    fn clone(&self) -> Self {
        SObjectType(Arc::clone(&self.0))
    }
}

impl SObjectType {
    pub fn new(api_name: String, describe: SObjectDescribe) -> SObjectType {
        SObjectType(Arc::new(SObjectTypeBody { api_name, describe }))
    }

    pub fn get_describe(&self) -> &SObjectDescribe {
        &self.describe
    }

    pub fn get_api_name(&self) -> &str {
        &self.api_name
    }
}

impl fmt::Display for SObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.api_name)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldValue {
    // TODO: JunctionIdList?
    Address(Address),
    Integer(i64), // TODO: long/short?
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(DateTime),
    Time(Time),
    Date(Date),
    Id(SalesforceId),
    Relationship(SObject),
    Blob(Blob),
    Geolocation(Geolocation),
    Null,
    CompositeReference(String),
}

impl FieldValue {
    pub fn is_address(&self) -> bool {
        matches!(self, FieldValue::Address(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, FieldValue::Integer(_))
    }

    pub fn is_double(&self) -> bool {
        matches!(self, FieldValue::Double(_))
    }

    pub fn is_bool(&self) -> bool {
        matches!(self, FieldValue::Boolean(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, FieldValue::String(_))
    }

    pub fn is_date_time(&self) -> bool {
        matches!(self, FieldValue::DateTime(_))
    }

    pub fn is_date(&self) -> bool {
        matches!(self, FieldValue::Date(_))
    }

    pub fn is_time(&self) -> bool {
        matches!(self, FieldValue::Time(_))
    }

    pub fn is_id(&self) -> bool {
        matches!(self, FieldValue::Id(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, FieldValue::Null)
    }

    pub fn is_geolocation(&self) -> bool {
        matches!(self, FieldValue::Geolocation(_))
    }

    pub fn is_relationship(&self) -> bool {
        matches!(self, FieldValue::Relationship(_))
    }

    pub fn is_composite_reference(&self) -> bool {
        matches!(self, FieldValue::CompositeReference(_))
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, FieldValue::Blob(_))
    }

    pub fn from_str(input: &str, field_type: &SoapType) -> Result<FieldValue> {
        match field_type {
            SoapType::Integer => Ok(FieldValue::Integer(input.parse()?)),
            SoapType::Double => Ok(FieldValue::Double(input.parse()?)),
            SoapType::Boolean => Ok(FieldValue::Boolean(input.parse()?)),
            SoapType::String => Ok(FieldValue::String(input.to_owned())),
            SoapType::DateTime => Ok(FieldValue::DateTime(input.parse()?)),
            SoapType::Time => Ok(FieldValue::Time(input.parse()?)),
            SoapType::Date => Ok(FieldValue::Date(input.parse()?)),
            SoapType::Id => Ok(FieldValue::Id(input.try_into()?)),
            _ => panic!("Unsupported type"), // TODO
        }
    }
}

impl From<&FieldValue> for serde_json::Value {
    fn from(f: &FieldValue) -> serde_json::Value {
        match f {
            FieldValue::Integer(i) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*i as f64).unwrap())
            }
            FieldValue::Double(i) => {
                serde_json::Value::Number(serde_json::Number::from_f64(*i).unwrap())
            }
            FieldValue::Boolean(i) => serde_json::Value::Bool(*i),
            FieldValue::String(i) => serde_json::Value::String(i.clone()),
            FieldValue::DateTime(i) => serde_json::Value::String(i.to_string()),
            FieldValue::Time(i) => serde_json::Value::String(i.to_string()),
            FieldValue::Date(i) => serde_json::Value::String(i.to_string()),
            FieldValue::Id(i) => serde_json::Value::String(i.to_string()),
            FieldValue::Null => serde_json::Value::Null,
            FieldValue::Address(address) => serde_json::to_value(address).unwrap(), // This should be infallible
            FieldValue::Relationship(_) => todo!(),
            FieldValue::Blob(_) => todo!(),
            FieldValue::Geolocation(g) => serde_json::to_value(g).unwrap(), // This should be infallible
            FieldValue::CompositeReference(s) => serde_json::Value::String(s.clone()),
        }
    }
}

impl From<&FieldValue> for String {
    fn from(f: &FieldValue) -> String {
        f.as_string()
    }
}

impl From<FieldValue> for String {
    fn from(f: FieldValue) -> String {
        f.as_string()
    }
}

impl FieldValue {
    pub fn as_string(&self) -> String {
        match self {
            FieldValue::Integer(i) => format!("{}", i),
            FieldValue::Double(i) => format!("{}", i),
            FieldValue::Boolean(i) => format!("{}", i),
            FieldValue::String(i) => i.clone(),
            FieldValue::DateTime(i) => i.to_string(),
            FieldValue::Time(i) => i.to_string(),
            FieldValue::Date(i) => i.to_string(),
            FieldValue::Id(i) => i.to_string(),
            FieldValue::Null => "".to_string(),
            FieldValue::Address(_) => panic!("Address fields cannot be rendered as strings."),
            FieldValue::Relationship(_) => todo!(),
            FieldValue::Blob(_) => todo!(),
            FieldValue::Geolocation(_) => {
                panic!("Geolocation fields cannot be rendered as strings.")
            }
            FieldValue::CompositeReference(i) => i.clone(),
        }
    }

    fn from_json(value: &serde_json::Value, soap_type: SoapType) -> Result<FieldValue> {
        if let serde_json::Value::Null = value {
            return Ok(FieldValue::Null);
        }

        match soap_type {
            // TODO: Make these not clone.
            SoapType::Any => Err(SalesforceError::SchemaError(
                "Unable to convert value from JSON".to_string(),
            )
            .into()),
            SoapType::Address => Ok(FieldValue::Address(serde_json::from_value::<Address>(
                value.clone(),
            )?)),
            SoapType::Blob => Ok(FieldValue::Blob(serde_json::from_value::<Blob>(
                value.clone(),
            )?)),
            SoapType::Boolean => Ok(FieldValue::Boolean(serde_json::from_value::<bool>(
                value.clone(),
            )?)),
            SoapType::Date => Ok(FieldValue::Date(serde_json::from_value::<Date>(
                value.clone(),
            )?)),
            SoapType::DateTime => Ok(FieldValue::DateTime(serde_json::from_value::<DateTime>(
                value.clone(),
            )?)),
            SoapType::Time => Ok(FieldValue::Time(serde_json::from_value::<Time>(
                value.clone(),
            )?)),
            SoapType::Double => Ok(FieldValue::Double(serde_json::from_value::<f64>(
                value.clone(),
            )?)),
            SoapType::Integer => Ok(FieldValue::Integer(serde_json::from_value::<i64>(
                value.clone(),
            )?)),
            SoapType::Id => Ok(FieldValue::Id(serde_json::from_value::<SalesforceId>(
                value.clone(),
            )?)),
            SoapType::String => Ok(FieldValue::String(serde_json::from_value::<String>(
                value.clone(),
            )?)),
            SoapType::Geolocation => Ok(FieldValue::Geolocation(serde_json::from_value::<
                Geolocation,
            >(value.clone())?)),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SObject {
    pub sobject_type: SObjectType,
    pub fields: HashMap<String, FieldValue>,
}

impl SObjectWithId for SObject {
    fn get_id(&self) -> FieldValue {
        self.get("id").unwrap_or(&FieldValue::Null).clone()
    }

    fn set_id(&mut self, id: FieldValue) -> Result<()> {
        match id {
            FieldValue::Id(_) | FieldValue::Null | FieldValue::CompositeReference(_) => {
                self.put("id", id);
                Ok(())
            }
            _ => Err(SalesforceError::UnsupportedId.into()),
        }
    }
}

impl TypedSObject for SObject {
    fn get_api_name(&self) -> &str {
        &self.sobject_type.api_name
    }
}

impl DynamicallyTypedSObject for SObject {}

impl SObjectSerialization for SObject {
    fn to_value(&self) -> Result<serde_json::Value> {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.into());
        }

        Ok(serde_json::Value::Object(map))
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
            if include_id {
                match self.get_id() {
                    FieldValue::Id(_) | FieldValue::CompositeReference(_) => {
                        map.insert("id".to_string(), Value::String(self.get_id().as_string()));
                    }
                    _ => {
                        return Err(SalesforceError::InvalidIdError(format!(
                            "{:?} is not a valid Salesforce Id",
                            self.get_id()
                        ))
                        .into());
                    }
                }
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
impl SObjectBase for SObject {}

impl SObjectDeserialization for SObject {
    fn from_value(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<SObject> {
        if let serde_json::Value::Object(content) = value {
            let mut ret = SObject::new(sobjecttype);
            for k in content.keys() {
                // Get the describe for this field.
                if k != "attributes" {
                    let describe = sobjecttype.get_describe().get_field(k).unwrap();

                    ret.put(
                        &k.to_lowercase(),
                        FieldValue::from_json(value.get(k).unwrap(), describe.soap_type)?,
                    );
                }
            }
            Ok(ret)
        } else {
            Err(Error::new(SalesforceError::GeneralError(
                "Invalid record JSON".to_string(),
            )))
        }
    }
}

impl SObject {
    pub fn new(sobject_type: &SObjectType) -> SObject {
        SObject {
            sobject_type: sobject_type.clone(),
            fields: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_address(mut self, key: &str, value: Address) -> SObject {
        self.put(key, FieldValue::Address(value));
        self
    }

    #[must_use]
    pub fn with_int(mut self, key: &str, value: i64) -> SObject {
        self.put(key, FieldValue::Integer(value));
        self
    }

    #[must_use]
    pub fn with_double(mut self, key: &str, value: f64) -> SObject {
        self.put(key, FieldValue::Double(value));
        self
    }

    #[must_use]
    pub fn with_boolean(mut self, key: &str, value: bool) -> SObject {
        self.put(key, FieldValue::Boolean(value));
        self
    }

    #[must_use]
    pub fn with_string(mut self, key: &str, value: String) -> SObject {
        self.put(key, FieldValue::String(value));
        self
    }

    #[must_use]
    pub fn with_str(mut self, key: &str, value: &str) -> SObject {
        self.put(key, FieldValue::String(value.to_owned()));
        self
    }

    #[must_use]
    pub fn with_datetime(mut self, key: &str, value: DateTime) -> SObject {
        self.put(key, FieldValue::DateTime(value));
        self
    }

    #[must_use]
    pub fn with_time(mut self, key: &str, value: Time) -> SObject {
        self.put(key, FieldValue::Time(value));
        self
    }

    #[must_use]
    pub fn with_date(mut self, key: &str, value: Date) -> SObject {
        self.put(key, FieldValue::Date(value));
        self
    }

    #[must_use]
    pub fn with_reference(mut self, key: &str, value: SalesforceId) -> SObject {
        self.put(key, FieldValue::Id(value));
        self
    }

    #[must_use]
    pub fn with_relationship(mut self, key: &str, value: SObject) -> SObject {
        self.put(key, FieldValue::Relationship(value));
        self
    }

    // TODO: Blob, Geolocation

    #[must_use]
    pub fn with_composite_reference(mut self, key: &str, value: &str) -> SObject {
        self.put(key, FieldValue::CompositeReference(value.to_owned()));
        self
    }

    #[must_use]
    pub fn with_geolocation(mut self, key: &str, value: Geolocation) -> SObject {
        self.put(key, FieldValue::Geolocation(value));
        self
    }

    #[must_use]
    pub fn with_null(mut self, key: &str) -> SObject {
        self.put(key, FieldValue::Null);
        self
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(&key.to_lowercase())
    }

    pub fn put(&mut self, key: &str, val: FieldValue) {
        self.fields.insert(key.to_lowercase(), val);
    }
}
