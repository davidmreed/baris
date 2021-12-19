use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Error, Result};
use serde_json::{json, Value};

use super::{
    types::*, DynamicallyTypedSObject, SObjectDeserialization, SObjectSerialization, SObjectWithId,
    TypedSObject,
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
        SObjectType {
            0: Arc::clone(&self.0),
        }
    }
}

impl SObjectType {
    pub fn new(api_name: String, describe: SObjectDescribe) -> SObjectType {
        SObjectType {
            0: Arc::new(SObjectTypeBody { api_name, describe }),
        }
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
    Blob(String), // TODO: implement Blobs
    Null,
    // TODO: implement reference parameters
}

impl FieldValue {
    // TODO: ensure these are complete.
    pub fn is_int(&self) -> bool {
        if let FieldValue::Integer(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_bool(&self) -> bool {
        if let FieldValue::Boolean(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_double(&self) -> bool {
        if let FieldValue::Double(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_string(&self) -> bool {
        if let FieldValue::String(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_date_time(&self) -> bool {
        if let FieldValue::DateTime(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_date(&self) -> bool {
        if let FieldValue::Date(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_time(&self) -> bool {
        if let FieldValue::Time(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_id(&self) -> bool {
        if let FieldValue::Id(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_null(&self) -> bool {
        if let FieldValue::Null = &self {
            true
        } else {
            false
        }
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
            FieldValue::Address(_) => todo!(),
            FieldValue::Relationship(_) => todo!(),
            FieldValue::Blob(_) => todo!(),
        }
    }

    fn from_json(value: &serde_json::Value, soap_type: SoapType) -> Result<FieldValue> {
        if let serde_json::Value::Null = value {
            return Ok(FieldValue::Null);
        }

        match soap_type {
            // TODO
            SoapType::Address | SoapType::Any | SoapType::Blob => panic!("Not supported"),
            SoapType::Boolean => {
                if let serde_json::Value::Bool(b) = value {
                    return Ok(FieldValue::Boolean(*b));
                }
            }
            SoapType::Date => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Date(b.parse()?));
                }
            }
            SoapType::DateTime => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::DateTime(b.parse()?));
                }
            }
            SoapType::Time => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Time(b.parse()?));
                }
            }
            SoapType::Double => {
                if let serde_json::Value::Number(b) = value {
                    return Ok(FieldValue::Double(b.as_f64().unwrap()));
                }
            }
            SoapType::Integer => {
                if let serde_json::Value::Number(b) = value {
                    return Ok(FieldValue::Integer(b.as_i64().unwrap()));
                }
            }
            SoapType::Id => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Id(SalesforceId::new(b)?));
                }
            }
            SoapType::String => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::String(b.to_string()));
                }
            }
        }

        Err(SalesforceError::SchemaError("Unable to convert value from JSON".to_string()).into())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SObject {
    pub sobject_type: SObjectType,
    pub fields: HashMap<String, FieldValue>,
}

impl SObjectWithId for SObject {
    fn get_id(&self) -> Option<SalesforceId> {
        if let Some(FieldValue::Id(id)) = self.get("id") {
            Some(id.clone())
        } else {
            None
        }
    }

    fn set_id(&mut self, id: Option<SalesforceId>) {
        if let Some(id) = id {
            self.put("id", FieldValue::Id(id));
        } else {
            self.put("id", FieldValue::Null);
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

    pub fn with_address(mut self, key: &str, value: Address) -> SObject {
        self.put(key, FieldValue::Address(value));
        self
    }

    pub fn with_int(mut self, key: &str, value: i64) -> SObject {
        self.put(key, FieldValue::Integer(value));
        self
    }

    pub fn with_double(mut self, key: &str, value: f64) -> SObject {
        self.put(key, FieldValue::Double(value));
        self
    }

    pub fn with_boolean(mut self, key: &str, value: bool) -> SObject {
        self.put(key, FieldValue::Boolean(value));
        self
    }

    pub fn with_string(mut self, key: &str, value: String) -> SObject {
        self.put(key, FieldValue::String(value));
        self
    }

    pub fn with_str(mut self, key: &str, value: &str) -> SObject {
        self.put(key, FieldValue::String(value.to_owned()));
        self
    }

    pub fn with_datetime(mut self, key: &str, value: DateTime) -> SObject {
        self.put(key, FieldValue::DateTime(value));
        self
    }

    pub fn with_time(mut self, key: &str, value: Time) -> SObject {
        self.put(key, FieldValue::Time(value));
        self
    }

    pub fn with_date(mut self, key: &str, value: Date) -> SObject {
        self.put(key, FieldValue::Date(value));
        self
    }

    pub fn with_reference(mut self, key: &str, value: SalesforceId) -> SObject {
        self.put(key, FieldValue::Id(value));
        self
    }

    pub fn with_relationship(mut self, key: &str, value: SObject) -> SObject {
        self.put(key, FieldValue::Relationship(value));
        self
    }

    // TODO: Blob, Geolocation

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
