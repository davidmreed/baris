use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Error, Result};
use chrono::{FixedOffset, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::rest::describe::SObjectDescribe;
use crate::Connection;

use super::errors::SalesforceError;

// The Salesforce API's required datetime format is mostly RFC 3339,
// but requires _exactly_ three fractional second digits (millisecond resultion).
// Using the wrong number of fractional digits can cause incorrect behavior
// in the Bulk API.
// See https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_valid_date_formats.htm
const DATETIME_FORMAT: &str = "%Y-%m-%dT%T.%.3fZ";
const DATE_FORMAT: &str = "%Y-%m-%d";
const TIME_FORMAT: &str = "%T.%.3fZ"; // TODO: validate

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(try_from = "String")]
pub struct SalesforceId {
    id: [u8; 18],
}

// TODO: store as 15-char and render as 18 on string conversion.
// OR store as 18 (because the API always returns 18) and
// don't verify on 18-character input.
impl SalesforceId {
    pub fn new(id: &str) -> Result<SalesforceId, SalesforceError> {
        const ALNUMS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";

        if id.len() != 15 && id.len() != 18 {
            return Err(SalesforceError::InvalidIdError(id.to_string()));
        }

        let mut full_id: [u8; 18] = [0; 18];
        let mut bitstring: usize = 0;

        for (i, c) in id[..15].chars().enumerate() {
            if c.is_ascii_alphanumeric() {
                if c.is_ascii_uppercase() {
                    bitstring |= 1 << i
                }
                full_id[i] = c as u8;
            } else {
                return Err(SalesforceError::InvalidIdError(id.to_string()));
            }
        }
        // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
        full_id[15] = ALNUMS[bitstring & 0x1F] as u8;
        full_id[16] = ALNUMS[bitstring >> 5 & 0x1F] as u8;
        full_id[17] = ALNUMS[bitstring >> 10] as u8;

        Ok(SalesforceId { id: full_id })
    }
}

impl TryFrom<String> for SalesforceId {
    type Error = SalesforceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        SalesforceId::new(&value)
    }
}

impl TryFrom<&str> for SalesforceId {
    type Error = SalesforceError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        SalesforceId::new(value)
    }
}

impl fmt::Debug for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
    }
}

impl fmt::Display for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
    }
}

pub type DateTime = chrono::DateTime<chrono::Utc>;
pub type Time = chrono::NaiveTime;
pub type Date = chrono::NaiveDate;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Address {
    pub city: Option<String>,
    pub country: Option<String>,
    pub country_code: Option<String>,
    pub geocode_accuracy: Option<String>, // TODO: this should be an enum.
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub postal_code: Option<String>,
    pub state: Option<String>,
    pub state_code: Option<String>,
    pub street: Option<String>,
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
            // TODO: validate chrono parse behavior against API.
            SoapType::DateTime => Ok(FieldValue::DateTime(input.parse()?)),
            SoapType::Time => Ok(FieldValue::Time(input.parse()?)),
            SoapType::Date => Ok(FieldValue::Date(input.parse()?)),
            SoapType::Id => Ok(FieldValue::Id(input.try_into()?)),
            _ => panic!("Unsupported type"), // TODO
        }
    }
}

pub trait SObjectRepresentation:
    SObjectCreation + SObjectSerialization + Send + Sync + Sized
{
    fn get_id(&self) -> Option<SalesforceId>;
    fn set_id(&mut self, id: Option<SalesforceId>);
    fn get_api_name(&self) -> &str;
}

pub trait SObjectCreation
where
    Self: Sized,
{
    fn from_value(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<Self>;
}

pub trait SObjectSerialization {
    fn to_value(&self) -> Result<Value>;
}

impl<'a, T> SObjectCreation for T
where
    T: serde::Deserialize<'a>,
{
    fn from_value(value: &serde_json::Value, _sobjecttype: &SObjectType) -> Result<Self> {
        Ok(serde_json::from_value::<Self>(value)?)
    }
}

impl<T> SObjectSerialization for T
where
    T: serde::Serialize,
{
    fn to_value(&self) -> Result<Value> {
        Ok(serde_json::to_value(self)?)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SObject {
    pub sobject_type: SObjectType,
    pub fields: HashMap<String, FieldValue>,
}

impl SObjectRepresentation for SObject {
    fn get_id(&self) -> Option<SalesforceId> {
        if let Some(FieldValue::Id(id)) = self.get("id") {
            Some(id.clone()) // TODO: does this need to clone?
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

    fn get_api_name(&self) -> &str {
        &self.sobject_type.api_name
    }
}

impl SObjectSerialization for SObject {
    fn to_value(&self) -> Result<serde_json::Value> {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.into());
        }

        Ok(serde_json::Value::Object(map))
    }
}

impl SObjectCreation for SObject {
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

    // TODO: similar methods for each data type.
    pub fn with_string(mut self, key: &str, value: &str) -> SObject {
        self.put(key, FieldValue::String(value.to_owned()));
        self
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(&key.to_lowercase())
    }

    pub fn put(&mut self, key: &str, val: FieldValue) {
        self.fields.insert(key.to_lowercase(), val);
    }

    pub(crate) fn from_csv(
        rec: &HashMap<String, String>,
        sobjecttype: &SObjectType,
    ) -> Result<SObject> {
        let mut ret = SObject::new(sobjecttype);

        for k in rec.keys() {
            // Get the describe for this field.
            if k != "attributes" {
                let describe = sobjecttype.get_describe().get_field(k).unwrap();

                ret.put(
                    &k.to_lowercase(),
                    FieldValue::from_str(rec.get(k).unwrap(), &describe.soap_type)?,
                );
            }
        }

        Ok(ret)
    }

    pub(crate) fn to_json_with_type(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.into());
        }

        map.insert(
            "attributes".to_string(),
            json!({"type": self.sobject_type.get_api_name() }),
        );

        serde_json::Value::Object(map)
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
            FieldValue::DateTime(i) => {
                serde_json::Value::String(i.format(DATETIME_FORMAT).to_string())
            }
            FieldValue::Time(i) => serde_json::Value::String(i.format(TIME_FORMAT).to_string()),
            FieldValue::Date(i) => serde_json::Value::String(i.format(DATE_FORMAT).to_string()),
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

impl FieldValue {
    pub fn as_string(&self) -> String {
        match self {
            FieldValue::Integer(i) => format!("{}", i),
            FieldValue::Double(i) => format!("{}", i),
            FieldValue::Boolean(i) => format!("{}", i),
            FieldValue::String(i) => i.clone(),
            FieldValue::DateTime(i) => i.format(DATETIME_FORMAT).to_string(),
            FieldValue::Time(i) => i.format(TIME_FORMAT).to_string(),
            FieldValue::Date(i) => i.format(DATE_FORMAT).to_string(),
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
            SoapType::Address | SoapType::Any | SoapType::Blob => panic!("Not supported"),
            SoapType::Boolean => {
                if let serde_json::Value::Bool(b) = value {
                    return Ok(FieldValue::Boolean(*b));
                }
            }
            SoapType::Date => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Date(Date::parse_from_str(b, DATE_FORMAT)?));
                }
            }
            SoapType::DateTime => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::DateTime(
                        chrono::DateTime::<FixedOffset>::parse_from_str(b, DATETIME_FORMAT)?
                            .with_timezone(&Utc),
                    ));
                }
            }
            SoapType::Time => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Time(Time::parse_from_str(b, TIME_FORMAT)?));
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

#[derive(Debug, Deserialize, PartialEq, Copy, Clone)]
pub enum SoapType {
    #[serde(rename = "urn:address")]
    Address,
    #[serde(rename = "xsd:anyType")]
    Any,
    #[serde(rename = "xsd:base64binary")]
    Blob,
    #[serde(rename = "xsd:boolean")]
    Boolean,
    #[serde(rename = "xsd:date")]
    Date,
    #[serde(rename = "xsd:dateTime")]
    DateTime,
    #[serde(rename = "xsd:double")]
    Double,
    #[serde(rename = "tns:ID")]
    Id,
    #[serde(rename = "xsd:int")]
    Integer,
    #[serde(rename = "xsd:string")]
    String,
    #[serde(rename = "xsd:time")]
    Time,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_salesforce_id() {
        assert_eq!(
            "01Q36000000RXX5EAO",
            SalesforceId::new("01Q36000000RXX5").unwrap().to_string()
        );
        assert_eq!(
            "01Q36000000RXX5EAO",
            SalesforceId::new("01Q36000000RXX5EAO").unwrap().to_string()
        );
        assert_eq!(
            "0013600001ohPTpAAM",
            SalesforceId::new("0013600001ohPTp").unwrap().to_string()
        );
    }

    #[test]
    fn test_salesforce_id_errors() {
        assert!(SalesforceId::new("1111111111111111111").is_err());
        assert!(SalesforceId::new("_______________").is_err());
    }
}
