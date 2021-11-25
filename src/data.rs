use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Display};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Error, Result};
use chrono::{TimeZone, Utc};
use serde_derive::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::rest::describe::SObjectDescribe;

use super::errors::SalesforceError;

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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(try_from = "&str")]
pub struct DateTime(chrono::DateTime<chrono::Utc>);

impl DateTime {
    pub fn new(
        year: i32,
        month: u32,
        day: u32,
        hours: u32,
        minutes: u32,
        seconds: u32,
        milliseconds: u32,
    ) -> Result<DateTime> {
        Ok(DateTime {
            0: chrono::Utc
                .ymd_opt(year, month, day)
                .and_hms_milli_opt(hours, minutes, seconds, milliseconds)
                .single()
                .ok_or(SalesforceError::DateTimeError)?,
        })
    }
}

impl Deref for DateTime {
    type Target = chrono::DateTime<chrono::Utc>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&str> for DateTime {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Salesforce's version of RFC3339 doesn't include a colon as required by the standard,
        // giving +0000 instead of the expected +00:00

        Ok(DateTime {
            0: chrono::DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.3fZ%z")?
                .with_timezone(&Utc),
        })
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.0.format("%Y-%m-%dT%H:%M:%S%.3fZ%z").to_string()
        )
    }
}

impl FromStr for DateTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        s.try_into()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Time(chrono::NaiveTime);

impl Time {
    pub fn new(hour: u32, min: u32, sec: u32, milli: u32) -> Result<Time> {
        Ok(Time {
            0: chrono::NaiveTime::from_hms_milli_opt(hour, min, sec, milli)
                .ok_or(SalesforceError::DateTimeError)?,
        })
    }
}

impl Deref for Time {
    type Target = chrono::NaiveTime;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&str> for Time {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Time {
            0: chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.3fZ")?,
        })
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%H:%M:%S%.3fZ").to_string())
    }
}

impl FromStr for Time {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        s.try_into()
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Date(chrono::NaiveDate);

impl Date {
    pub fn new(year: i32, month: u32, day: u32) -> Result<Date> {
        Ok(Date {
            0: chrono::NaiveDate::from_ymd_opt(year, month, day)
                .ok_or(SalesforceError::DateTimeError)?,
        })
    }
}

impl Deref for Date {
    type Target = chrono::NaiveDate;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&str> for Date {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Date {
            0: chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")?,
        })
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d").to_string())
    }
}

impl FromStr for Date {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        s.try_into()
    }
}

// TODO: add field type for Geolocation

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
    SObjectCreation + SObjectSerialization + Send + Sync + Sized + 'static
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
    fn to_value_with_options(&self, include_type: bool, include_id: bool) -> Result<Value>;
}

impl<'a, T> SObjectCreation for T
where
    T: for<'de> serde::Deserialize<'de>,
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
            }
            Ok(value)
        } else {
            Err(SalesforceError::UnknownError.into())
        }
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

impl SObjectCreation for SObject {
    fn from_value(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<SObject> {
        if let serde_json::Value::Object(content) = value {
            let mut ret = SObject::new(sobjecttype);
            for k in content.keys() {
                // Get the describe for this field.
                if k != "attributes" {
                    let describe = sobjecttype.get_describe().get_field(k).unwrap();

                    println!("Field: {:?}", k);
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

    // TODO: Blob

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

    #[test]
    fn test_datetimes_parse() -> Result<()> {
        assert!(
            DateTime::new(2021, 11, 19, 01, 51, 47, 323)?.to_string()
                == "2021-11-19T01:51:47.323+0000"
        );
        assert!(
            "2021-11-19T01:51:47.323+0000".parse::<DateTime>()?
                == DateTime::new(2021, 11, 19, 01, 51, 47, 323)?
        );
        Ok(())
    }

    #[test]
    fn test_datetimes_serde() -> Result<()> {
        assert!(
            serde_json::from_str::<DateTime>("\"2021-11-19T01:51:47.323+0000\"")?
                == DateTime::new(2021, 11, 19, 01, 51, 47, 323)?
        );
        assert!(
            serde_json::to_string(&DateTime::new(2021, 11, 19, 01, 51, 47, 323)?)?
                == "\"2021-11-19T01:51:47.323+0000\""
        );
        Ok(())
    }

    #[test]
    fn test_dates_parse() -> Result<()> {
        assert!("2021-11-15".parse::<Date>()? == Date::new(2021, 11, 15)?);
        assert!(Date::new(2021, 11, 15)?.to_string() == "2021-11-15");
        Ok(())
    }

    #[test]
    fn test_dates_serde() -> Result<()> {
        assert!(serde_json::from_str::<Date>("\"2021-11-15\"")? == Date::new(2021, 11, 15)?);
        assert!(serde_json::to_string(&Date::new(2021, 11, 15)?)? == "\"2021-11-15\"");
        Ok(())
    }

    #[test]
    fn test_times() {}
}
