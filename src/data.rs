use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Error, Result};
use serde_derive::{Deserialize, Serialize};
use serde_json::json;
use tokio_stream::StreamExt;

use crate::rest::describe::SObjectDescribe;
use crate::rest::query::QueryRequest;
use crate::streams::BufferedLocatorStream;
use crate::Connection;

use super::errors::SalesforceError;
use super::rest::{
    SObjectCreateRequest, SObjectDeleteRequest, SObjectRetrieveRequest, SObjectUpdateRequest,
    SObjectUpsertRequest,
};

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(try_from = "String")]
pub struct SalesforceId {
    id: [u8; 18],
}

// TODO: store as 15-char and render as 18 on string conversion.
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
        SalesforceId::new(&value)
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

type DateTime = chrono::DateTime<chrono::Utc>;
type Time = chrono::NaiveTime;
type Date = chrono::NaiveDate;

#[derive(Debug)]
struct Address {
    pub street: String,
    pub city: String,
    pub zip_postal_code: String,
    pub state_province: String,
    pub country: String,
}

#[derive(Debug, Debug, PartialEq)]
pub enum FieldValue {
    Address(Address),
    Integer(i64),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(DateTime),
    Time(Time),
    Date(Date),
    Id(SalesforceId),
    Null,
}

impl FieldValue {
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

    pub fn get_soap_type(&self) -> SoapType {
        match &self {
            FieldValue::Integer(_) => SoapType::Integer,
            FieldValue::Double(_) => SoapType::Double,
            FieldValue::Boolean(_) => SoapType::Boolean,
            FieldValue::String(_) => SoapType::String,
            FieldValue::DateTime(_) => SoapType::DateTime,
            FieldValue::Time(_) => SoapType::Time,
            FieldValue::Date(_) => SoapType::Date,
            FieldValue::Id(_) => SoapType::Id,
            _ => SoapType::Any, // TODO: this is probably not an optimal solution for nulls.
        }
    }

    pub fn from_str(input: &str, field_type: &SoapType) -> Result<FieldValue> {
        match field_type {
            SoapType::Integer => Ok(FieldValue::Integer(input.parse()?)),
            SoapType::Double => Ok(FieldValue::Double(input.parse()?)),
            SoapType::Boolean => Ok(FieldValue::Boolean(input.parse()?)),
            SoapType::String => Ok(FieldValue::String(input.to_owned())),
            SoapType::DateTime => Ok(FieldValue::DateTime(input.to_owned())),
            SoapType::Time => Ok(FieldValue::Time(input.to_owned())),
            SoapType::Date => Ok(FieldValue::Date(input.to_owned())),
            SoapType::Id => Ok(FieldValue::Id(input.try_into()?)),
            _ => panic!("Unsupported type"), // TODO
        }
    }
}

pub trait SObjectCollection {
    fn create(&mut self, all_or_none: bool) -> Result<()>;
    fn update(&self, all_or_none: bool) -> Result<()>;
    fn upsert(&mut self, external_id: &str, all_or_none: bool) -> Result<()>;
    fn delete(&mut self, all_or_none: bool) -> Result<()>;
    fn retrieve(ids: Vec<SalesforceId>, fields: Vec<String>) -> Result<Vec<Option<SObject>>>;
}

impl SObjectCollection for Vec<SObject> {
    fn create(&mut self, all_or_none: bool) -> Result<()> {
        todo!()
    }

    fn update(&self, all_or_none: bool) -> Result<()> {
        todo!()
    }

    fn upsert(&mut self, external_id: &str, all_or_none: bool) -> Result<()> {
        todo!()
    }

    fn delete(&mut self, all_or_none: bool) -> Result<()> {
        todo!()
    }

    fn retrieve(ids: Vec<SalesforceId>, fields: Vec<String>) -> Result<Vec<Option<SObject>>> {
        todo!()
    }
}

pub struct SObject {
    pub sobject_type: SObjectType,
    pub fields: HashMap<String, FieldValue>,
}

impl SObject {
    pub fn new(sobject_type: &SObjectType) -> SObject {
        SObject {
            sobject_type: sobject_type.clone(),
            fields: HashMap::new(),
        }
    }

    pub async fn query(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<BufferedLocatorStream> {
        let request = QueryRequest::new(sobject_type, query, all);

        Ok(conn.execute(&request).await?)
    }

    pub async fn query_vec(
        conn: &Connection,
        sobject_type: &SObjectType,
        query: &str,
        all: bool,
    ) -> Result<Vec<SObject>> {
        Ok(Self::query(conn, sobject_type, query, all)
            .await?
            .collect::<Result<Vec<SObject>>>()
            .await?)
    }

    pub async fn create(&mut self, conn: &Connection) -> Result<()> {
        let request = SObjectCreateRequest::new(self)?;
        let result = conn.execute(&request).await?;

        if result.success {
            self.set_id(result.id.unwrap())?;
            Ok(())
        } else {
            Err(result.into())
        }
    }

    pub async fn update(&mut self, conn: &Connection) -> Result<()> {
        conn.execute(&SObjectUpdateRequest::new(self)?).await
    }

    pub async fn upsert(&mut self, conn: &Connection, external_id: &str) -> Result<()> {
        conn.execute(&SObjectUpsertRequest::new(self, external_id)?)
            .await?
            .into()
    }

    pub async fn delete(&mut self, conn: &Connection) -> Result<()> {
        let result = conn.execute(&SObjectDeleteRequest::new(self)?).await;

        if let Ok(_) = &result {
            self.put("id", FieldValue::Null)?;
        }

        result
    }

    pub async fn retrieve(
        conn: &Connection,
        sobject_type: &SObjectType,
        id: SalesforceId,
    ) -> Result<SObject> {
        conn.execute(&SObjectRetrieveRequest::new(id, sobject_type))
            .await
    }

    pub fn put(&mut self, key: &str, val: FieldValue) -> Result<()> {
        // Locate the describe for this field.
        let describe = self.sobject_type.get_describe().get_field(key);

        if describe.is_none() {
            return Err(SalesforceError::SchemaError(format!(
                "Field {} does not exist or is not accessible",
                key
            ))
            .into());
        }

        let describe = describe.unwrap();

        // Validate that the provided value matches the type of this field
        // and satisfies any constraints we can check locally.
        let soap_type = val.get_soap_type();
        if describe.soap_type != soap_type && soap_type != SoapType::Any {
            Err(SalesforceError::SchemaError(format!(
                "Wrong type of value ({:?}) for field {}.{} (type {:?})",
                val.get_soap_type(),
                self.sobject_type.get_api_name(),
                key,
                describe.soap_type
            ))
            .into())
        } else {
            self.fields.insert(key.to_lowercase(), val);
            Ok(())
        }
    }

    pub fn get_id(&self) -> Option<&SalesforceId> {
        if let Some(FieldValue::Id(id)) = self.get("id") {
            Some(id)
        } else {
            None
        }
    }

    pub fn set_id(&mut self, id: SalesforceId) -> Result<()> {
        self.put("id", FieldValue::Id(id))
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(&key.to_lowercase())
    }

    pub fn get_binary_blob(&self, _key: &str) {
        unimplemented!();
    }

    pub fn has_reference_parameters(&self) -> bool {
        false
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
                )?;
            }
        }

        Ok(ret)
    }

    pub fn from_json(value: &serde_json::Value, sobjecttype: &SObjectType) -> Result<SObject> {
        if let serde_json::Value::Object(content) = value {
            let mut ret = SObject::new(sobjecttype);
            for k in content.keys() {
                // Get the describe for this field.
                if k != "attributes" {
                    let describe = sobjecttype.get_describe().get_field(k).unwrap();

                    ret.put(
                        &k.to_lowercase(),
                        FieldValue::from_json(value.get(k).unwrap(), describe.soap_type)?,
                    )?;
                }
            }
            Ok(ret)
        } else {
            Err(Error::new(SalesforceError::GeneralError(
                "Invalid record JSON".to_string(),
            )))
        }
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.into());
        }

        serde_json::Value::Object(map)
    }

    // TODO: clean up these three methods
    pub(crate) fn to_json_without_id(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            if k != "id" {
                map.insert(k.to_string(), v.into());
            }
        }

        serde_json::Value::Object(map)
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
            FieldValue::DateTime(i) => serde_json::Value::String(i.clone()),
            FieldValue::Time(i) => serde_json::Value::String(i.clone()),
            FieldValue::Date(i) => serde_json::Value::String(i.clone()),
            FieldValue::Id(i) => serde_json::Value::String(i.to_string()),
            FieldValue::Null => serde_json::Value::Null,
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
            FieldValue::DateTime(i) => i.clone(),
            FieldValue::Time(i) => i.clone(),
            FieldValue::Date(i) => i.clone(),
            FieldValue::Id(i) => i.to_string(),
            FieldValue::Null => "".to_string(),
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
                    return Ok(FieldValue::Date(b.to_string()));
                }
            }
            SoapType::DateTime => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::DateTime(b.to_string()));
                }
            }
            SoapType::Time => {
                if let serde_json::Value::String(b) = value {
                    return Ok(FieldValue::Time(b.to_string()));
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
