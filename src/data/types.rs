use std::{
    convert::{Infallible, TryFrom, TryInto},
    fmt::{self, Display},
    ops::Deref,
    pin::Pin,
    str::FromStr,
};

use anyhow::Result;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use futures::Stream;
use serde::{Serialize, Serializer};
use serde_derive::{Deserialize, Serialize};

use crate::{rest::rows::BlobRetrieveRequest, Connection, SalesforceError};

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct SalesforceId {
    id: [u8; 18],
}

impl SalesforceId {
    pub fn new(id: &str) -> Result<SalesforceId, SalesforceError> {
        const ALNUMS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";

        if id.len() != 15 && id.len() != 18 {
            return Err(SalesforceError::InvalidIdError(id.to_string()));
        }

        let mut full_id: [u8; 18] = [0; 18];
        let mut bitstring: usize = 0;

        // TODO: this might panic if `id` is valid UTF-8 but not a valid Id.
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
        // Cannot panic; Ids are guaranteed to be valid UTF-8
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
    }
}

impl fmt::Display for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Cannot panic; Ids are guaranteed to be valid UTF-8
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
    }
}

impl From<SalesforceId> for String {
    fn from(value: SalesforceId) -> String {
        value.to_string()
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
#[serde(try_from = "String")]
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

impl TryFrom<String> for DateTime {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        // Salesforce's version of RFC3339 doesn't include a colon as required by the standard,
        // giving +0000 instead of the expected +00:00

        Ok(DateTime {
            0: chrono::DateTime::parse_from_str(&value, "%Y-%m-%dT%H:%M:%S%.3f%z")?
                .with_timezone(&Utc),
        })
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.0.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
        )
    }
}

impl FromStr for DateTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        s.to_owned().try_into()
    }
}

// TODO: can we handle this with a Serde attribute like SalesforceId?
impl Serialize for DateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
#[serde(try_from = "String")]
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

impl TryFrom<String> for Time {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Time {
            0: chrono::NaiveTime::parse_from_str(&value, "%H:%M:%S%.3fZ")?,
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
        s.to_owned().try_into()
    }
}

// TODO: Serde attribute instead?
impl Serialize for Time {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
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

impl TryFrom<String> for Date {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Date {
            0: chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")?,
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
        s.to_owned().try_into()
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct Blob(String);

// TODO: can we elide the reqwest reference in our public API via a stream adapter?
impl Blob {
    pub async fn stream(
        &self,
        conn: &Connection,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>>>>> {
        Ok(conn
            .execute_raw_request(&BlobRetrieveRequest::new(self.0.clone()))
            .await?)
    }
}

impl Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for Blob {
    type Error = Infallible;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Ok(Blob { 0: value })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Geolocation {
    pub latitude: f64,
    pub longitude: f64,
}
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

#[derive(Debug, Deserialize, PartialEq, Copy, Clone)]
pub enum SoapType {
    #[serde(rename = "urn:address")]
    Address,
    #[serde(rename = "xsd:anyType")]
    Any,
    #[serde(rename = "xsd:base64Binary")]
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
    #[serde(rename = "urn:location")]
    Geolocation,
    #[serde(rename = "xsd:string")]
    String,
    #[serde(rename = "xsd:time")]
    Time,
}
