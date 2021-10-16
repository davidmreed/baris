extern crate reqwest;
extern crate serde;
extern crate serde_derive;
extern crate serde_json;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::data::{FieldValue, SObject, SObjectDescribe, SObjectType, SalesforceId, SoapType};
use super::errors::SalesforceError;

use crate::rest::SObjectDescribeRequest;

use anyhow::{Error, Result};
use reqwest::{header, Client, Method};
use serde::de::DeserializeOwned;
use serde_json::Value;

pub trait SalesforceRequest {
    type ReturnValue;

    fn get_body(&self) -> Option<Value> {
        None
    }

    fn get_url(&self) -> String;
    fn get_method(&self) -> Method;

    fn get_query_parameters(&self) -> Option<Value> {
        None
    }

    fn has_reference_parameters(&self) -> bool {
        false
    }

    fn get_result<T>(&self, conn: &Connection, body: &Value) -> Result<Self::ReturnValue>
    where
        T: DeserializeOwned,
        for<'de> <Self as SalesforceRequest>::ReturnValue: serde::Deserialize<'de>,
    {
        // TODO: make this not clone
        Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
    }
}

pub trait CompositeFriendlyRequest {}

pub struct Connection {
    instance_url: String,
    api_version: String,
    sobject_types: RefCell<HashMap<String, Rc<SObjectType>>>,
    pub(crate) client: Client,
}

impl Connection {
    pub fn new(sid: &str, instance_url: &str, api_version: &str) -> Result<Connection> {
        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", sid))?,
        );

        Ok(Connection {
            api_version: api_version.to_string(),
            instance_url: instance_url.to_string(),
            sobject_types: RefCell::new(HashMap::new()),
            client: Client::builder().default_headers(headers).build()?,
        })
    }

    pub fn get_base_url(&self) -> String {
        format!("{}/services/data/{}", self.instance_url, self.api_version)
    }

    pub async fn get_type(&self, type_name: &str) -> Result<Rc<SObjectType>> {
        if !self.sobject_types.borrow().contains_key(type_name) {
            // Pull the Describe information for this sObject
            let describe: SObjectDescribe = self
                .execute(&SObjectDescribeRequest::new(type_name))
                .await?;
            self.sobject_types.borrow_mut().insert(
                type_name.to_string(),
                Rc::new(SObjectType::new(type_name.to_string(), describe)),
            );
        }

        match self.sobject_types.borrow().get(type_name) {
            Some(rc) => Ok(Rc::clone(rc)),
            None => Err(Error::new(SalesforceError::GeneralError(
                "sObject Type not found".to_string(),
            ))),
        }
    }

    pub async fn execute<K, T>(&self, request: &K) -> Result<T>
    where
        K: SalesforceRequest<ReturnValue = T>,
        T: DeserializeOwned,
    {
        let url = format!("{}{}", self.get_base_url(), request.get_url());
        let mut builder = self.client.request(request.get_method(), &url);
        let method = request.get_method();

        if method == Method::POST || method == Method::PUT || method == Method::PATCH {
            if let Some(body) = request.get_body() {
                builder = builder.json(&body);
            }
        }

        if let Some(params) = request.get_query_parameters() {
            builder = builder.query(&params);
        }

        let result = builder.send().await?.json().await?;

        Ok(request.get_result::<T>(&self, &result)?)
    }
}

impl FieldValue {
    fn to_json(&self) -> serde_json::Value {
        match &self {
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
        }
    }

    fn from_json(value: &serde_json::Value, soap_type: SoapType) -> Result<FieldValue> {
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

impl SObject {
    pub(crate) fn from_csv(
        rec: &HashMap<String, String>,
        sobjecttype: &Rc<SObjectType>,
    ) -> Result<SObject> {
        let mut ret = SObject::new(sobjecttype);

        for k in rec.keys() {
            // Get the describe for this field.
            if k != "attributes" {
                let describe = sobjecttype.get_describe().get_field(k).unwrap();

                ret.put(
                    k,
                    FieldValue::from_str(rec.get(k).unwrap(), &describe.soap_type)?,
                )?;
            }
        }

        Ok(ret)
    }

    pub(crate) fn from_json(
        value: &serde_json::Value,
        sobjecttype: &Rc<SObjectType>,
    ) -> Result<SObject> {
        let mut ret = SObject::new(sobjecttype);

        if let Value::Object(content) = value {
            for k in content.keys() {
                // Get the describe for this field.
                if k != "attributes" {
                    let describe = sobjecttype.get_describe().get_field(k).unwrap();

                    ret.put(
                        k,
                        FieldValue::from_json(value.get(k).unwrap(), describe.soap_type)?,
                    )?;
                }
            }
        } else {
            return Err(Error::new(SalesforceError::GeneralError(
                "Invalid record JSON".to_string(),
            )));
        }

        Ok(ret)
    }

    pub(crate) fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.to_json());
        }

        serde_json::Value::Object(map)
    }
}
