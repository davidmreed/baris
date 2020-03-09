extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate reqwest;

use std::collections::HashMap;
use std::fmt;
use std::error::Error;
use std::rc::Rc;
use std::cell::RefCell;

use super::data::{SalesforceId, SObject, SObjectType, FieldValue, SObjectDescribe, SoapType};
use super::errors::SalesforceError;

use reqwest::blocking::Client;
use reqwest::header;
use serde_derive::Deserialize;
use serde_json::Value;


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryResult {
    total_size: usize,
    done: bool,
    records: Vec<serde_json::Value>,
    next_records_url: Option<String>,
}

pub struct QueryIterator<'a> {
    result: QueryResult,
    conn: &'a Connection,
    sobjecttype: &'a Rc<SObjectType>,
    index: usize,
}

impl QueryIterator<'_> {
    fn new<'a>(result: QueryResult, conn: &'a Connection, sobjecttype: &'a Rc<SObjectType>) -> QueryIterator<'a> {
        QueryIterator {
            result,
            conn,
            sobjecttype,
            index: 0
        }
    }
}

impl Iterator for QueryIterator<'_> {
    type Item = Result<SObject, Box<dyn Error>>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.result.total_size, Some(self.result.total_size))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.result.records.len() && !self.result.done {
            // Attempt to fetch the next block of records.
            if let Some(next_url) = &self.result.next_records_url {
                let request_url = format!("{}/{}", self.conn.instance_url, next_url);
                self.result = self.conn.client.get(&request_url)
                    .send().unwrap()
                    .json().unwrap(); // FIXME: better error propagation.
                self.index = 0;
            }
        }

        if self.index < self.result.records.len() {
            self.index += 1;

            Some(SObject::from_json(
                &self.result.records[self.index - 1],
                self.sobjecttype
            ))    
        } else {
            None
        }
    }
}

impl ExactSizeIterator for QueryIterator<'_> {
    fn len(&self) -> usize {
        self.result.total_size
    }
}

pub struct Connection {
    instance_url: String,
    api_version: String,
    sobject_types: RefCell<HashMap<String, Rc<SObjectType>>>,
    client: Client
}

impl Connection {
    pub fn new(sid: &str, instance_url: &str, api_version: &str) -> Result<Connection, Box<dyn Error>> {
        let mut headers = header::HeaderMap::new();

        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&format!("Bearer {}", sid))?
        );

        Ok(Connection {
            api_version: api_version.to_string(),
            instance_url: instance_url.to_string(),
            sobject_types: RefCell::new(HashMap::new()),
            client: Client::builder().default_headers(headers).build()?
        })
    }

    pub fn get_type(&self, type_name: &str) -> Result<Rc<SObjectType>, Box<dyn Error>> {
        if !self.sobject_types.borrow().contains_key(type_name) {
            // Pull the Describe information for this sObject
            let request_url = format!(
                "{}/services/data/{}/sobjects/{}/describe",
                self.instance_url,
                self.api_version,
                type_name
            );
            let describe: SObjectDescribe = self.client.get(&request_url).send()?.json()?;
    
            self.sobject_types.borrow_mut().insert(
                type_name.to_string(),
                Rc::new(SObjectType::new(type_name.to_string(), describe))
            );
        }

        match self.sobject_types.borrow().get(type_name) {
            Some(rc) => Ok(Rc::clone(rc)),
            None => Err(Box::new(SalesforceError::GeneralError("sObject Type not found".to_string())))
        }
    }

    pub fn create(&self, obj: &mut SObject) -> Result<(), Box<dyn Error>> {
        if obj.get_id().is_some() {
            return Err(Box::new(SalesforceError::CreateExistingRecord()))
        }

        let request_url = format!(
            "{}/services/data/{}/sobjects/{}/",
            self.instance_url,
            self.api_version,
            obj.sobjecttype.get_api_name()
        );
        let result: CreateResult = self.client.post(&request_url)
            .json(&obj.to_json())
            .send()?
            .json()?;

        if result.success {
            obj.put("id", FieldValue::Id(SalesforceId::new(&result.id)?))?;

            Ok(())
        } else {
            Err(Box::new(result)) 
        }
    }

    pub fn update(&self, obj: &SObject) -> Result<(), Box<dyn Error>> {
       unimplemented!(); 
    }

    pub fn upsert(&self, obj: &mut SObject) -> Result<(), Box<dyn Error>> {
        unimplemented!();
    }

    pub fn delete(&self, obj: SObject) -> Result<(), Box<dyn Error>> {
        unimplemented!();
    }

    pub fn creates(&self, objs: &mut Vec<SObject>) -> Vec<Result<(), Box<dyn Error>>> {
        unimplemented!();
    }

    pub fn updates(&self, objs: &Vec<SObject>) -> Vec<Result<(), Box<dyn Error>>> {
        unimplemented!();
    }

    pub fn upserts(&self, objs: &mut Vec<SObject>) -> Vec<Result<(), Box<dyn Error>>> {
        unimplemented!();
    }

    pub fn deletes(&self, objs: Vec<SObject>) -> Vec<Result<(), Box<dyn Error>>> {
        unimplemented!();
    }

    fn execute_query<'a>(&'a self, sobjecttype: &'a Rc<SObjectType>, query: &str, endpoint: &str) -> Result<QueryIterator<'a>, Box<dyn Error>> {
        let request_url = format!(
            "{}/services/data/{}/{}/",
            self.instance_url,
            self.api_version,
            endpoint
        );
        let result: QueryResult = self.client.get(&request_url).query(&[("q", query)]).send()?.json()?;

        Ok(QueryIterator::new(result, self, sobjecttype))
    }

    pub fn query<'a>(&'a self, sobjecttype: &'a Rc<SObjectType>, query: &str) -> Result<QueryIterator<'a>, Box<dyn Error>> {
        self.execute_query(sobjecttype, query, "query")
    }

    pub fn query_all<'a>(&'a self, sobjecttype: &'a Rc<SObjectType>, query: &str) -> Result<QueryIterator<'a>, Box<dyn Error>> {
        self.execute_query(sobjecttype, query, "queryAll")
    }

    pub fn retrieve(&self, id: &SalesforceId, sobjecttype: &Rc<SObjectType>, fields: &Vec<String>) -> Result<SObject, Box<dyn Error>> {
        let request_url = format!(
            "{}/services/data/{}/sobjects/{}/{}/",
            self.instance_url,
            self.api_version,
            sobjecttype.get_api_name(),
            id
        );
        SObject::from_json(
            &self.client.get(&request_url).send()?.json()?,
            sobjecttype
        )
    }

    pub fn retrieves(&self, ids: &Vec<SalesforceId>, fields: &Vec<String>) -> Result<Vec<SObject>, Box<dyn Error>> {
        unimplemented!();
    }
}

#[derive(Debug, Deserialize)]
struct CreateResult {
    id: String,
    errors: Vec<String>,
    success: bool
}

impl fmt::Display for CreateResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.success {
            write!(f, "Success ({})", self.id)
        } else {
            write!(f, "DML error: {}", self.errors.join("\n"))
        }
    }
}

impl Error for CreateResult {
}

impl FieldValue {
    fn to_json(&self) -> serde_json::Value {
        match &self {
            FieldValue::Integer(i) => serde_json::Value::Number(serde_json::Number::from_f64(*i as f64).unwrap()),
            FieldValue::Double(i) => serde_json::Value::Number(serde_json::Number::from_f64(*i).unwrap()),
            FieldValue::Boolean(i) => serde_json::Value::Bool(*i),
            FieldValue::String(i) => serde_json::Value::String(i.clone()),
            FieldValue::DateTime(i) => serde_json::Value::String(i.clone()),
            FieldValue::Time(i) => serde_json::Value::String(i.clone()),
            FieldValue::Date(i) => serde_json::Value::String(i.clone()),
            FieldValue::Id(i) => serde_json::Value::String(i.to_string())
        }
    }

    fn from_json(value: &serde_json::Value, soap_type: SoapType) -> Result<FieldValue, Box<dyn Error>> {
        match soap_type {
            SoapType::Address | SoapType::Any | SoapType::Blob => { panic!("Not supported") },
            SoapType::Boolean => { if let serde_json::Value::Bool(b) = value { return Ok(FieldValue::Boolean(*b)); } },
            SoapType::Date => { if let serde_json::Value::String(b) = value { return Ok(FieldValue::Date(b.to_string())); } },
            SoapType::DateTime => { if let serde_json::Value::String(b) = value { return Ok(FieldValue::DateTime(b.to_string())); } },
            SoapType::Time => { if let serde_json::Value::String(b) = value { return Ok(FieldValue::Time(b.to_string())); } },
            SoapType::Double => { if let serde_json::Value::Number(b) = value { return Ok(FieldValue::Double(b.as_f64().unwrap())); } },
            SoapType::Integer => { if let serde_json::Value::Number(b) = value { return Ok(FieldValue::Integer(b.as_i64().unwrap())); } },
            SoapType::Id => { if let serde_json::Value::String(b) = value { return Ok(FieldValue::Id(SalesforceId::new(b)?)); } },
            SoapType::String => { if let serde_json::Value::String(b) = value { return Ok(FieldValue::String(b.to_string())); } }
        }

        return Err(Box::new(SalesforceError::SchemaError("Unable to convert value from JSON".to_string())));
    }
}

impl SObject {
    fn from_json(value: &serde_json::Value, sobjecttype: &Rc<SObjectType>) -> Result<SObject, Box<dyn Error>> {
        let mut ret = SObject::new(sobjecttype);

        if let Value::Object(content) = value {
            for k in content.keys() {
                // Get the describe for this field.
                if k != "attributes" {
                    let describe = sobjecttype.get_describe().get_field(k).unwrap();

                    ret.put(k, FieldValue::from_json(value.get(k).unwrap(), describe.soap_type)?)?;
                }
            }
        } else {
            return Err(Box::new(SalesforceError::GeneralError("Invalid record JSON".to_string())))
        }

        Ok(ret)
    }

    fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for (k, v) in self.fields.iter() {
            map.insert(k.to_string(), v.to_json());
        }

        serde_json::Value::Object(map)
    }
}