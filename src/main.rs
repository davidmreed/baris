extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate reqwest;

use std::fmt;
use std::fmt::Display;
use std::error::Error;
use std::collections::HashMap;
use serde_derive::Deserialize;
use serde_json::Value;
use reqwest::blocking::Client;
use reqwest::header;

type SObjectType = String;

#[derive(Deserialize)]
struct QueryResult {
    done: bool,
    records: Vec<serde_json::Value>,
    totalSize: usize,
    nextRecordsUrl: Option<String>,
}

pub struct QueryIterator<'a> {
    result: QueryResult,
    conn: &'a Connection,
    sobjecttype: Rc<SObjectType>,
    iterator: std::vec::IntoIter<serde_json::Value>,
}

impl QueryIterator<'_> {
    fn new<'a>(result: QueryResult, conn: &'a Connection, sobjecttype: Rc<SObjectType>) -> QueryIterator<'a> {
        QueryIterator {
            result,
            conn,
            sobjecttype,
            iterator: result.records.into_iter()
        }
    }

    fn process_next(&mut self) -> Option<SObject> {
        let res = self.iterator.next();

        match res {
            Some(sobj) => Some(SObject::from_query_result(&sobj, self.sobjecttype, self.conn).ok()),
            None => None
        }
    }
}

impl Iterator for QueryIterator<'_> {
    type Item = SObject;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.result.totalSize, Some(self.result.totalSize))
    }
 
    fn next(&mut self) -> Option<Self::Item> {
        let res = self.process_next();

        if res.is_none() && !self.result.done {
            if let Some(next_url) = self.result.nextRecordsUrl {
                let request_url = format!("{}/{}", self.conn.instance_url, next_url);
                self.result = self.conn.client.get(&request_url)
                    .send()?
                    .json()?;
                self.iterator = self.result.records.into_iter();
            } 
            self.process_next()
        } else {
            res
        }
    }
}

impl ExactSizeIterator for QueryIterator<'_> {
    fn len(&self) -> usize {
        self.totalSize
    }
}

#[derive(Debug)]
pub struct SalesforceId {
    id: String,
}

impl SalesforceId {

    pub fn new(id: &str) -> Result<SalesforceId, &'static str> {
        const alnums: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345";
        if id.len() != 15 && id.len() != 18 {
            return Err("Invalid Salesforce Id")
        }

        let full_id = String::with_capacity(18);
        full_id.push_str(id);

        if full_id.len() == 15 {
            let bitstring: usize = 0;

            for i in 0..15 {
                if full_id[i] >= 'A' && full_id[i] <= 'Z' {
                    bitstring |= 1 << i
                }
            }
    
            // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
            full_id.push_str(alnums[bitstring & 0x1F]);
            full_id.push_str(alnums[bitstring>>5 & 0x1F]);
            full_id.push_str(alnums[bitstring>>10]);
        }

        Ok(SalesforceId { id: full_id })
    }
}

impl fmt::Display for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

pub enum FieldValue {
    Integer(i64),
    Number(f64),
    Checkbox(bool),
    Text(String),
    DateTime(String),
    Time(String),
    Date(String),
    Reference(SalesforceId)
}

pub struct SObject {
    pub id: Option<SalesforceId>,
    pub sobjecttype: Rc<SObjectType>,
    pub fields: HashMap<String, FieldValue>
}

impl SObject {
    pub fn new(id: Option<SalesforceId>, sobjecttype: Rc<SObjectType>, fields: HashMap<String, FieldValue>) -> SObject {
        SObject { id, sobjecttype, fields }
    }

    pub fn put(&mut self, key: &str, val: FieldValue) {
        self.fields.insert(key.to_string(), val);
    }

    fn from_query_result(value: &serde_json::Value, sobjecttype: Rc<SObjectType>, conn: &Connection) -> Result<SObject, Box<dyn Error>> {
        let ret = SObject::new(None, Rc::clone(sobjecttype), HashMap::new());

        if let Value::Object(content) = value {
            for k in content.keys() {
                match content.get(k)? {
                    Value::Bool(b) => ret.put(k, FieldValue::Checkbox(*b)),
                    Value::String(s) => ret.put(k, FieldValue::Text(s.clone())),
                    Value::Number(n) => ret.put(k, FieldValue::Number(n.as_f64()?)),
                    _ => {}
                }
            }
        } else {
            panic!("Query result data is not in expected format");
        }

        Ok(ret)
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateResult {
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

#[derive(Debug, Display, Error)]
pub struct SalesforceError {
    error: String,
}

pub struct Connection {
    sid: String,
    instance_url: String,
    api_version: String,
    sobject_types: HashMap<String, Rc<SObjectType>>,
    client: Client
}

impl Connection {
    pub fn new(sid: &str, api_version: &str, sandbox: bool) -> Result<Connection, Box<dyn Error>> {
        let mut headers = header::HeaderMap::new();

        headers.insert(header::AUTHORIZATION, header::HeaderValue::from_str(sid)?);

        Ok(Connection {
            sid: sid.to_string(),
            api_version: api_version.to_string(),
            instance_url: format!("https://{}.salesforce.com", if sandbox { "test" } else { "login" }),
            sobject_types: HashMap::new(),
            client: Client::builder()
                .default_headers(headers)
                .build()?
        })
    }

    pub fn get_type(&mut self, type_name: &str) -> Option<&SObjectType> {
        if !self.sobject_types.contains_key(type_name) {
            self.sobject_types.insert(type_name.to_string(), Rc::new(type_name.to_string()));
        }

        self.sobject_types.get(type_name)
    }

    pub fn create(&self, obj: &mut SObject) -> Result<(), Box<dyn Error>> {
        if let Some(id) = obj.id {
            return Err(Box::new(SalesforceError { error: "This object already has a Salesforce Id" }))
        }

        let request_url = format!("{}/services/data/{}/sobjects/{}/", self.instance_url, self.api_version, obj.sobjecttype);
        let result: CreateResult = self.client.get(&request_url).send()?.json()?;

        if result.success {
            obj.id = Some(SalesforceId::new(&result.id)?);

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

    pub fn query(&self, sobjecttype: Rc<SObjectType>, query: &str) -> Result<QueryIterator, Box<dyn Error>> {
        let request_url = format!(
            "{}/services/data/{}/query",
            self.instance_url,
            self.api_version
        );
        let result: QueryResult = self.client.get(&request_url)
            .query(&["query", query])
            .send()?
            .json()?;

        Ok(QueryIterator { result: result, Rc::clone(sobjecttype), conn: self, iterator: result.records.iter()})
    }

    pub fn query_all(&self, query: &str) -> Result<QueryIterator, Box<dyn Error>> {
        unimplemented!();
    }

    pub fn retrieve(&self, id: &SalesforceId, fields: &Vec<String>) -> Result<SObject, Box<dyn Error>> {
        unimplemented!();
    }

    pub fn retrieves(&self, ids: &Vec<SalesforceId>, fields: &Vec<String>) -> Result<Vec<SObject>, Box<dyn Error>> {
        unimplemented!();
    }
}
