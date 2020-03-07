extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate reqwest;

use std::error::Error;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use reqwest::blocking::Client;
use reqwest::header;
use serde_derive::Deserialize;
use serde_json::Value;

struct SObjectType {
    api_name: String
}
impl fmt::Display for SObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.api_name)
    }
}

#[derive(Debug)]
pub enum SalesforceError {
    InvalidIdError(String),
    CreateExistingRecord(),
    GeneralError(String)
} 

impl fmt::Display for SalesforceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SalesforceError::InvalidIdError(id) => write!(f, "Invalid Salesforce Id: {}", id),
            SalesforceError::CreateExistingRecord() => write!(f, "Cannot create record with an Id"),
            SalesforceError::GeneralError(err) => write!(f, "General Salesforce error: {}", err)
        }
         
    }
}

impl Error for SalesforceError {
}


#[derive(Debug)]
pub struct SalesforceId {
    id: [u8; 18],
}

impl SalesforceId {
    pub fn new(id: &str) -> Result<SalesforceId, SalesforceError> {
        const ALNUMS: &[u8] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345".as_bytes();

        if id.len() != 15 && id.len() != 18 {
            return Err(SalesforceError::InvalidIdError(id.to_string()))
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
                return Err(SalesforceError::InvalidIdError(id.to_string()))
            }
        }
    
        // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
        full_id[15] = ALNUMS[bitstring & 0x1F] as u8;
        full_id[16] = ALNUMS[bitstring>>5 & 0x1F] as u8;
        full_id[17] = ALNUMS[bitstring>>10] as u8;

        Ok(SalesforceId { id: full_id })
    }
}

impl fmt::Display for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
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
        let mut ret = SObject::new(None, Rc::clone(&sobjecttype), HashMap::new());

        if let Value::Object(content) = value {
            for k in content.keys() {
                match content.get(k) {
                    Some(Value::Bool(b)) => ret.put(k, FieldValue::Checkbox(*b)),
                    Some(Value::String(s)) => ret.put(k, FieldValue::Text(s.clone())),
                    Some(Value::Number(n)) => ret.put(k, FieldValue::Number(n.as_f64().unwrap())),
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
    iterator: std::vec::Iter<serde_json::Value>,
}

impl QueryIterator<'_> {
    fn new<'a>(result: QueryResult, conn: &'a Connection, sobjecttype: Rc<SObjectType>) -> QueryIterator<'a> {
        QueryIterator {
            result,
            conn,
            sobjecttype,
            iterator: result.records.iter()
        }
    }

    fn process_next(&mut self) -> Option<Result<SObject, Box<dyn Error>>> {
        let res = self.iterator.next();

        match res {
            Some(sobj) => Some(SObject::from_query_result(&sobj, Rc::clone(&self.sobjecttype), self.conn)),
            None => None
        }
    }

    fn get_next_results(&mut self) {
        if let Some(next_url) = &self.result.nextRecordsUrl {
            let request_url = format!("{}/{}", self.conn.instance_url, next_url);
            self.result = self.conn.client.get(&request_url)
                .send().unwrap()
                .json().unwrap();
            self.iterator = self.result.records.iter();
        } 
    }
}

impl Iterator for QueryIterator<'_> {
    type Item = Result<SObject, Box<dyn Error>>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.result.totalSize, Some(self.result.totalSize))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.process_next();

        if res.is_none() && !self.result.done {
            self.get_next_results();
            self.process_next()
        } else {
            res
        }
    }
}

impl ExactSizeIterator for QueryIterator<'_> {
    fn len(&self) -> usize {
        self.result.totalSize
    }
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

    pub fn get_type(&mut self, type_name: &str) -> Option<&Rc<SObjectType>> {
        if !self.sobject_types.contains_key(type_name) {
            self.sobject_types.insert(type_name.to_string(), Rc::new(SObjectType { api_name: type_name.to_string()} ));
        }

        self.sobject_types.get(type_name)
    }

    pub fn create(&self, obj: &mut SObject) -> Result<(), Box<dyn Error>> {
        if let Some(id) = &obj.id {
            return Err(Box::new(SalesforceError::CreateExistingRecord))
        }

        let request_url = format!(
            "{}/services/data/{}/sobjects/{}/",
            self.instance_url,
            self.api_version,
            &obj.sobjecttype.api_name
        );
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

        Ok(QueryIterator { result: result, sobjecttype: Rc::clone(&sobjecttype), conn: self, iterator: result.records.into_iter()})
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
