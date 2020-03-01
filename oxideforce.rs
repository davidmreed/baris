extern crate serde;
extern crate serde_derive;
extern crate serde_json;
extern crate reqwest;

use reqwest::Client;
use reqwest::header;

#[derive(Deserialize)]
struct QueryResult {
    bool done,
    Vec<serde_json::Value> records,
    usize totalSize,
    String nextRecordsUrl,
}

pub struct QueryIterator {
    result: QueryResult,
    conn: Connection
}

impl Iterator for QueryIterator {
    type Item = SObject;

    pub fn size_hint(&self) -> (usize, Option<usize>) {
        (self.result.total_size, Some(self.result.total_size))
    }

    fn process_next(&mut self) {
        let res = self.result.records.next();

        match res {
            Some(sobj) => Some(SObject::from_value(sobj)),
            None => None
        }
    }
 
    pub fn next(&mut self) -> Option<Item> {
        let res = self.process_next();

        if let None = res && !self.result.done {
            let request_url = format!("{}/{}", self.conn.instance_url, self.result.nextRecordsUrl);
            self.result = reqwest::get(&request_url)?
                .query(&["query", query])
                .send()
                .await?
                .json()?

            self.process_next()
        } else {
            res
        }
    }
}

impl ExactSizeIterator for QueryIterator {
    fn len(&self) -> usize {
        self.total_size
    }
}

pub struct SalesforceId {
    id: String,
}

impl SalesforceId {
    const alnums: &str = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ012345';

    pub fn new(id: &str) -> Result<SalesforceId, &'static str> {
        if id.len() != 15 && id.len() != 18 {
            Err("Invalid Salesforce Id")
        }

        String full_id = String::with_capacity(18);
        full_id.push_str(id);

        if full_id.len() == 15 {
            let bitstring: u16 = 0;

            for i in 0..15 {
                if full_id[i] >= 'A' && full_id[i] <= 'Z':
                    bitstring |= 1 << i
            }
    
            // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
            full_id.push_str(alnums[bitstring & 0x1F])
            full_id.push_str(alnums[bitstring>>5 & 0x1F])
            full_id.push_str(alnums[bitstring>>10])
        }

        Ok(SalesforceId { full_id })
    }
}

pub struct FieldValue {
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
    pub type: &SObjectType,
    pub fields: HashMap<String, FieldValue>
}

impl SObject {
    pub fn new(id: Option<SalesforceId>, type: &SObjectType, fields: HashMap<String, FieldValue>) -> SObject {
        SObject { id, type, fields }
    }

    pub fn from_value(value: &serde_json::Value) -> SObject {

    }
}

pub struct SObjectType {
    api_name: String,
    describe_data: DescribeSobjectResult,
}

#[derive(Deserialize)]
pub struct CreateResult {
    id: SalesforceId,
    errors: Vec<String>,
    success: bool
}

pub struct Connection {
    sid: String,
    instance_url: String,
    api_version: String,
    sobject_types: HashMap<String, SObjectType>

}

impl Connection {
    pub fn new(sid: &str, api_version: &str, sandbox: bool) -> Connection {
        let mut headers = header::HeaderMap::new();

        headers.insert(header::AUTHORIZATION, header::HeaderValue::from_static(sid));

        Connection {
            sid,
            api_version,
            instance_url: format!("https://{}.salesforce.com", if sandbox { "test" } else { "login" }),
            HashMap::new(),
            Client::builder()
                .default_headers(headers)
                .build()?
        }
    }

    pub fn get_type(&mut self, type_name: &str) -> Option<&SObjectType> {
        if !self.sobject_types.contains(type_name) {

        }

        sobject_types.get(type_name)
    }

    pub fn create(obj: &mut SObject) -> Result<()>, Box<dyn Error>> {
        if let Some(id) = obj.id {
            Err("This object already has a Salesforce Id")
        }

        let request_url = format!("{}/services/data/{}/sobjects/{}/", self.instance_url, self.api_version, obj.type.api_name);
        let result: CreateResult = reqwest::get(&request_url)?.json()?

        if result.success {
            obj.id = result.id;

            Ok(())
        } else {
            Err() // FIXME: convert the creation errors.
        }
    }

    pub fn update(obj: &SObject) -> Result<(), Box<dyn Error>> {
        Err("Not implemented")
    }

    pub fn upsert(obj: &mut SObject) -> Result<(), Box<dyn Error>> {
        Err("Not implemented")
    }

    pub fn delete(obj: SObject) -> Result<(), Box<dyn Error>> {
        Err("Not implemented")
    }

    pub fn creates(objs: &mut Vec<SObject>) -> Vec<Result<()>, Box<dyn Error>>> {
        Err("Not implemented")
    }

    pub fn updates(objs: &Vec<SObject>) -> Vec<Result<()>, Box<dyn Error>>> {
        Err("Not implemented")
    }

    pub fn upserts(objs: &mut Vec<SObject>) -> Vec<Result<()>, Box<dyn Error>>> {
        Err("Not implemented")
    }

    pub fn deletes(objs: Vec<SObject>) -> Vec<Result<()>, Box<dyn Error>>> {
        Err("Not implemented")
    }

    pub fn query(query: &str) -> Result<QueryIterator, Box<dyn Error>> {
        let request_url = format!("{}/services/data/{}/query", self.instance_url, self.api_version, obj.type.api_name);
        let result: QueryResult = reqwest::get(&request_url)?
            .query(&["query", query])
            .send()
            .await?
            .json()?

        Ok(QueryIterator { result: result, conn: &self })
    }

    pub fn query_all(query: &str) -> Result<QueryIterator, Box<dyn Error>> {
        Err("Not implemented")
    }

    pub fn retrieve(id: &SalesforceId, fields: &Vec<String>) -> Result<SObject, Box<dyn Error>> {
        Err("Not implemented")
    }

    pub fn retrieves(ids: &Vec<SalesforceId>, fields: &Vec<String>) -> Result<Vec<SObject>, Box<dyn Error>> {
        Err("Not implemented")
    }
}