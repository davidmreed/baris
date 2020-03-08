use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use super::errors::SalesforceError;

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

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(key)
    }
}

pub struct SObjectType {
    pub api_name: String
}

impl fmt::Display for SObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.api_name)
    }
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
