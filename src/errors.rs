use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum SalesforceError {
    InvalidIdError(String),
    CreateExistingRecord(),
    SchemaError(String),
    GeneralError(String)
} 

impl fmt::Display for SalesforceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SalesforceError::InvalidIdError(id) => write!(f, "Invalid Salesforce Id: {}", id),
            SalesforceError::CreateExistingRecord() => write!(f, "Cannot create record with an Id"),
            SalesforceError::GeneralError(err) => write!(f, "General Salesforce error: {}", err),
            SalesforceError::SchemaError(err) => write!(f, "Schema error: {}", err)
        }
         
    }
}

impl Error for SalesforceError {
}
