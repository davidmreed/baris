use std::fmt;
use std::error::Error;

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
