use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum SalesforceError {
    InvalidIdError(String),
    RecordExistsError(),
    RecordDoesNotExistError(),
    SchemaError(String),
    GeneralError(String),
    CannotRefresh,
}

impl fmt::Display for SalesforceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SalesforceError::InvalidIdError(id) => write!(f, "Invalid Salesforce Id: {}", id),
            SalesforceError::RecordExistsError() => write!(f, "Cannot create record with an Id"),
            SalesforceError::RecordDoesNotExistError() => {
                write!(f, "Cannot update record without an Id")
            }
            SalesforceError::GeneralError(err) => write!(f, "General Salesforce error: {}", err),
            SalesforceError::SchemaError(err) => write!(f, "Schema error: {}", err),
            SalesforceError::CannotRefresh => write!(f, "Cannot refresh access token auth"),
        }
    }
}

impl Error for SalesforceError {}
