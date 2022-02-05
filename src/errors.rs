use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum SalesforceError {
    InvalidIdError(String),
    RecordExistsError,
    RecordDoesNotExistError,
    SchemaError(String),
    GeneralError(String),
    CannotRefresh,
    SObjectCollectionError,
    ResponseBodyExpected,
    UnknownError,
    NotAuthenticated,
    DateTimeError,
    UnsupportedId,
}

impl fmt::Display for SalesforceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SalesforceError::InvalidIdError(id) => write!(f, "Invalid Salesforce Id: {}", id),
            SalesforceError::RecordExistsError => write!(f, "Cannot create record with an Id"),
            SalesforceError::RecordDoesNotExistError => {
                write!(f, "Cannot perform this operation on a record without an Id")
            }
            SalesforceError::GeneralError(err) => write!(f, "General Salesforce error: {}", err),
            SalesforceError::SchemaError(err) => write!(f, "Schema error: {}", err),
            SalesforceError::CannotRefresh => write!(f, "Cannot refresh access token auth"),
            SalesforceError::SObjectCollectionError => {
                write!(f, "An sObject Collections API limitation was breached")
            }
            SalesforceError::ResponseBodyExpected => {
                write!(f, "A response body was expected, but is not present")
            }
            SalesforceError::UnknownError => {
                write!(f, "An unknown error occurred")
            }
            SalesforceError::NotAuthenticated => {
                write!(
                    f,
                    "Data cannot be obtained until an authorization refresh is executed"
                )
            }
            SalesforceError::DateTimeError => {
                write!(f, "An date, time, or datetime value could not be created")
            }
            SalesforceError::UnsupportedId => {
                write!(
                    f,
                    "An unsupported Id type (such as a null or composite reference) was provided"
                )
            }
        }
    }
}

impl Error for SalesforceError {}
