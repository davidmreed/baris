
mod data;
pub use crate::data::{SalesforceId, SObject, SObjectType, FieldValue};
mod errors;
pub use crate::errors::SalesforceError;
mod api;
pub use crate::api::Connection;