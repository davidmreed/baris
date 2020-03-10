mod data;
pub use crate::data::{FieldValue, SObject, SObjectType, SalesforceId};
mod errors;
pub use crate::errors::SalesforceError;
mod api;
pub use crate::api::Connection;