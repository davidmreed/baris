#![feature(async_stream)]

mod data;
pub use crate::data::{FieldValue, SObject, SObjectType, SalesforceId};
mod errors;
pub use crate::errors::SalesforceError;
mod api;
pub use crate::api::Connection;
mod bulk;
mod rest;

extern crate chrono;
extern crate csv;
