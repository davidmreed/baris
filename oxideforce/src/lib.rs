#![feature(async_stream)]

pub mod data;
pub use crate::data::{FieldValue, SObject, SObjectType, SalesforceId};
mod errors;
pub use crate::errors::SalesforceError;
pub mod api;
pub use crate::api::Connection;
pub mod bulk;
pub mod rest;

extern crate chrono;
extern crate csv;
