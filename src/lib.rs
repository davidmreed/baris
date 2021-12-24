#![feature(async_stream)]

pub mod data;
pub use crate::data::{FieldValue, SObject, SObjectType, SalesforceId};
mod errors;
mod streams;
pub use crate::errors::SalesforceError;
pub mod api;
pub use crate::api::Connection;
pub mod auth;
pub mod bulk;
pub mod rest;
pub mod tooling;

#[cfg(test)]
mod test_integration_base;

extern crate chrono;
extern crate csv;
