pub use crate::api::Connection;
// Typed Bulk traits
pub use crate::bulk::v2::traits::{
    BulkDeletable, BulkInsertable, BulkQueryable, BulkUpdateable, BulkUpsertable,
};
// Untyped Bulk traits
pub use crate::bulk::v2::traits::{
    SingleTypeBulkDeletable, SingleTypeBulkInsertable, SingleTypeBulkQueryable,
    SingleTypeBulkUpdateable, SingleTypeBulkUpsertable,
};

// Data
pub use crate::data::types::{SalesforceId, Geolocation, Address, DateTime, Date, Time};
pub use crate::data::sobjects::{FieldValue, SObject, SObjectType};
pub use crate::data::traits::{
    DynamicallyTypedSObject, SObjectBase, SObjectDeserialization, SObjectRepresentation,
    SObjectSerialization, SObjectWithId, SingleTypedSObject, TypedSObject,
};

// REST
pub use crate::rest::collections::traits::{
    SObjectCollectionCreateable, SObjectCollectionDeleteable, SObjectCollectionUpdateable,
    SObjectCollectionUpsertable,
};
pub use crate::rest::collections::SObjectStream;
pub use crate::rest::composite::CompositeRequest;
pub use crate::rest::query::traits::{Queryable, QueryableSingleType};
pub use crate::rest::query::AggregateResult;
pub use crate::rest::rows::traits::{
    SObjectDynamicallyTypedRetrieval, SObjectRowCreateable, SObjectRowDeletable,
    SObjectRowUpdateable, SObjectRowUpsertable, SObjectSingleTypedRetrieval,
};

// Tooling
pub use crate::tooling;

// Errors
pub use crate::errors::SalesforceError;
