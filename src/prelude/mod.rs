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
pub use crate::data::sobjects::{FieldValue, SObject, SObjectType};
pub use crate::data::traits::{
    DynamicallyTypedSObject, SObjectDeserialization, SObjectRepresentation, SObjectSerialization,
    SingleTypedSObject, TypedSObject, SObjectBase, SObjectWithId
};

// REST
pub use crate::rest::collections::SObjectStream;
pub use crate::rest::collections::traits::{SObjectCollectionCreateable, SObjectCollectionUpdateable, SObjectCollectionUpsertable, SObjectCollectionDeleteable};
pub use crate::rest::rows::traits::{SObjectRowCreateable, SObjectRowUpdateable, SObjectRowUpsertable, SObjectRowDeletable, SObjectSingleTypedRetrieval, SObjectDynamicallyTypedRetrieval};
pub use crate::rest::composite::CompositeRequest;
pub use crate::rest::query::traits::{Queryable, QueryableSingleType};
pub use crate::rest::query::AggregateResult;

// Tooling

pub use crate::tooling;

// Errors

pub use crate::errors::SalesforceError;
