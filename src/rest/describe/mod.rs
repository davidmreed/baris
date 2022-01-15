use std::collections::HashMap;

use anyhow::Result;
use reqwest::Method;
use serde_derive::Deserialize;
use serde_json::Value;

use crate::{
    api::Connection, api::SalesforceRequest, data::SalesforceId, data::SoapType,
    errors::SalesforceError,
};

#[cfg(test)]
mod test;

pub struct SObjectDescribeRequest {
    sobject: String,
}

impl SObjectDescribeRequest {
    pub fn new(sobject: &str) -> SObjectDescribeRequest {
        SObjectDescribeRequest {
            sobject: sobject.to_owned(),
        }
    }
}

impl SalesforceRequest for SObjectDescribeRequest {
    type ReturnValue = SObjectDescribe;

    fn get_url(&self) -> String {
        format!("sobjects/{}/describe", self.sobject)
    }

    fn get_method(&self) -> Method {
        Method::GET
    }

    fn get_result(&self, _conn: &Connection, body: Option<&Value>) -> Result<Self::ReturnValue> {
        if let Some(body) = body {
            Ok(serde_json::from_value::<Self::ReturnValue>(body.clone())?)
        } else {
            Err(SalesforceError::ResponseBodyExpected.into())
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldDescribe {
    pub aggregatable: bool,
    pub ai_prediction_field: bool,
    pub auto_number: bool,
    pub byte_length: u32,
    pub calculated: bool,
    pub calculated_formula: Option<String>,
    pub cascade_delete: bool,
    pub case_sensitive: bool,
    pub compound_field_name: Option<String>,
    pub controller_name: Option<String>,
    pub createable: bool,
    pub custom: bool,
    pub default_value: Option<serde_json::Value>,
    pub default_value_formula: Option<String>,
    pub defaulted_on_create: bool,
    pub dependent_picklist: bool,
    pub deprecated_and_hidden: bool,
    pub digits: u16,
    pub display_location_in_decimal: bool,
    pub encrypted: bool,
    pub external_id: bool,
    //pub extraTypeInfo: null
    pub filterable: bool,
    //filteredLookupInfo: null
    pub formula_treat_null_number_as_zero: bool,
    pub groupable: bool,
    pub high_scale_number: bool,
    pub html_formatted: bool,
    pub id_lookup: bool,
    pub inline_help_text: Option<String>,
    pub label: String,
    pub length: u32,
    //pub mask: null
    //pub maskType: null
    pub name: String,
    pub name_field: bool,
    pub name_pointing: bool,
    pub nillable: bool,
    pub permissionable: bool,
    pub picklist_values: Vec<PicklistValueDescribe>,
    pub polymorphic_foreign_key: bool,
    pub precision: u16,
    pub query_by_distance: bool,
    pub reference_target_field: Option<String>,
    pub reference_to: Vec<String>,
    pub relationship_name: Option<String>,
    pub relationship_order: Option<u16>,
    pub restricted_delete: bool,
    pub restricted_picklist: bool,
    pub scale: u16,
    pub search_prefilterable: bool,
    pub soap_type: SoapType,
    pub sortable: bool,
    #[serde(rename = "type")]
    pub field_type: String,
    pub unique: bool,
    pub updateable: bool,
    pub write_requires_master_read: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildRelationshipDescribe {
    pub cascade_delete: bool,
    #[serde(rename = "childSObject")]
    pub child_sobject: String,
    pub deprecated_and_hidden: bool,
    pub field: String,
    pub junction_id_list_names: Option<Vec<String>>,
    pub junction_reference_to: Option<Vec<String>>,
    pub relationship_name: String,
    pub restricted_delete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordTypeDescribe {
    pub active: bool,
    pub available: bool,
    pub default_record_type_mapping: bool,
    pub developer_name: String,
    pub master: bool,
    pub name: String,
    pub record_type_id: SalesforceId,
    pub urls: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeDescribe {
    pub label: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SObjectDescribe {
    //action_overrides: Vec<ActionOverrideDescribe>,
    pub activateable: bool,
    pub compact_layoutable: bool,
    pub createable: bool,
    pub custom: bool,
    pub custom_setting: bool,
    pub deep_cloneable: bool,
    //defaultImplementation: null // FIXME
    pub deletable: bool,
    //pub deprecated_and_hidden: bool,
    //extendedBy: null
    //extendsInterfaces: null
    pub feed_enabled: bool,
    fields: Vec<FieldDescribe>,
    pub has_subtypes: bool,
    //implementedBy: Option<String>,
    //implementsInterfaces: Option<String>,
    pub is_interface: bool,
    pub is_subtype: bool,
    pub key_prefix: String,
    pub label: String,
    pub label_plural: String,
    pub layoutable: bool,
    pub listviewable: Option<bool>,
    pub lookup_layoutable: Option<bool>,
    pub mergeable: bool,
    pub mru_enabled: bool,
    pub name: String,
    pub named_layout_infos: Vec<HashMap<String, String>>,
    pub network_scope_field_name: Option<String>,
    pub queryable: bool,
    pub record_type_infos: Vec<RecordTypeDescribe>,
    pub replicateable: bool,
    pub retrieveable: bool,
    pub search_layoutable: bool,
    pub searchable: bool,
    pub supported_scopes: Vec<ScopeDescribe>,
    pub triggerable: bool,
    pub undeletable: bool,
    pub updateable: bool,
    pub urls: HashMap<String, String>,
}

impl SObjectDescribe {
    pub fn get_field(&self, api_name: &str) -> Option<&FieldDescribe> {
        // TODO: cache a case-insensitive HashMap for fields.
        let target = api_name.to_lowercase();

        for f in self.fields.iter() {
            if f.name.to_lowercase() == target {
                return Some(f);
            }
        }

        None
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PicklistValueDescribe {
    pub active: bool,
    pub default_value: bool,
    pub label: String,
    pub valid_for: Option<String>, // fixme: probably a new type
    pub value: String,
}
