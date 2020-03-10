use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use serde_derive::Deserialize;

use super::errors::SalesforceError;

#[serde(try_from = "&str")]
#[derive(Debug, Deserialize)]
pub struct SalesforceId {
    id: [u8; 18],
}

impl SalesforceId {
    pub fn new(id: &str) -> Result<SalesforceId, SalesforceError> {
        const ALNUMS: &[u8] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345".as_bytes();

        if id.len() != 15 && id.len() != 18 {
            return Err(SalesforceError::InvalidIdError(id.to_string()));
        }

        let mut full_id: [u8; 18] = [0; 18];
        let mut bitstring: usize = 0;

        for (i, c) in id[..15].chars().enumerate() {
            if c.is_ascii_alphanumeric() {
                if c.is_ascii_uppercase() {
                    bitstring |= 1 << i
                }
                full_id[i] = c as u8;
            } else {
                return Err(SalesforceError::InvalidIdError(id.to_string()));
            }
        }
        // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
        full_id[15] = ALNUMS[bitstring & 0x1F] as u8;
        full_id[16] = ALNUMS[bitstring >> 5 & 0x1F] as u8;
        full_id[17] = ALNUMS[bitstring >> 10] as u8;

        Ok(SalesforceId { id: full_id })
    }
}

impl TryFrom<&str> for SalesforceId {
    type Error = SalesforceError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        SalesforceId::new(value)
    }
}

impl fmt::Display for SalesforceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", std::str::from_utf8(&self.id).unwrap())
    }
}

#[derive(Debug)]
pub enum FieldValue {
    Integer(i64),
    Double(f64),
    Boolean(bool),
    String(String),
    DateTime(String),
    Time(String),
    Date(String),
    Id(SalesforceId),
}

impl FieldValue {
    pub fn is_int(&self) -> bool {
        if let FieldValue::Integer(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_bool(&self) -> bool {
        if let FieldValue::Boolean(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_double(&self) -> bool {
        if let FieldValue::Double(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_string(&self) -> bool {
        if let FieldValue::String(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_date_time(&self) -> bool {
        if let FieldValue::DateTime(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_date(&self) -> bool {
        if let FieldValue::Date(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_time(&self) -> bool {
        if let FieldValue::Time(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn is_id(&self) -> bool {
        if let FieldValue::Id(_) = &self {
            true
        } else {
            false
        }
    }

    pub fn get_soap_type(&self) -> SoapType {
        match &self {
            FieldValue::Integer(_) => SoapType::Integer,
            FieldValue::Double(_) => SoapType::Double,
            FieldValue::Boolean(_) => SoapType::Boolean,
            FieldValue::String(_) => SoapType::String,
            FieldValue::DateTime(_) => SoapType::DateTime,
            FieldValue::Time(_) => SoapType::Time,
            FieldValue::Date(_) => SoapType::Date,
            FieldValue::Id(_) => SoapType::Id,
        }
    }
}

#[derive(Debug)]
pub struct SObject {
    pub sobjecttype: Rc<SObjectType>,
    pub fields: HashMap<String, FieldValue>,
}

impl SObject {
    pub fn new(sobjecttype: &Rc<SObjectType>) -> SObject {
        SObject {
            sobjecttype: Rc::clone(sobjecttype),
            fields: HashMap::new(),
        }
    }

    pub fn put(&mut self, key: &str, val: FieldValue) -> Result<(), Box<dyn Error>> {
        // Locate the describe for this field.
        let describe = self.sobjecttype.get_describe().get_field(key);

        if describe.is_none() {
            return Err(Box::new(SalesforceError::SchemaError(format!(
                "Field {} does not exist or is not accessible",
                key
            ))));
        }

        let describe = describe.unwrap();

        // Validate that the provided value matches the type of this field
        // and satisfies any constraints we can check locally.
        if describe.soap_type != val.get_soap_type() {
            Err(Box::new(SalesforceError::SchemaError(format!(
                "Wrong type of value ({:?}) for field {}.{} (type {:?})",
                val.get_soap_type(),
                self.sobjecttype.get_api_name(),
                key,
                describe.soap_type
            ))))
        } else {
            self.fields.insert(key.to_lowercase(), val);
            Ok(())
        }
    }

    pub fn get_id(&self) -> Option<&SalesforceId> {
        if let Some(FieldValue::Id(id)) = self.get("id") {
            Some(id)
        } else {
            None
        }
    }

    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.fields.get(&key.to_lowercase())
    }

    pub fn get_binary_blob(&self, key: &str) {
        unimplemented!();
    }
}

#[derive(Debug)]
pub struct SObjectType {
    api_name: String,
    describe: SObjectDescribe,
}

impl SObjectType {
    pub fn new(api_name: String, describe: SObjectDescribe) -> SObjectType {
        SObjectType { api_name, describe }
    }

    pub fn get_describe(&self) -> &SObjectDescribe {
        &self.describe
    }

    pub fn get_api_name(&self) -> &str {
        &self.api_name
    }
}

impl fmt::Display for SObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.api_name)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PicklistValueDescribe {
    active: bool,
    default_value: bool,
    label: String,
    valid_for: Option<String>, // fixme: probably a new type
    value: String,
}

#[derive(Debug, Deserialize, PartialEq, Copy, Clone)]
pub enum SoapType {
    #[serde(rename = "urn:address")]
    Address,
    #[serde(rename = "xsd:anyType")]
    Any,
    #[serde(rename = "xsd:base64binary")]
    Blob,
    #[serde(rename = "xsd:boolean")]
    Boolean,
    #[serde(rename = "xsd:date")]
    Date,
    #[serde(rename = "xsd:dateTime")]
    DateTime,
    #[serde(rename = "xsd:double")]
    Double,
    #[serde(rename = "tns:ID")]
    Id,
    #[serde(rename = "xsd:int")]
    Integer,
    #[serde(rename = "xsd:string")]
    String,
    #[serde(rename = "xsd:time")]
    Time,
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
    pub default_value: Option<bool>,
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
        let target = api_name.to_lowercase();

        for f in self.fields.iter() {
            if f.name.to_lowercase() == target {
                return Some(f);
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_salesforce_id() {
        assert_eq!(
            "01Q36000000RXX5EAO",
            SalesforceId::new("01Q36000000RXX5").unwrap().to_string()
        );
        assert_eq!(
            "0013600001ohPTpAAM",
            SalesforceId::new("0013600001ohPTp").unwrap().to_string()
        );
    }

    #[test]
    fn test_salesforce_id_errors() {
        assert!(SalesforceId::new("1111111111111111111").is_err());
        assert!(SalesforceId::new("_______________").is_err());
    }
}