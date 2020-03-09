use std::cell::RefCell;
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
            return Err(SalesforceError::InvalidIdError(id.to_string()))
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
                return Err(SalesforceError::InvalidIdError(id.to_string()))
            }
        }
    
        // Take three slices of the bitstring and use them as 5-bit indices into the alnum sequence.
        full_id[15] = ALNUMS[bitstring & 0x1F] as u8;
        full_id[16] = ALNUMS[bitstring>>5 & 0x1F] as u8;
        full_id[17] = ALNUMS[bitstring>>10] as u8;

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
    Id(SalesforceId)
}

impl FieldValue {
    pub fn is_int(&self) -> bool {
        if let FieldValue::Integer(_) = &self { true } else { false }
    }

    pub fn is_bool(&self) -> bool {
        if let FieldValue::Boolean(_) = &self { true } else { false }
    }

    pub fn is_double(&self) -> bool {
        if let FieldValue::Double(_) = &self { true } else { false }
    }

    pub fn is_string(&self) -> bool {
        if let FieldValue::String(_) = &self { true } else { false }
    }

    pub fn is_date_time(&self) -> bool {
        if let FieldValue::DateTime(_) = &self { true } else { false }
    }

    pub fn is_date(&self) -> bool {
        if let FieldValue::Date(_) = &self { true } else { false }
    }

    pub fn is_time(&self) -> bool {
        if let FieldValue::Time(_) = &self { true } else { false }
    }

    pub fn is_id(&self) -> bool {
        if let FieldValue::Id(_) = &self { true } else { false }
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
            FieldValue::Id(_) => SoapType::Id
        }
    }






}

#[derive(Debug)]
pub struct SObject {
    pub sobjecttype: Rc<SObjectType>,
    pub fields: HashMap<String, FieldValue>
}

impl SObject {
    pub fn new(sobjecttype: &Rc<SObjectType>, fields: HashMap<String, FieldValue>) -> SObject {
        SObject { sobjecttype: Rc::clone(sobjecttype), fields }
    }

    pub fn put(&mut self, key: &str, val: FieldValue) -> Result<(), Box<dyn Error>> {
        // Locate the describe for this field.
        let describe = self.sobjecttype.get_describe().get_field(key);

        if describe.is_none() {
            return Err(Box::new(SalesforceError::SchemaError(format!("Field {} does not exist or is not accessible", key))));
        }

        let describe = describe.unwrap();

        // Validate that the provided value matches the type of this field
        // and satisfies any constraints we can check locally.
        if describe.soap_type != val.get_soap_type() {
            Err(Box::new(SalesforceError::SchemaError(
                format!(
                    "Wrong type of value ({:?}) for field {}.{}",
                    val.get_soap_type(),
                    self.sobjecttype.get_api_name(),
                    key
                )
            )))
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
    describe: SObjectDescribe
}

impl SObjectType {
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
    validFor: String, // fixme: probably a new type
    value: String
}

#[derive(Debug, Deserialize, PartialEq)]
pub enum SoapType {
    #[serde(rename="anytype")]
    Any,
    #[serde(rename="base64binary")]
    Blob,
    Boolean,
    Date,
    DateTime,
    Double,
    Id,
    Integer,
    String,
    Time
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FieldDescribe {
    aggregatable: bool,
    ai_prediction_field: bool,
    auto_number: bool,
    byte_length: u16,
    calculated: bool,
    calculated_formula: Option<String>,
    cascade_delete: bool,
    case_sensitive: bool,
    compound_field_name: Option<String>,
    controller_name: Option<String>,
    createable: bool,
    custom: bool,
    default_value: Option<String>,
    default_value_formula: Option<String>,
    defaulted_on_create: bool,
    dependent_picklist: bool,
    deprecated_and_hidden: bool,
    digits: u16,
    display_location_in_decimal: bool,
    encrypted: bool,
    external_id: bool,
    //extraTypeInfo: null
    filterable: bool,
    //filteredLookupInfo: null
    formula_treat_null_number_as_zero: bool,
    groupable: bool,
    high_scale_number: bool,
    html_formatted: bool,
    id_lookup: bool,
    inline_help_text: Option<String>,
    label: String,
    length: u16,
    //mask: null
    //maskType: null
    name: String,
    name_field: bool,
    name_pointing: bool,
    nillable: bool,
    permissionable: bool,
    picklist_values: Vec<PicklistValueDescribe>,
    polymorphic_foreign_key: bool,
    precision: u16,
    query_by_distance: bool,
    reference_target_field: Option<String>,
    reference_to: Vec<String>,
    relationship_name: Option<String>,
    relationship_order: Option<u16>,
    restricted_delete: bool,
    restricted_picklist: bool,
    scale: u16,
    search_prefilterable: bool,
    soap_type: SoapType,
    sortable: bool,
    #[serde(rename = "type")]
    field_type: String,
    unique: bool,
    updateable: bool,
    write_requires_master_read: bool
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChildRelationshipDescribe {
    cascade_delete: bool,
    #[serde(rename="childSObject")]
    child_sobject: String,
    deprecated_and_hidden: bool,
    field: String,
    junction_id_list_names: Option<Vec<String>>,
    junction_reference_to: Option<Vec<String>>,
    relationship_name: String,
    restricted_delete: bool
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordTypeDescribe {
    active: bool,
    available: bool,
    default_record_type_mapping: bool,
    developer_name: String,
    master: bool,
    name: String,
    record_type_id: SalesforceId,
    //urls: Vec<RecordTypeURLDescribe>
}

#[derive(Debug, Deserialize)]
pub struct ScopeDescribe {
    label: String,
    name: String
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SObjectDescribe<'a> {
    //action_overrides: Vec<ActionOverrideDescribe>,
    pub activateable: bool,
    pub compact_layoutable: bool,
    pub createable: bool,
    pub custom: bool,
    pub custom_setting: bool,
    pub deep_cloneable: bool,
    //defaultImplementation: null // FIXME
    pub deletable: bool,
    pub deprecated_and_hidden: bool,
    //extendedBy: null
    //extendsInterfaces: null
    pub feed_enabled: bool,
    fields: Vec<FieldDescribe>,
    pub has_subtypes: bool,
    //implementedBy: null
    //implementsInterfaces: null
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
    //named_layout_infos: Vec<LayoutDescribe>,
    //networkScopeFieldName: null
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
    #[serde(skip)]
    #[serde(default = "SObjectDescribe::default_refcell_hashmap")]
    field_map: RefCell<HashMap<String, &'a FieldDescribe>>
}


impl SObjectDescribe<'a> {
    fn default_refcell_hashmap() -> RefCell<HashMap<String, &'a FieldDescribe>> {
        RefCell::new(HashMap::new())
    }
    
    fn populate_field_map(&self) {
        let mut field_map = self.field_map.borrow_mut();

        for f in self.fields.iter() {
            field_map.insert(f.name, f);
        }
    }

    pub fn get_field(&self, api_name: &str) -> Option<&FieldDescribe> {
        if self.field_map.borrow().len() == 0 {
            self.populate_field_map();
        }
        
        self.field_map.borrow().get(api_name)
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
