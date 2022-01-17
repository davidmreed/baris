use anyhow::Result;
use reqwest::Url;
use serde_derive::{Deserialize, Serialize};
use std::env;

use crate::data::{SObjectWithId, SingleTypedSObject, SObjectBase};
use crate::{auth::AccessTokenAuth, api::Connection};
use crate::data::{FieldValue, SalesforceId};

pub fn get_test_connection() -> Result<Connection> {
    let access_token = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;

    Connection::new(
        Box::new(AccessTokenAuth::new(
            access_token,
            Url::parse(&instance_url)?,
        )),
        "v52.0",
    )
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Account {
    pub id: Option<SalesforceId>,
    pub name: String,
}

impl SObjectBase for Account {}

impl SObjectWithId for Account {
    fn get_id(&self) -> FieldValue {
        if let Some(id) = self.id {
            FieldValue::Id(id)
        } else {
            FieldValue::Null
        }
    }

    fn set_id(&mut self, id: FieldValue) {
        match id {
            FieldValue::Id(id) => {
                self.id = Some(id);
            }
            FieldValue::Null => self.id = None,
            _ => {
                panic!("Invalid id: {:?}", id);
            }
        }
    }
}

impl SingleTypedSObject for Account {
    fn get_type_api_name() -> &'static str {
        "Account"
    }
}
