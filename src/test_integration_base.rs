use anyhow::Result;
use reqwest::Url;
use serde_derive::{Deserialize, Serialize};
use std::env;

use crate::prelude::*;
use crate::{api::Connection, auth::AccessTokenAuth};

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
        match self.get_opt_id() {
            Some(id) => FieldValue::Id(id),
            None => FieldValue::Null,
        }
    }

    fn set_id(&mut self, id: FieldValue) -> Result<()> {
        match id {
            FieldValue::Id(id) => {
                self.set_opt_id(Some(id))?;
                Ok(())
            }
            FieldValue::Null => {
                self.set_opt_id(None)?;
                Ok(())
            }
            _ => Err(SalesforceError::UnsupportedId.into()),
        }
    }

    fn get_opt_id(&self) -> Option<crate::data::types::SalesforceId> {
        self.id
    }

    fn set_opt_id(&mut self, id: Option<crate::data::types::SalesforceId>) -> Result<()> {
        self.id = id;
        Ok(())
    }
}

impl SingleTypedSObject for Account {
    fn get_type_api_name() -> &'static str {
        "Account"
    }
}
