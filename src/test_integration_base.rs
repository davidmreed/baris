use anyhow::Result;
use reqwest::Url;
use serde_derive::{Deserialize, Serialize};
use std::env;

use crate::data::{SObjectWithId, SingleTypedSObject, TypedSObject};
use crate::SalesforceId;
use crate::{auth::AccessTokenAuth, Connection};

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

impl SObjectWithId for Account {
    fn get_id(&self) -> Option<SalesforceId> {
        self.id
    }

    fn set_id(&mut self, id: Option<SalesforceId>) {
        self.id = id;
    }
}

impl SingleTypedSObject for Account {
    fn get_type_api_name() -> &'static str {
        "Account"
    }
}
