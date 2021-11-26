use anyhow::Result;
use futures::future::join_all;
use itertools::Itertools;
use reqwest::Url;
use serde_derive::{Deserialize, Serialize};
use std::env;

use crate::data::{DateTime, SObjectRepresentation};
use crate::rest::rows::SObjectDML;
use crate::SalesforceId;
use crate::{
    auth::AccessTokenAuth, rest::collections::SObjectCollection, rest::query::Queryable,
    Connection, FieldValue, SObject,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Account {
    pub id: Option<SalesforceId>,
    pub name: String,
}

impl SObjectRepresentation for Account {
    fn get_id(&self) -> Option<SalesforceId> {
        self.id
    }

    fn set_id(&mut self, id: Option<SalesforceId>) {
        self.id = id;
    }

    fn get_api_name(&self) -> &str {
        "Account"
    }
}

fn get_test_connection() -> Result<Connection> {
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

#[tokio::test]
async fn test_generic_sobject_rows() -> Result<()> {
    let mut conn = get_test_connection().expect("No connection present");
    let account_type = conn.get_type("Account").await?;

    let before_count = SObject::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Generic Test'",
        false,
    )
    .await?
    .len();

    let mut account = SObject::new(&account_type).with_str("Name", "Generic Test");

    account.create(&conn).await?;

    let mut accounts = SObject::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Generic Test'",
        false,
    )
    .await?;

    assert_eq!(accounts.len(), before_count + 1);
    assert_eq!(accounts[0].get("Name").unwrap(), &FieldValue::String("Generic Test".to_owned()));

    account.put("Name", FieldValue::String("Generic Test 2".to_owned()));
    account.update(&conn).await?;

    let updated_account =
        SObject::retrieve(&conn, &account_type, account.get_id().unwrap().to_owned()).await?;
    assert_eq!(updated_account.get("Name").unwrap(), &FieldValue::String("Generic Test 2".to_owned()));

    accounts[0].delete(&conn).await?;

    Ok(())
}

#[tokio::test]
async fn test_concrete_sobject_rows() -> Result<()> {
    let mut conn = get_test_connection().expect("No connection present");
    let account_type = conn.get_type("Account").await?;

    let before_count = Account::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Concrete Test'",
        false,
    )
    .await?
    .len();

    let mut account = Account {
        id: None,
        name: "Concrete Test".to_owned(),
    };

    account.create(&conn).await?;

    let mut accounts = Account::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Concrete Test'",
        false,
    )
    .await?;

    assert_eq!(accounts.len(), before_count + 1);
    assert_eq!(accounts[0].name, "Concrete Test");

    account.name = "Concrete Test 2".to_owned();
    account.update(&conn).await?;

    let updated_account =
        Account::retrieve(&conn, &account_type, account.get_id().unwrap().to_owned()).await?;
    assert_eq!(updated_account.name, "Concrete Test 2");

    accounts[0].delete(&conn).await?;

    Ok(())
}

#[tokio::test]
async fn test_generic_collections_parallel() -> Result<()> {
    let mut conn = get_test_connection()?;
    let account_type = conn.get_type("Account").await?;

    let mut sobject_chunks: Vec<Vec<SObject>> = (0..1000)
        .map(|i| SObject::new(&account_type).with_string("Name", format!("Account {}", i)))
        .chunks(200)
        .into_iter()
        .map(|v| v.collect::<Vec<SObject>>())
        .collect();

    join_all(
        sobject_chunks
            .iter_mut()
            .map(|v| v.create(conn.clone(), true)),
    )
    .await
    .into_iter()
    .collect::<Result<Vec<Vec<Result<()>>>>>()?
    .into_iter()
    .flatten()
    .collect::<Result<Vec<()>>>()?;
    Ok(())
}
