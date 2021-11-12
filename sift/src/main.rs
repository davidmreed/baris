use std::env;

use anyhow::Result;
use oxideforce::auth::{AccessTokenAuth, AuthDetails, ConnectedApp, UsernamePasswordAuth};
use oxideforce::bulk::v2::{BulkQueryJob, BulkQueryOperation};
use oxideforce::rest::query::QueryRequest;
use oxideforce::rest::{
    SObjectCreateRequest, SObjectDeleteRequest, SObjectUpdateRequest, SObjectUpsertRequest,
};
use oxideforce::{Connection, FieldValue, SObject, SalesforceId};

/*
async fn test_username_password() -> Result<()> {
    let username = env::var("SALESFORCE_USER")?;
    let password = env::var("SALESFORCE_PASSWORD")?;
    let security_token = env::var("SECURITY_TOKEN")?;
    let instance_url = env::var("INSTANCE_URL")?;
    let consumer_key = env::var("CONSUMER_KEY")?;
    let client_secret = env::var("CLIENT_SECRET")?;

    let mut conn = Connection::new(
        AuthDetails::UsernamePassword(UsernamePasswordAuth::new(
            username,
            password,
            Some(security_token),
            ConnectedApp::new(consumer_key, client_secret, None),
            instance_url,
        )),
        "v52.0",
    )?;

    Ok(())
}*/

#[tokio::main]
async fn main() -> Result<()> {
    let access_token = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;

    let mut conn = Connection::new(
        AuthDetails::AccessToken(AccessTokenAuth::new(access_token, instance_url)),
        "v52.0",
    )?;

    // Basic sObject manipulation.
    let account_type = conn.get_type("Account").await?;
    let mut account = SObject::new(&account_type);
    account.put("Name", FieldValue::String("Test".to_owned()))?;

    account.create(&conn).await?;

    let mut accounts = SObject::query_vec(
        &conn,
        &account_type,
        "SELECT Name FROM Account WHERE Name = 'Name'",
        false,
    )
    .await?;

    assert!(accounts.len() == 1);
    assert!(accounts[0].get("Name").unwrap() == &FieldValue::String("Name".to_owned()));
    accounts[0].delete(&conn).await?;
    Ok(())
}
