use anyhow::Result;
use std::env;

use crate::{
    auth::{AccessTokenAuth, AuthDetails},
    Connection, FieldValue, SObject,
};

#[tokio::test]
async fn test_individual_sobjects() -> Result<()> {
    let access_token = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;

    let mut conn = Connection::new(
        AuthDetails::AccessToken(AccessTokenAuth::new(access_token, instance_url)),
        "v52.0",
    )?;

    // Basic sObject manipulation.
    let account_type = conn.get_type("Account").await?;

    let before_count = SObject::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Test'",
        false,
    )
    .await?
    .len();

    let mut account = SObject::new(&account_type);
    account.put("Name", FieldValue::String("Test".to_owned()))?;

    account.create(&conn).await?;

    let mut accounts = SObject::query_vec(
        &conn,
        &account_type,
        "SELECT Id, Name FROM Account WHERE Name = 'Test'",
        false,
    )
    .await?;

    assert!(accounts.len() == before_count + 1);
    assert!(accounts[0].get("Name").unwrap() == &FieldValue::String("Test".to_owned()));
    accounts[0].delete(&conn).await?;

    Ok(())
}
