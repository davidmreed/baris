use anyhow::Result;
use std::env;

use crate::{
    auth::{AccessTokenAuth, AuthDetails},
    Connection, FieldValue, SObject,
};

fn get_test_connection() -> Result<Connection> {
    let access_token = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;

    Connection::new(
        AuthDetails::AccessToken(AccessTokenAuth::new(access_token, instance_url)),
        "v52.0",
    )
}

#[tokio::test]
async fn test_individual_sobjects() -> Result<()> {
    let mut conn = get_test_connection()?;
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

    account.put("Name", FieldValue::String("Test 2".to_owned()))?;
    account.update(&conn).await?;

    let updated_account =
        SObject::retrieve(&conn, &account_type, account.get_id().unwrap().to_owned()).await?;
    assert!(updated_account.get("Name").unwrap() == &FieldValue::String("Test 2".to_owned()));

    accounts[0].delete(&conn).await?;

    Ok(())
}
