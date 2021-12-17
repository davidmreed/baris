use anyhow::Result;

use crate::data::traits::SObjectWithId;
use crate::rest::query::traits::Queryable;
use crate::rest::rows::traits::SObjectDML;
use crate::test_integration_base::{get_test_connection, Account};
use crate::{FieldValue, SObject};

#[tokio::test]
#[ignore]
async fn test_generic_sobject_rows() -> Result<()> {
    let mut conn = get_test_connection().expect("No connection present");
    let account_type = conn.get_type("Account").await?;

    let before_count = SObject::count_query(
        &conn,
        "SELECT count() FROM Account WHERE Name = 'Generic Test'",
        false,
    )
    .await?;

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
    assert_eq!(
        accounts[0].get("Name").unwrap(),
        &FieldValue::String("Generic Test".to_owned())
    );

    account.put("Name", FieldValue::String("Generic Test 2".to_owned()));
    account.update(&conn).await?;

    let updated_account = SObject::retrieve(
        &conn,
        &account_type,
        account.get_id().unwrap().to_owned(),
        None,
    )
    .await?;
    assert_eq!(
        updated_account.get("Name").unwrap(),
        &FieldValue::String("Generic Test 2".to_owned())
    );

    accounts[0].delete(&conn).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_concrete_sobject_rows() -> Result<()> {
    let mut conn = get_test_connection().expect("No connection present");

    let before_count = Account::count_query(
        &conn,
        "SELECT count() FROM Account WHERE Name = 'Concrete Test'",
        false,
    )
    .await?;

    let mut account = Account {
        id: None,
        name: "Concrete Test".to_owned(),
    };

    account.create(&conn).await?;

    let mut accounts = Account::query_vec(
        &conn,
        "SELECT Id, Name FROM Account WHERE Name = 'Concrete Test'",
        false,
    )
    .await?;

    assert_eq!(accounts.len(), before_count + 1);
    assert_eq!(accounts[0].name, "Concrete Test");

    account.name = "Concrete Test 2".to_owned();
    account.update(&conn).await?;

    let updated_account =
        Account::retrieve(&conn, account.get_id().unwrap().to_owned(), None).await?;
    assert_eq!(updated_account.name, "Concrete Test 2");

    accounts[0].delete(&conn).await?;

    Ok(())
}
