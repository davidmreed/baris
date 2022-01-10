use crate::{
    bulk::v2::traits::{BulkQueryable, SingleTypeBulkQueryable, SingleTypeBulkUpdateable},
    data::SObjectWithId,
    rest::rows::traits::{SObjectDML, SObjectSingleTypedRetrieval},
    test_integration_base::{get_test_connection, Account},
    SObject,
};
use anyhow::Result;
use tokio_stream::StreamExt;

#[tokio::test]
#[ignore]
async fn test_bulk_query_single_type() -> Result<()> {
    let conn = get_test_connection().expect("No connection present");

    let mut account = Account {
        id: None,
        name: "Bulk Query Test".to_owned(),
    };

    account.create(&conn).await?;

    let mut stream = Account::bulk_query(&conn, "SELECT Id, Name FROM Account", false).await?;

    while let Some(act) = stream.next().await {
        let act = act?;
        println!(
            "I found an Account with Id {} and Name {}",
            act.id.unwrap(),
            act.name
        );
        // TODO: add assertions.
    }

    account.delete(&conn).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_bulk_query_dynamic_type() -> Result<()> {
    let conn = get_test_connection().expect("No connection present");
    let sobject_type = conn.get_type("Account").await?;

    let mut account = SObject::new(&sobject_type).with_str("Name", "Dynamic Bulk Query Test");

    account.create(&conn).await?;

    let mut stream =
        SObject::bulk_query(&conn, &sobject_type, "SELECT Id, Name FROM Account", false).await?;

    while let Some(act) = stream.next().await {
        let act = act?;
        println!(
            "I found an Account with Id {} and Name {}",
            act.get_opt_id().unwrap(),
            act.get("Name").unwrap().as_string()
        );
        // TODO: add assertions.
    }

    account.delete(&conn).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_bulk_query_to_update() -> Result<()> {
    let conn = get_test_connection().expect("No connection present");

    let mut account = Account {
        id: None,
        name: "Bulk Query-Update Test".to_owned(),
    };

    account.create(&conn).await?;

    Account::bulk_query(&conn, "SELECT Id, Name FROM Account LIMIT 1", false)
        .await?
        .map(|r| {
            let mut r = r.unwrap();
            r.name = format!("{} Updated", r.name);
            r
        })
        .bulk_update(&conn)
        .await?;

    let account = Account::retrieve(
        &conn,
        account.id.unwrap(),
        Some(vec!["Id".to_owned(), "Name".to_owned()]),
    )
    .await?;

    assert_eq!(account.name, "Bulk Query-Update Test");

    Ok(())
}
