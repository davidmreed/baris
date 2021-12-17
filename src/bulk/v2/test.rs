use anyhow::Result;
use tokio_stream::StreamExt; // FIXME: why is this import required?

use crate::{
    bulk::v2::BulkQueryable,
    rest::rows::SObjectDML,
    test_integration_base::{get_test_connection, Account},
};

#[tokio::test]
#[ignore]
async fn test_bulk_query() -> Result<()> {
    let mut conn = get_test_connection().expect("No connection present");
    let account_type = conn.get_type("Account").await?;

    let mut account = Account {
        id: None,
        name: "Bulk Query Test".to_owned(),
    };

    account.create(&conn).await?;

    let mut stream =
        Account::bulk_query(&conn, &account_type, "SELECT Id, Name FROM Account", false).await?;

    while let Some(act) = stream.next().await {
        let act = act?;
        println!(
            "I found an Account with Id {} and Name {}",
            act.id.unwrap(),
            act.name
        );
    }

    account.delete(&conn).await?;

    Ok(())
}
