use crate::{
    bulk::v2::SingleTypeBulkQueryable,
    rest::rows::traits::SObjectDML,
    test_integration_base::{get_test_connection, Account},
};
use anyhow::Result;
use tokio_stream::StreamExt;

#[tokio::test]
#[ignore]
async fn test_bulk_query() -> Result<()> {
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
    }

    account.delete(&conn).await?;

    Ok(())
}
