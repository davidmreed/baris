use anyhow::Result;
use tokio_stream::{iter, StreamExt};

use crate::test_integration_base::{get_test_connection, Account};

use super::SObjectStream;

#[tokio::test]
#[ignore]
async fn test_collection_streams() -> Result<()> {
    let conn = get_test_connection()?;

    let mut stream = iter(0..1000)
        .map(|i| Account {
            id: None,
            name: format!("Account {}", i),
        })
        .create_all(&conn, 200, true, Some(5))?;

    let mut count = 0;
    while let Some(r) = stream.next().await {
        match r {
            Ok(_) => count += 1,
            _ => {}
        }
    }

    assert_eq!(1000, count);

    Ok(())
}
