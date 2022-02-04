use anyhow::Result;
use tokio_stream::{iter, StreamExt};

use crate::test_integration_base::{get_test_connection, Account};

use super::SObjectStream;

#[tokio::test]
#[ignore]
async fn test_collection_stream_create() -> Result<()> {
    let conn = get_test_connection()?;

    let mut stream = iter(0..1000)
        .map(|i| Account {
            id: None,
            name: format!("Account {}", i),
        })
        .create_all(&conn, 200, true, Some(5))?;

    let mut count = 0;
    while let Some(r) = stream.next().await {
        if r.is_ok() {
            count += 1
        }
    }

    assert_eq!(1000, count);

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_collection_stream_update() -> Result<()> {
    let conn = get_test_connection()?;

    let mut stream = iter(0..100)
        .map(|i| Account {
            id: None,
            name: format!("Account {}", i),
        })
        .create_all(&conn, 20, true, Some(5))?
        .map(|r| Account {
            id: Some(r.unwrap()),
            name: "Updated".to_owned(),
        })
        .update_all(&conn, 20, true, Some(5))?;

    while let Some(r) = stream.next().await {
        r?;
    }

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_collection_stream_create_delete() -> Result<()> {
    let conn = get_test_connection()?;

    let mut stream = iter(0..100)
        .map(|i| Account {
            id: None,
            name: format!("Account {}", i),
        })
        .create_all(&conn, 20, true, Some(5))?
        .map(|r| Account {
            id: Some(r.unwrap()),
            name: "".to_owned(),
        })
        .delete_all(&conn, 20, true, Some(5))?;

    while let Some(r) = stream.next().await {
        assert!(r.is_ok());
    }

    Ok(())
}
