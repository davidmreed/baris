use anyhow::Result;
use futures::future::join_all;
use itertools::Itertools;

use crate::test_integration_base::get_test_connection;
use crate::{rest::collections::traits::SObjectCollection, SObject};

#[tokio::test]
#[ignore]
async fn test_generic_collections_parallel() -> Result<()> {
    let conn = get_test_connection()?;
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
