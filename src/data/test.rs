use anyhow::Result;
use bytes::{BufMut, BytesMut};
use futures::StreamExt;

use crate::{
    prelude::*,
    test_integration_base::get_test_connection,
};

use super::*;

#[test]
fn test_salesforce_id() {
    assert_eq!(
        "01Q36000000RXX5EAO",
        SalesforceId::new("01Q36000000RXX5").unwrap().to_string()
    );
    assert_eq!(
        "01Q36000000RXX5EAO",
        SalesforceId::new("01Q36000000RXX5EAO").unwrap().to_string()
    );
    assert_eq!(
        "0013600001ohPTpAAM",
        SalesforceId::new("0013600001ohPTp").unwrap().to_string()
    );
}

#[test]
fn test_salesforce_id_errors() {
    assert!(SalesforceId::new("1111111111111111111").is_err());
    assert!(SalesforceId::new("_______________").is_err());
}

#[test]
fn test_datetimes_parse() -> Result<()> {
    assert_eq!(
        "2021-11-19T01:51:47.323+0000".parse::<DateTime>()?,
        DateTime::new(2021, 11, 19, 01, 51, 47, 323)?
    );
    Ok(())
}

#[test]
fn test_datetimes_format() -> Result<()> {
    assert_eq!(
        DateTime::new(2021, 11, 19, 01, 51, 47, 323)?.to_string(),
        "2021-11-19T01:51:47.323+0000"
    );
    Ok(())
}

#[test]
fn test_datetimes_deserialize() -> Result<()> {
    assert_eq!(
        serde_json::from_str::<DateTime>("\"2021-11-19T01:51:47.323+0000\"")?,
        DateTime::new(2021, 11, 19, 01, 51, 47, 323)?
    );
    Ok(())
}

#[test]
fn test_datetimes_serialize() -> Result<()> {
    assert_eq!(
        serde_json::to_string(&DateTime::new(2021, 11, 19, 01, 51, 47, 323)?)?,
        "\"2021-11-19T01:51:47.323+0000\""
    );
    Ok(())
}

#[test]
fn test_dates_parse() -> Result<()> {
    assert_eq!("2021-11-15".parse::<Date>()?, Date::new(2021, 11, 15)?);
    Ok(())
}

#[test]
fn test_dates_format() -> Result<()> {
    assert_eq!(Date::new(2021, 11, 15)?.to_string(), "2021-11-15");
    Ok(())
}

#[test]
fn test_dates_deserialize() -> Result<()> {
    assert_eq!(
        serde_json::from_str::<Date>("\"2021-11-15\"")?,
        Date::new(2021, 11, 15)?
    );
    Ok(())
}

#[test]
fn test_dates_serialize() -> Result<()> {
    assert_eq!(
        serde_json::to_string(&Date::new(2021, 11, 15)?)?,
        "\"2021-11-15\""
    );
    Ok(())
}

#[test]
fn test_times() {
    todo!()
}

#[tokio::test]
#[ignore]
async fn test_blob_retrieve() -> Result<()> {
    // TODO: when blob DML is implemented, rebuild this test to not use Anonymous Apex
    let conn = get_test_connection()?;

    conn.execute_anonymous(
        "insert new ContentVersion(Title = 'Bar', PathOnClient = 'Test.txt', VersionData = Blob.valueOf('Foo'));".to_owned(),
    )
    .await?;

    let mut sobjects = SObject::query_vec(
        &conn,
        &conn.get_type("ContentVersion").await?,
        "SELECT Id, VersionData FROM ContentVersion LIMIT 1",
        false,
    )
    .await?;

    let content = sobjects[0].get("VersionData").unwrap();

    if let FieldValue::Blob(b) = content {
        let mut stream = b.stream(&conn).await?;
        let mut result = BytesMut::with_capacity(32);

        while let Some(chunk) = stream.next().await {
            result.put(&chunk?[..]);
        }

        assert_eq!(b"Foo", &result[..]);
    } else {
        panic!("Wrong type returned")
    }

    sobjects.delete(&conn, false).await?;

    Ok(())
}
