use anyhow::Result;

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
fn test_times() {}
