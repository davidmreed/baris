use anyhow::Result;

use super::CompositeRequest;
use crate::prelude::*;
use crate::rest::collections::SObjectCollectionCreateRequest;
use crate::rest::rows::{SObjectCreateRequest, SObjectDeleteRequest, SObjectUpdateRequest};
use crate::test_integration_base::get_test_connection;

#[tokio::test]
#[ignore]
async fn test_composite_request_create_with_reference() -> Result<()> {
    let conn = get_test_connection()?;
    let mut request = CompositeRequest::new(conn.get_base_url_path(), Some(true), Some(false));
    let account_type = &conn.get_type("Account").await?;
    let contact_type = &conn.get_type("Contact").await?;
    let account = SObject::new(&account_type).with_str("Name", "Test");
    let contact = SObject::new(&contact_type)
        .with_str("LastName", "Foo")
        .with_composite_reference("AccountId", "@{acct.id}");
    let mut account_request = SObjectCreateRequest::new(&account)?;
    let mut contact_request = SObjectCreateRequest::new(&contact)?;

    request.add("acct", &mut account_request)?;
    request.add("ct", &mut contact_request)?;

    let result = conn.execute(&request).await?;

    let account_result = result.get_result(&conn, "acct", &account_request)?;
    let contact_result = result.get_result(&conn, "ct", &contact_request)?;

    assert!(account_result.success);
    assert!(contact_result.success);

    let mut account =
        SObject::retrieve(&conn, &account_type, account_result.id.unwrap(), None).await?;
    let mut contact =
        SObject::retrieve(&conn, &contact_type, contact_result.id.unwrap(), None).await?;

    assert_eq!(
        contact.get("AccountId").unwrap(),
        account.get("Id").unwrap()
    );

    contact.delete(&conn).await?;
    account.delete(&conn).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_composite_request_create_update_delete() -> Result<()> {
    let conn = get_test_connection()?;
    let mut request = CompositeRequest::new(conn.get_base_url_path(), Some(true), Some(false));
    let account_type = &conn.get_type("Account").await?;
    let account = SObject::new(&account_type).with_str("Name", "Test");
    let mut account_request = SObjectCreateRequest::new(&account)?;
    let updated_account = SObject::new(&account_type)
        .with_composite_reference("Id", "@{create.id}")
        .with_str("Name", "Foo");
    let delete_account = SObject::new(&account_type).with_composite_reference("Id", "@{create.id}");
    let mut update_account_request = SObjectUpdateRequest::new(&updated_account)?;
    let mut delete_account_request = SObjectDeleteRequest::new(&delete_account)?;

    request.add("create", &mut account_request)?;
    request.add("update", &mut update_account_request)?;
    request.add("delete", &mut delete_account_request)?;

    let result = conn.execute(&request).await?;
    let _account_result = result.get_result(&conn, "delete", &delete_account_request)?;

    //assert!(account_result.success); TODO

    /* Future state:
        let result = composite!({
            "create" => account.create_request(),
            "update" => account.with_str("Name", "foo").update_request(),
            "delete" => account.delete_request()
        }).execute(&conn).await?;

        assert_eq!(result.http_status, 200);
        assert_eq!(result.create.http_status, 200);
        assert!(result.create.body.id != null);
    */

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_composite_request_collections() -> Result<()> {
    let conn = get_test_connection()?;
    let mut request = CompositeRequest::new(conn.get_base_url_path(), Some(true), Some(false));
    let account_type = &conn.get_type("Account").await?;
    let account = SObject::new(&account_type).with_str("Name", "Test");
    let mut account_request = SObjectCollectionCreateRequest::new(&vec![account], true)?;
    let delete_account =
        SObject::new(&account_type).with_composite_reference("Id", "@{create[0].id}");
    let mut delete_account_request = SObjectDeleteRequest::new(&delete_account)?;

    request.add("create", &mut account_request)?;
    request.add("delete", &mut delete_account_request)?;

    let result = conn.execute(&request).await?;
    let _account_result = result.get_result(&conn, "delete", &delete_account_request)?;

    Ok(())
}
