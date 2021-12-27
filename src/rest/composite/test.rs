use anyhow::Result;

use crate::rest::rows::traits::{SObjectDML, SObjectDynamicallyTypedRetrieval};
use crate::rest::rows::SObjectCreateRequest;
use crate::test_integration_base::get_test_connection;
use crate::SObject;

use super::CompositeRequest;

#[tokio::test]
#[ignore]
async fn test_composite_request() -> Result<()> {
    let conn = get_test_connection()?;
    let mut request = CompositeRequest::new(conn.get_base_url_path(), Some(true), Some(false));
    let account_type = &conn.get_type("Account").await?;
    let contact_type = &conn.get_type("Contact").await?;
    let mut account = SObject::new(&account_type).with_str("Name", "Test");
    let mut contact = SObject::new(&contact_type)
        .with_str("LastName", "Foo")
        .with_composite_reference("AccountId", "@{acct.id}");
    let mut account_request = SObjectCreateRequest::new(&mut account)?;
    let mut contact_request = SObjectCreateRequest::new(&mut contact)?;

    request.add("acct", &mut account_request);
    request.add("ct", &mut contact_request);

    let result = conn.execute(&request).await?;

    let account_result = result.get_result(&conn, "acct", &account_request)?;
    let contact_result = result.get_result(&conn, "ct", &contact_request)?;

    assert!(account_result.success);
    assert!(contact_result.success);

    let account = SObject::retrieve(&conn, &account_type, account_result.id.unwrap(), None).await?;
    let contact = SObject::retrieve(&conn, &contact_type, contact_result.id.unwrap(), None).await?;

    assert_eq!(
        contact.get("AccountId").unwrap(),
        account.get("Id").unwrap()
    );

    contact.delete(&conn).await?;
    account.delete(&conn).await?;

    Ok(())
}
