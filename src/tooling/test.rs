use crate::test_integration_base::get_test_connection;
use anyhow::Result;

use super::{ExecuteAnonymousApexRequest, ExecuteAnonymousApexResponse};

#[tokio::test]
#[ignore]
async fn test_anon_apex_success() -> Result<()> {
    let conn = get_test_connection()?;
    let response = conn
        .execute(&ExecuteAnonymousApexRequest::new(
            "System.debug('Test');".to_owned(),
        ))
        .await?;

    assert_eq!(
        response,
        ExecuteAnonymousApexResponse {
            line: -1,
            column: -1,
            compiled: true,
            success: true,
            compile_problem: None,
            exception_stack_trace: None,
            exception_message: None
        }
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_anon_apex_failure() -> Result<()> {
    let conn = get_test_connection()?;
    let response = conn
        .execute(&ExecuteAnonymousApexRequest::new(
            "System.debug('Test')".to_owned(),
        ))
        .await?;

    assert_eq!(
        response,
        ExecuteAnonymousApexResponse {
            line: 1,
            column: 13,
            compiled: false,
            success: false,
            compile_problem: Some("Unexpected token '('.".to_owned()),
            exception_stack_trace: None,
            exception_message: None
        }
    );

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_anon_apex_exception() -> Result<()> {
    let conn = get_test_connection()?;
    let response = conn
        .execute(&ExecuteAnonymousApexRequest::new(
            "System.debug(Id.valueOf('foo'));".to_owned(),
        ))
        .await?;

    assert_eq!(
        response,
        ExecuteAnonymousApexResponse {
            line: 1,
            column: 1,
            compiled: true,
            success: false,
            compile_problem: None,
            exception_stack_trace: Some("AnonymousBlock: line 1, column 1".to_owned()),
            exception_message: Some("System.StringException: Invalid id: foo".to_owned())
        }
    );

    Ok(())
}
