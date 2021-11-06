use std::env;

use anyhow::Result;
use oxideforce::auth::{AuthDetails, ConnectedApp, UsernamePasswordAuth};
use oxideforce::bulk::v2::{BulkQueryJob, BulkQueryOperation};
use oxideforce::rest::query::QueryRequest;
use oxideforce::rest::{
    SObjectCreateRequest, SObjectDeleteRequest, SObjectUpdateRequest, SObjectUpsertRequest,
};
use oxideforce::{Connection, FieldValue, SObject, SalesforceId};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let username = env::var("USERNAME")?;
    let password = env::var("PASSWORD")?;
    let security_token = env::var("SECURITY_TOKEN")?;
    let instance_url = env::var("INSTANCE_URL")?;
    let customer_key = env::var("CUSTOMER_KEY")?;
    let client_secret = env::var("CLIENT_SECRET")?;
    let args: Vec<String> = env::args().collect();
    let conn = Connection::new(
        AuthDetails::UsernameToken(UsernamePasswordAuth::new(
            username,
            password,
            security_token,
            ConnectedApp::new(customer_key, client_secret, None),
            instance_url,
        )),
        "v52.0",
    )?;
    let sobject_type = conn.get_type(&args[1]).await?;

    match args[2].as_str() {
        "query" => {
            let request = QueryRequest::new(&sobject_type, &"SELECT Id FROM Account", false);
            let mut stream = conn.execute(&request).await?;

            while let Some(sobj) = stream.next().await {
                println!("I received sObject {:?}", sobj?.fields);
            }
        }
        "bulk_query" => {
            let request =
                BulkQueryJob::new(&conn, &"SELECT Id FROM Account", BulkQueryOperation::Query)
                    .await?;

            request.complete(&conn).await?;
            let mut stream = request.get_results_stream(&conn, &sobject_type).await;

            while let Some(sobj) = stream.next().await {
                println!("I received sObject {:?}", sobj?.fields);
            }
        }
        "create" => {
            let mut sobj = SObject::new(&sobject_type);
            sobj.put("Name", FieldValue::String(args[3].clone()))?;

            let result = conn.execute(&SObjectCreateRequest::new(sobj)?).await?;

            println!("Created {} {:?}", &args[1], result);
        }
        "update" => {
            let mut sobj = SObject::new(&sobject_type);

            sobj.put("Id", FieldValue::Id(SalesforceId::new(&args[3])?))?;
            sobj.put("Name", FieldValue::String(args[4].clone()))?;
            let result = conn.execute(&SObjectUpdateRequest::new(sobj)?).await?;

            println!("Created {} {:?}", &args[1], result);
        }
        "upsert" => {
            let mut sobj = SObject::new(&sobject_type);

            sobj.put(&args[3], FieldValue::String(args[4].clone()))?;
            sobj.put("Name", FieldValue::String(args[5].clone()))?;
            let result = conn
                .execute(&SObjectUpsertRequest::new(sobj, &args[3])?)
                .await?;

            println!("Upserted {} {:?}", &args[1], result);
        }
        "delete" => {
            let mut sobj = SObject::new(&sobject_type);

            sobj.put("Id", FieldValue::Id(SalesforceId::new(&args[3])?))?;
            conn.execute(&SObjectDeleteRequest::new(sobj)?).await?;

            println!("Deleted {} {}", &args[1], &args[3]);
        }
        _ => {
            println!("Unknown operation");
        }
    }

    Ok(())
}
