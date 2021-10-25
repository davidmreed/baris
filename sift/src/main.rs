use std::env;

use anyhow::Result;
use oxideforce::rest::query::QueryRequest;
use oxideforce::rest::SObjectCreateRequest;
use oxideforce::{Connection, FieldValue, SObject};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let sid = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;
    let args: Vec<String> = env::args().collect();
    let conn = Connection::new(&sid, &instance_url, "v52.0")?;
    let sobject_type = conn.get_type(&args[1]).await?;

    match args[2].as_str() {
        "query" => {
            let request = QueryRequest::new(&sobject_type, &"SELECT Id FROM Account", false);
            let mut stream = conn.execute(&request).await?;

            while let Some(sobj) = stream.next().await {
                println!("I received sObject {:?}", sobj?.fields);
            }
        }
        "create" => {
            let mut sobj = SObject::new(&sobject_type);

            sobj.put("Name", FieldValue::String(args[3].clone()))?;
            let request = SObjectCreateRequest::new(sobj)?;

            let result = conn.execute(&request).await?;

            println!("Created {} {:?}", &args[1], result);
        } /*
        "update" => {
        let mut sobj = SObject::new(&sobjecttype);

        sobj.put("Id", FieldValue::Id(SalesforceId::new(&args[3])?))?;
        sobj.put("Name", FieldValue::String(args[4].clone()))?;
        conn.update(&mut sobj)?;

        println!("Created {} {}", &args[1], sobj.get_id().unwrap());
        }
        "upsert" => {
        let mut sobj = SObject::new(&sobjecttype);

        sobj.put(&args[3], FieldValue::String(args[4].clone()))?;
        sobj.put("Name", FieldValue::String(args[5].clone()))?;
        conn.upsert(&mut sobj, &args[3])?;

        println!("Upserted {} {}", &args[1], sobj.get_id().unwrap());
        }
        "delete" => {
        let mut sobj = SObject::new(&sobjecttype);

        sobj.put("Id", FieldValue::Id(SalesforceId::new(&args[3])?))?;
        conn.delete(sobj)?;

        println!("Deleted {} {}", &args[1], &args[3]);
        }*/
        _ => {
            println!("Unknown operation");
        }
    }

    Ok(())
}
