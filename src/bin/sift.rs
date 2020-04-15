use std::env;
use std::error::Error;

use oxideforce::{Connection, FieldValue, SObject, SalesforceId};

fn main() -> Result<(), Box<dyn Error>> {
    let sid = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;
    let args: Vec<String> = env::args().collect();
    let conn = Connection::new(&sid, &instance_url, "v47.0")?;
    let sobjecttype = conn.get_type(&args[1]).unwrap();

    match args[2].as_str() {
        "query" => {
            for sobj in conn.query(&sobjecttype, &args[3])? {
                println!("I received sObject {:?}", sobj?.fields);
            }
        }
        "create" => {
            let mut sobj = SObject::new(&sobjecttype);

            sobj.put("Name", FieldValue::String(args[3].clone()))?;
            conn.create(&mut sobj)?;

            println!("Created {} {}", &args[1], sobj.get_id().unwrap());
        }
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
        }
        _ => {
            println!("Unknown operation");
        }
    }

    Ok(())
}
