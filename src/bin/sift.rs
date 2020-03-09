use std::env;
use std::error::Error;

use oxideforce::{Connection, FieldValue, SObject};

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

            sobj.put("Name", FieldValue::String("Test".to_string()))?;
            conn.create(&mut sobj)?;

            println!("Created {} {}", &args[1], sobj.get_id().unwrap());
        }
        _ => {
            println!("Unknown operation");
        }
    }

    Ok(())
}
