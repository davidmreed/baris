use std::env;
use std::error::Error;

use oxideforce::Connection;

fn main() -> Result<(), Box<dyn Error>> {
    let sid = env::var("SESSION_ID")?;
    let instance_url = env::var("INSTANCE_URL")?;
    let args: Vec<String> = env::args().collect();
    let conn = Connection::new(&sid, &instance_url, "v47.0")?;
    let sobjecttype = conn.get_type(&args[1]).unwrap();

    for sobj in conn.query(&sobjecttype, &args[2])? {
        println!("I received sObject {:?}", sobj?);
    }
    Ok(())
}