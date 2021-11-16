- Add builder for queries
  X Autorefresh expired tokens
- Add DML operations as stream and iterator adapters:

SObject::query(&conn, some_type, some_query)
.map(|s| s.put("Industry", &FieldValue::String("Foo")))
.chunks(200) // from futures::stream::StreamExt
.update(&conn) // or .update_parallel(&conn) to spawn()?
.collect::<Result<()>>()
.await?;

- Add parallelized get-whole-describe method
- Get sObject describes only at need
- Implement JWT auth
- Make it possible to use custom structs in place of SObjects, with a trait:

trait SObject {
fn get_id(&self) -> Option<SalesforceId>;
fn set_id(&self, Option<SalesforceId>);
}

with blanket impls for DML operations. Require Serialize/Deserialize.

- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
  X Remove data type verification
- Add support for nested sObjects
