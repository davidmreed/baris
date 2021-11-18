- Add builder for queries
  X Autorefresh expired tokens
- Implement Index and IndexMut for SObject
- Implement Serialize and Deserialize for SObject
- Add parallelized get-whole-describe method
- Get sObject describes only at need
- Break requirement to have reference-counted SObjectType objects.
  - Remove from Query methods
- Implement JWT auth
- Resolve connection-as-ref or not in parameters.
  - Parallelization militates for using clone() explicitly in the parameters.
  - Should we have a separate set of \_parallel() DML methods that take `conn` without a ref?
- Make it possible to use custom structs in place of SObjects, with a trait:
  X Turn the various Auth implementations into an async trait; store it boxed in Connection

trait SObject: SObjectCreation {
fn get_id(&self) -> Option<SalesforceId>;
fn set_id(&self, Option<SalesforceId>);
}

#[async_trait]
trait SObjectCreation {
async fn from_json(Value) -> Result<Self>;
fn from_csv(Value, SObjectType) -> Result<Self>;
}

impl SObjectCreation for T
where T: Deserialize {
fn from*json(value: Value, *: Connection) -> Result<Self> {
// If we're a strongly-typed struct that can be deserialized, we do not need the SObjectType
return serde_json::from_value::<Self>(value);
}
}

WE DON'T NEED FROM_CSV! Just have the bulk classes handle it - they can cache the describe, and then send
a Value to from_json. We can then provide adapters for dealing with actual CSV data.

with blanket impls for DML operations. Require Serialize/Deserialize.

- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
  X Remove data type verification
- Add support for nested sObjects
