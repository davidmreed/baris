- Add builder for queries
  X Autorefresh expired tokens
- Implement Index and IndexMut for SObject
- Implement Serialize and Deserialize for SObject
- Add parallelized get-whole-describe method
- Get sObject describes only at need
- Break requirement to have reference-counted SObjectType objects.
- Implement JWT auth
- Resolve connection-as-ref or not in parameters.
  - Parallelization militates for using clone() explicitly in the parameters.
- Make it possible to use custom structs in place of SObjects, with a trait:

trait SObject {
fn get_id(&self) -> Option<SalesforceId>;
fn set_id(&self, Option<SalesforceId>);
}

with blanket impls for DML operations. Require Serialize/Deserialize.

- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
  X Remove data type verification
- Add support for nested sObjects
