- Add builder for queries
  X Autorefresh expired tokens
- Implement Index and IndexMut for SObject
  X Implement Serialize and Deserialize for SObject
- Add parallelized get-whole-describe method
- Get sObject describes only at need
- Break requirement to have reference-counted SObjectType objects.
  - Remove from Query methods
- Implement JWT auth
- Resolve connection-as-ref or not in parameters.

  - Parallelization militates for using clone() explicitly in the parameters.
  - Should we have a separate set of \_parallel() DML methods that take `conn` without a ref?
    X Make it possible to use custom structs in place of SObjects, with a trait:
    X Turn the various Auth implementations into an async trait; store it boxed in Connection with blanket impls for DML operations. Require Serialize/Deserialize.

- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
  X Remove data type verification
- Add support for nested sObjects
