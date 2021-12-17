MVP

- Get sObject describes only at need
- Implement JWT auth
- Resolve connection-as-ref or not in parameters.

  - Parallelization militates for using clone() explicitly in the parameters.
  - Should we have a separate set of \_parallel() DML methods that take `conn` without a ref?

- We should implement sObject Collections DML on collections other than Vecs, as well as iterators and streams.
- Add support for nested sObjects
- Break requirement to have reference-counted SObjectType objects.
  - Remove from Query methods
  - Add SingleTypedSobject trait

Next

- Add builder for queries
- Implement Index and IndexMut for SObject
- Add parallelized get-whole-describe method
- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
