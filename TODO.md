Cactusforce

- Open source
- Add support for Bulk DML
- [Ideal] We should implement sObject Collections DML on collections other than Vecs, as well as iterators.
- [Ideal] Build prelude
- [Ideal] Allow Delete streams to accept Item = SalesforceId (will require decomposing the trait)
- [Ideal] Build example web client
- Why are queries &str but object names are String? Be consistent.

MVP

- Macro (?) for retrieving a static struct with all of its fields.
- Can we remove the SObjectWithId trait entirely, since we're now using early serialization?
- Implement JWT auth
- Resolve connection-as-ref or not in parameters.

  - Parallelization militates for using clone() explicitly in the parameters.
  - Should we have a separate set of \_parallel() DML methods that take `conn` without a ref?

- Error handling for requests that do not return a DmlResult is currently not great.

  - Queries with bad fields are not currently having errors handled other than via `error_for_status()`

- Add support for nested sObjects
- Add support for Blob DML
- Docs
- Tests
- Experiment with behavior of refresh of same token across multiple threads
- Add Clippy linting
- Add CI
- Consider decomposing DML traits to allow more fine-grained trait bounds

Next

- Get sObject describes only at need
- Add tracing
- Add builder for queries
- Add derive macros
- Add support for single-typed SObjects with SObjectType elided from their blanket-impl APIs.
- Implement Index and IndexMut for SObject
- Add parallelized get-whole-describe method
- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
