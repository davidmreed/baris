Cactusforce

- Add support for Bulk DML
- Implement MVP Composite
- Open source
- Queries with bad fields are not currently having errors handled.
- [Ideal] We should implement sObject Collections DML on collections other than Vecs, as well as iterators and streams.
- [Ideal] Build prelude

POSSIBLE APPROACHES TO REFERENCE HANDLING FOR IDS

Reference Ids are _only_ relevant when using Composite requests. We do not want to make
other operations less ergonomic.

I think we want to do _both_ Request early serialization _and_ changes to the SObjectWithId trait

# Handle at the Request level by providing overrides

The Request would then have to handle injecting these overrides into the serialized representation of
the SObject. We would have to use a builder pattern to be able to add back our validation for Ids.

# Handle at the Request level with early serialization

Rather than hold a mutable reference to the SObject, each Request would serialize the SObject
at creation time, and can then access whatever value is in the Id field via the Value enum
rather than our trait.

This doesn't solve the impedance mismatch with the SObjectWithId trait, and loses the property
that objects cannot be mutated while requests are in flight, but would make it more ergonomic
to compose multiple mutating requests in Composite against the same SObject.

# Handle at the SalesforceId level by making the type algebraic

Confusing because FieldValue already handles both Ids and References

# Handle at the trait level by changing the interface for SObjectWithId

Unclear what this would need to look like. Have `get_id()` return an enumerated type _above_
the level of SalesforceId? Perhaps a FieldValue? That might actually work.

How would this impact our derives? We might need a more sophisticated derive method that
can handle Id elements of different types.

Our Request trait will probably need to have more Result outputs, or a `validate()` method, or both.

MVP

- Implement JWT auth
- Resolve connection-as-ref or not in parameters.

  - Parallelization militates for using clone() explicitly in the parameters.
  - Should we have a separate set of \_parallel() DML methods that take `conn` without a ref?

- Add support for nested sObjects
- Add support for Blob DML
- Docs
- Tests
- Review and narrow trait bounds
- Experiment with behavior of refresh of same token across multiple threads
- Add Clippy linting
- Add CI

Next

- Get sObject describes only at need
- Add tracing
- Add builder for queries
- Implement Index and IndexMut for SObject
- Add parallelized get-whole-describe method
- Implement error enum based on SOAP API documentation. https://developer.salesforce.com/docs/atlas.en-us.api.meta/api/sforce_api_calls_concepts_core_data_objects.htm
