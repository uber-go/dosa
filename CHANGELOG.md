# Changelog

## v2.6.0 (unreleased)
- Add encoding package which contains basic implementations of json and gob encoding
- Fallback connector fixes, including always return all results as pointers and only writing to fallback when origin succeeds 

## v2.5.0 (unreleased)

- Coming Soon

## v2.4.0 (2017-11-28)

- Added new "Routing Connector" allowing for a connector that forwards requests to one of several other connectors (#210 & #222)
- Remove unimplemented Search API (#227)
- Better YARPC connector error messages (#230)
- Add new Test Client (#240)
- Fix bug with time.Time fields being used as part of the primary key with the in-memory connector (#244)
- Remove usage of dosa-dev-gateway in the CLI as it's no longer needed (#238)
- Better error message when trying to do a `RemoveRange` on secondary-index fields (#232)
- Issue errors when secondary index fields are not exported (#246)

## v2.3.0 (2017-08-28)

- **[Breaking]** Remove `dosaclient` package in favor of `config.NewClient` (#213 & #218)
- Improve yarpc connector errors (#211)
- Remove directory scan config option (#203)
- Add pointer support for entity fields (#209)
- Add `WalkRange` function to client API (#201)
- Implment the Cassandra connector (#180)

## v2.2.0 (2017-08-07)

- Secondary Index Support for CQL Schema Dumps (#208)
- Secondary Index Support for Avro Schema Dumps (#206)
- Enhanced, clearer RemoveRange API (#205)
- Ensure Registered Entities have index information attached (#199)
- Fix bug where embeded secondary indexes are not found (#198)
- Secondary Indexes (#192)
- Remove Range (#190)
- Add support for continuation tokens for in-memory connector (#184)
- Memory connector should return empty sets (#177)
- Validate caller and scope values before invoking YARPC dispatcher (#169)
- Add RemoveRange for in-memory connector (#158)
- Add functionality for RemoveRange in both the DevNull and Random connectors. (#156)
- Change Random and DevNull connectors to not return errors when Remove is called (#155)
- Report card improvements (#153)
- Improve error message when no entities are found (#150)

## v2.1.1 (2017-06-07)

- Fix bug when only one value is in the range (#173)
- Formatting fixes for the in-memory connector (#160)

## v2.1.0 (2017-05-24)

- In-memory connector (aka "TestClient"); you can now test your code without having a server (#147)
- Added matchers to make mocking Range and Scan easier (#146)
- Add support for custom build versions via brew (#143 & #145)
- Documentation improvements (#144)
- Better error reporting for missing or poorly formed UUIDs (#142)

## v2.0.0 (2017-05-02)

Initial release. Complete rewrite from v1 (closed-source)
