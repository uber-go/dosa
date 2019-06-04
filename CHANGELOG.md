# Changelog

## v3.4.3 (unreleased)
 - Add optional 'columns' tag to Index definitions
 - update dosa-idl into v3.2.1
 - add columns support to materialized view creation using Schema dump
 - rename CLI command "schema check" to "schema upsertable"

## v3.4.2 (2019-03-26)
 - Update version.VERSION

## v3.4.1 (2018-11-07)
 - Remove all uses of satori.uuid

## v3.4.0 (2018-10-26)
 - CLI: Send Auth header
 - Library: send CallerName in header

## v3.3.0 (2018-10-04)
 - Switch to gofrs/uuid (#361)
 - Cleanup EnsureValidRangeConditions (#364)
 - Add Java client cli support (#363)
 - Add String() to cli output (#366)

## v3.2.1 (2018-08-31)
 - Support RFC3339Nano and unix epoch time in millisecond for CLI timestamp query (#360)

## v3.2.0 (2018-08-17)
 - Add readonly scope metadata access to CLI (#349)

## v3.1.1 (2018-07-25)
 - Add keyPrefix to the config of redis connector (#344)
 - Fixed memory connector upsert bug (#350)
 - Print CLI results in tabulated format (#351)

## v3.1.0 (2018-07-12)
 - Add functionality to not invalidate cache when upsert (#341)
 - Add query (read/range) support to CLI (#342)
 - Fix CLI help message (#346)
 - CLI help messages should all start with an uppercase character (#347)

## v3.0.1 (2018-06-07)
- Fixed version const

## v3.0.0 (2018-06-07)
- Simplified client creation when using the YARPC connector. (#321)
- Scope metadata handling, and rudimentary auth* support. The Connector interface (Admin API) has changed. (#273)
- Per-row TTL. (#323)
- Satori UUID library version 1.2 -- do not use HEAD. (#313)
- "Eq Matchers" have been removed. (#319)
- FindAll() will not return an error if there are no entities. (#316)
- Remove the notion of FQN (Fully Qualified Names). (#320)
- Convert usernaems to lowercase when constructing scope names etc. (#322)
- Initial support for Adaptive Rate Limits.
- Many accidentally exported names have been unexported. (#327)
- Add new GetEntitySchema function to the Connector interface (#335 and #338)
- Remove unused PluginFunc argument from the routing connector (#337)

## v2.6.0 (2018-04-16)
- Fix bug in invalidating fallback cache on upsert (#292)
- Implement MultiRemove and MultiUpsert in fallback connector (#298)
- Refactor fallback connector (#299)
- Implement MultiRead in fallback connector (#296)
- Implement multi upsert and multi remove methods in memory connector (#301)
- Implement multi upsert and multi remove in YARPC connector (#303)
- Added TTL support (#286)

## v2.5.3 (2018-03-22)
- Invalidate fallback cache on upsert (#285)
- Properly Interpret NotFound errors for MultiRead in the YARPC connector (#287)

## v2.5.2 (2018-02-05)
- Fix memory connector bug with compound partition keys (#281)
- Fix memory connector race condition with Read, MultiRead, Range, and Scane (#282)
- Fix memory connector race condition with Upsert and CreateIfNotExists (#283)

## v2.5.1 (2018-01-23)
-  Fix the broken 2.5.0 tag

## v2.5.0 (2018-01-19)
-  Fix memory connector bugs in remove API (#278) and findInsertionPoint (#276)
-  Exposed GetRegistrar() API in client (#272)
-  Implement multiread for dosa client (#270, #274)
-  Add encoding package which contains basic implementations of json and gob encoding (#267)
-  Fallback connector fixes, including always return all results as pointers and only writing to fallback when origin succeeds (#264, #266, #271)
-  Fix the `check` schema command in CLI, which can be used to check whether a new entity can be upserted or not (#247, #268)
-  Make scope name required for CLI schema commands, to avoid accidental access of unintended scope and misleading error message (#251)
-  Obtain redis connector success rate (#253)
-  Better error handling in yarpc connector (#258) and routing connector (#259)
-  Better error message for malformed entity tag (#260)

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
