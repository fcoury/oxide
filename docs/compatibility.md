# Compatibility contract

Oxide supports a bounded subset of MongoDB commands. A wire-version handshake permits client communication; it is not a claim of full MongoDB 5.0 compatibility. `buildInfo` includes the actual `oxideVersion` and `storageVersion`; it no longer reports an invented Git revision.

## Implemented behavior

| Area | Supported contract |
| --- | --- |
| Writes | Ordered/unordered insert, update and delete batches with indexed write errors. Duplicate `_id` returns 11000. Replacements preserve `_id`; changing it is rejected. |
| Updates | Replacement, `$set`, `$unset`, `$inc`, `$addToSet`/`$each`; dotted paths through documents; equality-seeded upserts; accurate matched/modified/upserted counts. |
| Atomicity | Each update entry takes a collection write lock before selecting its preimages, applies every operator, and commits once. Writes to a collection are serialized; readers remain concurrent. Failure rolls that entry back, including all its selected documents. Separate entries in a batch commit independently. This is stronger atomicity for one multi-update entry than MongoDB promises. |
| Find | Filter, scalar sort, skip, limit, inclusion/exclusion projection, batch size, single batch, getMore and killCursors. Projection through arrays is explicitly unsupported. |
| Filters | `$eq`, `$ne`, comparisons, `$in`, `$nin`, `$and`, `$or`, `$nor`, `$not`, `$exists`, regex. Scalar matching traverses arrays; negative predicates negate positive existence. Regex uses the intersection of Rust/PostgreSQL syntax, not all MongoDB PCRE features. |
| Aggregation | Ordered `$match`, `$sort`, `$skip`, `$limit`, `$project`, `$group`, `$count`, `$lookup`, `$set`/`$addFields`, `$unset`, `$unwind`, and `$replaceRoot`/`$replaceWith`, including an empty pipeline. Group supports `$sum`, `$avg`, and `$first` in input order; expressions include `$literal`, arithmetic, UTC `$dateToString`, `$cond`, `$eq`, `$gte`, `$let`, two-operand `$indexOfArray`/`$arrayElemAt`, `$map`, `$reduce` with `input`/`initialValue`/`in`, `$concatArrays` expressions, and `$filter` without `limit`. Lookup supports local/foreign fields and nested pipelines, without `let` variables. Lookup, set/addFields, unwind, and includeArrayIndex output fields must be top-level. Aggregation field traversal supports document/array paths up to eight levels; nested lookup pipelines are limited to sixteen levels. Mixed-type comparisons retain the JSONB limitations described below. |
| findAndModify | Update or remove, query, scalar sort, upsert, and explicit `new`; defaults to the preimage. |
| Count | `query`, skip and limit. |
| Indexes | Collection-scoped names, deterministic physical names, catalog-backed definitions, mandatory unique `_id`, scalar secondary indexes, and a partial filter containing one `{field: {$type: "string"}}` condition. Unique secondary indexes normalize numeric widths and missing/null; single-field root indexes also enforce multikey uniqueness through transactional expanded keys. Compound/nested multikey and embedded-document keys are rejected. |
| Protocol | Checked OP_MSG bodies/document sequences in either order, and legacy OP_QUERY handshake. OP_MSG CRC32C checksums are validated when present; compression remains unsupported and rejected. |
| HTTP | Loopback-only, bearer-token API; 64 KiB input; four concurrent database operations; only compiler-generated read queries. |

Unsupported data-command options return an error, including retryable-write metadata, collation and write concerns other than `w:1` (with the commit exception documented below). There is no replication, sharding, authentication, authorization, arbitrary SQL API, or full BSON comparison ordering.

JSONB canonicalizes document field order. Embedded-document equality and ordering therefore do not reproduce MongoDB's order-sensitive behavior. Grouped numeric keys are canonicalized across numeric widths. Mixed-type and array sorting follow PostgreSQL JSONB behavior and are not a complete BSON sorting implementation. Decimal128, regex and other opaque BSON types round-trip, but their query/arithmetic semantics are not implemented. Nonfinite numeric comparisons are rejected. The legacy nested-query shorthand remains supported; prefer dotted paths, and use `$eq` for exact embedded-document equality.

Secondary expression indexes enforce constraints and are listed accurately. The general JSONPath filter compiler does not yet promise that every predicate will use those expression indexes. Query-plan optimization requires separate measured work.

## Resource bounds

- Wire frames: 48,000,000 bytes; individual BSON documents: 16 MiB; nesting: 64 levels; at most four OP_MSG sections with one command body and unique sequence identifiers.
- Write batches: at most 10,000 entries. Filters/pipelines: at most 64 KiB; pipelines: 256 stages; individual compiled expressions: 256 KiB.
- Query snapshots and update preimages: at most 10,000 documents and 8 MiB of BSON. Exceeding either produces an error; results are not silently truncated. A selective filter or limit can keep a query within bounds.
- Cursors are bounded snapshots, shared across pooled client connections. They hold no PostgreSQL connection. At most 64 cursors and 32 MiB of retained BSON per server; 60-second idle expiration. `getMore` uses the namespace and cursor id; it does not re-query changed data. Expired state is reclaimed on the next cursor operation.
- Wire serving: 32 workers and 32 queued connections. Excess connections are closed. Idle pooled connections remain open; once the first byte arrives, frames have a 30-second read deadline and replies have a 30-second write deadline. PostgreSQL uses at most 10 pooled connections, a 30-second statement timeout and five-second lock/checkout timeouts.

These are correctness and resource limits for the current experimental implementation, not production sizing guarantees. Batch size does not bypass the snapshot limit.

## Storage version and legacy databases

Owned schemas have the PostgreSQL comment `oxide:database:v1`; collections have `oxide:jsonb:v1`. Native BSON values use reserved internal tags, and user keys starting with `$` are escaped. The codec preserves Int32/Int64/double distinctions, negative zero, nonfinite doubles, dates, ObjectIds, binary subtypes, regex and other BSON scalars. Strings containing NUL use the opaque BSON representation because PostgreSQL JSONB cannot store NUL directly.

Each collection has separate `_jsonb` and canonical `_id` columns and a unique identity index. Numeric `_id` values compare across numeric widths. Array, regex and nonfinite identities are rejected.

**Do not add version comments to old tables.** They are not migrations. The old encoding can be ambiguous or already lossy, so automatic reinterpretation cannot safely recover the original values. Unversioned schemas/collections are rejected before access or mutation. Only owned schemas appear as databases; PostgreSQL's `public` schema is not an Oxide database.

To migrate:

1. Back up the original PostgreSQL database and retain the original Oxide binary.
2. Export documents through the original version to BSON or canonical Extended JSON. Verify representative ObjectIds, numeric widths, dates, regexes and user fields resembling tags against the source application. Already lost type information needs application-specific repair.
3. Create a separate PostgreSQL database for this storage version, import the verified documents through the wire API, and recreate supported indexes.
4. Compare counts and representative queries, then switch clients. Keep the old database unchanged until the application is verified.

There is intentionally no in-place migration command or automatic schema rewrite.

## Embedded shell

The optional `shell` feature uses QuickJS-NG and a MongoDB client; neither is
compiled into the default core build. The owned shell retains the original eager
array API and speaks the same wire protocol as external clients. See the
[README](../README.md#oxide-shell) for supported commands, BSON constructors,
resource limits, and cancellation behavior. Native release archives include the
shell, while the server container stays core-only.

## Release and verification policy

Core and optional features compile separately. CI runs on pull requests, pins Rust/PostgreSQL/MongoDB versions, exercises a current MongoDB driver, runs differential tests, and audits the complete lockfile without ignored advisories. The primary sources for selecting database pins are the [PostgreSQL version policy](https://www.postgresql.org/support/versioning/) and [MongoDB 8.0 release notes](https://www.mongodb.com/docs/manual/release-notes/8.0/).

Local PostgreSQL tests, a dependency audit, and a build do not establish remote CI status, platform coverage, or production readiness. Linux/Windows release artifacts must be produced by their own runners; a macOS binary is never relabeled as a Linux binary.

## Sessions and transactions

Logical sessions and multi-command transactions use a pinned PostgreSQL connection
at repeatable-read isolation. Command-local savepoints preserve the enclosing
transaction. Commit and abort affect every collection written in the transaction;
failed writes abort it. Serialization/deadlock conflicts return MongoDB code 112
with `TransientTransactionError`. Retried commits are idempotent during the
session lifetime. Transactions expire after 60 seconds of inactivity; logical
session metadata expires after 30 minutes. Session metadata is bounded to 512
entries. Active transactions count against the PostgreSQL connection pool.
Transaction cursors cannot be consumed outside their transaction and close on
commit or abort. Retryable writes are unsupported: use `retryWrites=false`.
Commit supports `w:1` and single-node `w:"majority"` with synchronous PostgreSQL
commit; Oxide provides no replica failover or replicated durability guarantee.

## Change streams

Collection change streams support `fullDocument:"updateLookup"`,
`resumeAfter`, bounded `getMore`, and supported follow-on aggregation stages
without lookups. `oxide_internal` holds the durable journal and instance identity;
it is excluded from user database/collection catalogs. Collection triggers emit
transactional WAL messages. PostgreSQL's built-in `pgoutput` plugin publishes
committed messages in commit order through a dedicated logical replication slot.
The collector stores events idempotently by message LSN and commits the journal
before advancing the slot. A crash between those steps replays already-journaled
messages without duplicating events. No application write lock is held by the
collector, so hooks may write through separate connections during a transaction.

PostgreSQL must run with `wal_level=logical`, an available replication slot for
each Oxide PostgreSQL database, and a role allowed to create/use logical slots
and publications. Oxide fails startup if these prerequisites are missing. A
publication with no tables avoids decoding user tuples twice; transactional
messages carry the versioned BSON representation. The collector polls every
100 ms and on stream reads. Opening a stream drains the committed backlog before
choosing its starting token, with a 25-second initialization budget. Decoding
batches are bounded to 10,002 messages and 64 MiB; a larger transaction fails
explicitly without acknowledging its WAL. Monitor inactive slots and retained WAL; an unavailable
or lost slot fails explicitly rather than silently recreating stream history.
Stop Oxide and remove its slot only when intentionally discarding that database's
stream history. Disposable test databases also create slots and must be cleaned
up after tests. CI configures 100 slots for its disposable test server.

The journal retains up to 100,000 events and 24 hours, with background cleanup
every minute. Expired resume tokens return `ChangeStreamHistoryLost` (286).
Update lookups read the current committed document. Stream cursors expire after
60 idle seconds and survive connection changes through a durable resume token,
rather than retaining a PostgreSQL connection. Database-wide watches, DDL events,
preimages, and exact nested-field update descriptions are not implemented.

## Unique multikey indexes

Unique single-field root indexes expand scalar array elements into an internal
key table. Duplicate keys within one document collapse; keys shared by different
documents fail atomically, including during backfill and concurrent writes.
Updates/deletes maintain the key table transactionally. Nested and compound
multikey indexes, nested arrays, and document-valued unique keys remain unsupported.

## Aggregation execution

Aggregation uses sequential materialized CTEs, with a 256-stage limit and the
existing byte, nesting, parameter and SQL-size budgets. `$concat` supports string concatenation with null/missing propagation.
`$mergeObjects` supports expression operands, null skipping and last-document field precedence. Oxide
sets PostgreSQL JIT off on each connection: compilation costs on deeply nested
JSONB expressions can dominate query execution time.

Whole-number BSON doubles are accepted for integer command options when they fit in signed 64-bit range. Fractional, nonfinite, and out-of-range values are rejected. The stock upstream shell exercises this representation in the [upstream compatibility suite](upstream-tests.md).
