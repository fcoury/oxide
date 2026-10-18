# Compatibility contract

Oxide supports a bounded subset of MongoDB commands. A wire-version handshake permits client communication; it is not a claim of full MongoDB 5.0 compatibility. `buildInfo` includes the actual `oxideVersion` and `storageVersion`; it no longer reports an invented Git revision.

## Implemented behavior

| Area | Supported contract |
| --- | --- |
| Writes | Ordered/unordered insert, update and delete batches with indexed write errors. Duplicate `_id` returns 11000. Replacements preserve `_id`; changing it is rejected. |
| Updates | Replacement, `$set`, `$unset`, `$inc`, `$addToSet`/`$each`; dotted paths through documents; equality-seeded upserts; accurate matched/modified/upserted counts. |
| Atomicity | Each update entry selects and locks its preimages once, applies every operator, and commits once. Failure rolls that entry back, including all its selected documents. Separate entries in a batch commit independently. This is stronger atomicity for one multi-update entry than MongoDB promises. |
| Find | Filter, scalar sort, skip, limit, inclusion/exclusion projection, batch size, single batch, getMore and killCursors. Projection through arrays is explicitly unsupported. |
| Filters | `$eq`, `$ne`, comparisons, `$in`, `$nin`, `$and`, `$or`, `$nor`, `$not`, `$exists`, regex. Scalar matching traverses arrays; negative predicates negate positive existence. Regex uses the intersection of Rust/PostgreSQL syntax, not all MongoDB PCRE features. |
| Aggregation | Ordered `$match`, `$sort`, `$skip`, `$limit`, `$project`, `$group`, `$count`, including an empty pipeline. Group supports `$sum` and `$avg`; expressions support `$literal`, arithmetic, and UTC `$dateToString` formats `%Y` and `%Y-%m-%d`. |
| findAndModify | Update or remove, query, scalar sort, upsert, and explicit `new`; defaults to the preimage. |
| Count | `query`, skip and limit. |
| Indexes | Collection-scoped names, deterministic physical names, catalog-backed definitions, mandatory unique `_id`, scalar secondary indexes. Unique secondary indexes normalize numeric widths and missing/null; constraints reject arrays and embedded-document keys instead of pretending to support multikey uniqueness. |
| Protocol | Checked OP_MSG bodies/document sequences in either order, and legacy OP_QUERY handshake. Compression and OP_MSG checksums are unsupported and rejected. |
| HTTP | Loopback-only, bearer-token API; 64 KiB input; four concurrent database operations; only compiler-generated read queries. |

Unsupported data-command options return an error, including sessions, transactions, retryable-write metadata, collation and write concerns other than `w:1`. There is no replication, sharding, authentication, authorization, change stream, transaction API, arbitrary SQL API, or full BSON comparison ordering.

JSONB canonicalizes document field order. Embedded-document equality and ordering therefore do not reproduce MongoDB's order-sensitive behavior. Grouped numeric keys are canonicalized across numeric widths. Mixed-type and array sorting follow PostgreSQL JSONB behavior and are not a complete BSON sorting implementation. Decimal128, regex and other opaque BSON types round-trip, but their query/arithmetic semantics are not implemented. Nonfinite numeric comparisons are rejected. The legacy nested-query shorthand remains supported; prefer dotted paths, and use `$eq` for exact embedded-document equality.

Secondary expression indexes enforce constraints and are listed accurately. The general JSONPath filter compiler does not yet promise that every predicate will use those expression indexes. Query-plan optimization requires separate measured work.

## Resource bounds

- Wire frames: 48,000,000 bytes; individual BSON documents: 16 MiB; nesting: 64 levels; at most four OP_MSG sections with one command body and unique sequence identifiers.
- Write batches: at most 10,000 entries. Filters/pipelines: at most 64 KiB; pipelines: 64 stages; individual compiled expressions: 256 KiB.
- Query snapshots and update preimages: at most 10,000 documents and 8 MiB of BSON. Exceeding either produces an error; results are not silently truncated. A selective filter or limit can keep a query within bounds.
- Cursors are bounded snapshots, shared across pooled client connections. They hold no PostgreSQL connection. At most 64 cursors and 32 MiB of retained BSON per server; 60-second idle expiration. `getMore` uses the namespace and cursor id; it does not re-query changed data. Expired state is reclaimed on the next cursor operation.
- Wire serving: 32 workers and 32 queued connections. Excess connections are closed. Whole frames have a 30-second read deadline and replies have a 30-second write deadline. PostgreSQL uses at most 10 pooled connections, a 30-second statement timeout and five-second lock/checkout timeouts.

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

## Release and verification policy

Core and optional features compile separately. CI runs on pull requests, pins Rust/PostgreSQL/MongoDB versions, exercises a current MongoDB driver, runs differential tests, and audits the complete lockfile without ignored advisories. The primary sources for selecting database pins are the [PostgreSQL version policy](https://www.postgresql.org/support/versioning/) and [MongoDB 8.0 release notes](https://www.mongodb.com/docs/manual/release-notes/8.0/).

Local PostgreSQL tests, a dependency audit, and a build do not establish remote CI status, platform coverage, or production readiness. Linux/Windows release artifacts must be produced by their own runners; a macOS binary is never relabeled as a Linux binary.
