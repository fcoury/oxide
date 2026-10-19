# DxFlow local parity workflow

This checkout is being validated against the sibling `../msuite` DxFlow app and
the real HepQuant configuration in `../hepquant-config`, using synthetic records.
MongoDB remains the application's default. The Oxide candidate is isolated.

| Service | Address | Purpose |
| --- | --- | --- |
| MongoDB 6 replica set | `127.0.0.1:27017` | Reference implementation; existing app stays here |
| Oxide | `127.0.0.1:27018` | Candidate wire endpoint |
| PostgreSQL 16.15 | `127.0.0.1:55432` | Dedicated Oxide storage, Docker container `dxflow-oxide-postgres` |
| Reference app | `https://hepquant.dev.dxflow.io` | Existing MongoDB-backed app |

The synthetic fixture database is `hepquant_parity_reference` on both wire
endpoints. The normal `hepquant` database is separate. Candidate API and UI processes are available separately for retesting; the normal
app remains MongoDB-backed until the remaining acceptance work is complete.

## Current compatibility work

The four original blockers now have implementations and retained regressions:
unique root multikey tenant domains, the 92-stage detailed-order query (including
`$mergeObjects` and bounded sequential CTE compilation), real session-owned
transactions, and durable change streams backed by PostgreSQL logical decoding.
Physician lookup concatenation and persistent idle driver connections are also
covered by the retest. JIT is disabled on Oxide connections to avoid expensive compilation of the
relation query. The normal MongoDB application remains available for comparison.

Use the separate candidate endpoints for the next app retest:

- LIMS: `https://hepquant-oxide.dev.dxflow.io/queues/orders`
- Portal: `https://provider-hepquant-oxide.dev.dxflow.io/queues/test_requisitions`
- MongoDB reference LIMS: `https://hepquant-parity.dev.dxflow.io/queues/orders`

PostgreSQL `dxflow-oxide-postgres` now uses `wal_level=logical` and 100 replication
slots. Each disposable test PostgreSQL database creates its own slot. Production
roles, sizing and slot monitoring require separate operational validation.
The retained retest report is `target/manual-smoke/compatibility-retest/EVIDENCE.md`.

## Run the checks

The local Oxide build, including its shell, is installed at
`target/local-install/bin/oxide`; it runs in the `oxide-final` window of tmux session
`dxflow-local`. PostgreSQL uses local-only development credentials and contains no
production database import. Keep its container until retained data is no longer
needed; removing it removes this instance's storage.

From this repository:

```sh
python3 -m venv target/dxflow-parity-venv
target/dxflow-parity-venv/bin/pip install -r scripts/dxflow-parity-requirements.txt
target/dxflow-parity-venv/bin/python scripts/dxflow-parity.py \
  --report target/manual-smoke/dxflow-parity/parity.json
```

The command returns nonzero while any implemented probe fails. Add `--copy` to
replace the candidate fixture collections and indexes from the MongoDB reference.
It verifies endpoint identities first, restricts fixture database names, and never
mutates reference records. Scratch write probes use separate, uniquely named
databases and remove those databases afterward. Reports contain counts, hashes,
index comparisons, and outcomes rather than tenant secrets or record contents.

Large collections are compared through bounded pages because Oxide deliberately
limits each query snapshot. Canonical Extended JSON hashes preserve BSON numeric
widths, dates, IDs, binary subtypes, and array order; document key order is
canonicalized, matching Oxide's documented JSONB limitation. Index comparison
preserves compound-key order and compares uniqueness and partial filters. Numeric
index direction representations such as `1` and `1.0` are equivalent.

The reference fixture was built by cloning the existing local configuration into
an isolated database, refreshing its config, and seeding **only**
`hepquant-config/seeds/test.yml`. It includes one patient, two orders, two samples,
two runs, one test result, and reference data. `msuite config indexes` creates the
remaining empty entity collections and their indexes. The seed command drops
each included entity collection: do not run it against the regular app database
or run the whole seeds directory. Local environment files are in HepQuant's
ignored `.tmp/parity-reference.env` and `.tmp/parity-oxide.env`.

## Replay the application code

From `../msuite`:

```sh
cargo build --locked --offline --example oxide_parity_probe
./target/debug/examples/oxide_parity_probe \
  'mongodb://127.0.0.1:27017/hepquant_parity_reference?directConnection=true&retryWrites=false'
./target/debug/examples/oxide_parity_probe \
  'mongodb://127.0.0.1:27018/hepquant_parity_reference?directConnection=true&retryWrites=false&maxPoolSize=4'
```

This runs Orders, Samples, and Runs through DxFlow's actual provider stack and
MongoDB 2.8 driver. Standard output contains synthetic results; standard error
contains generated pipelines as structured trace events. The current outputs
match exactly. The provider calls also create audit records with fresh IDs and
timestamps. Re-copy the fixture after such calls before comparing its entire
audit collection; independently generated audit IDs should not be expected to
match.

Five captured list/count aggregation pipelines are retained in
`tests/fixtures/dxflow/` and replayed by the parity script. Samples uses the
driver's count operation separately; its complete result is checked by the
application probe. Keep the application probe in the loop when changing query
generation, relations, driver versions, or configuration.

## Fix and verification loop

1. Run the parity command and the actual application probe. Retain the first
   failing command and its MongoDB outcome.
2. Reduce that command to an isolated fixture in `tests/differential_test.rs`.
   Use the same BSON values on MongoDB and Oxide.
3. Fix the responsible Oxide layer, preserving rejection of unsupported
   semantics. Do not advertise session/transaction capabilities before they work.
4. Run the focused regression, the differential tests, required repository
   checks, and the original DxFlow operation. Rebuild and restart the installed
   candidate before repeating the application probe.
5. Retain the report and logs under `target/manual-smoke/dxflow-parity/`. Raw
   application output belongs in ignored evidence, not committed fixture data.

The local regression environment uses:

```sh
export DATABASE_URL=postgres://oxide:oxide-local@127.0.0.1:55432/oxide_dxflow
export TEST_DATABASE_URL=postgres://oxide:oxide-local@127.0.0.1:55432
export MONGODB_REFERENCE_URL=mongodb://127.0.0.1:27017
cargo test --locked --all-features
cargo test --locked --test differential_test -- --ignored
cargo clippy --locked --lib --all-features -- -D clippy::correctness
cargo fmt --check
```

These point tests at the dedicated PostgreSQL instance. Do not use an unrelated
PostgreSQL database: test setup creates disposable databases on that server.

## Original plan and remaining acceptance

Use this sequence to reach data and behavior parity for DxFlow's exercised
workload. Each stage produces a reviewable change, regression fixtures, and an
updated report. Keep the existing MongoDB app running until the final stage
passes. This plan does not claim complete MongoDB feature compatibility.

At plan creation, the retained baseline passed data/index checks and Orders, Samples, and Runs
provider reads. Transaction commit, transaction rollback, and change streams
failed in that baseline. Application writes, queue recovery, detailed records, tenant cache
behavior, and authenticated UI workflows need additional acceptance tests.

The original end-to-end smoke run added two confirmed blockers to that baseline:
DxFlow's required unique multikey index on `tenants.domains` prevents candidate
auth configuration from loading, and its generated `withDetails` order pipeline
contains 92 stages, exceeding Oxide's then-current 64-stage limit. MongoDB authenticates
both applications and loads the seeded order detail. Actual entity CRUD and queue
dead-lettering confirm the transaction blocker; candidate scheduling, claiming,
retry, and completion work through the queue library. Retained local evidence is
under `target/manual-smoke/e2e-20261018-213438-719c4d0/`.

### 1. Make the baseline and missing coverage reproducible

Extend `scripts/dxflow-parity.py`, `tests/differential_test.rs`, and the msuite
provider probe before implementing more protocol features.

- Record each required behavior as passing, failing, or untested. Untested
  required behavior must prevent cutover.
- Prepare isolated master/config databases and tenant records on both endpoints.
  Set all master, config, and tenant database URLs explicitly; assert endpoint
  identity before each write scenario so tenant routing cannot fall back to the
  regular MongoDB app.
- Retain the current query, index, and concurrent job-claim fixes as regressions
  in CI. Exercise msuite's MongoDB 2.8 driver and the queue dependency's driver;
  a passing PyMongo probe alone does not cover both callers.
- Expand the deterministic synthetic fixture beyond two orders: multiple
  patients, insurance relationships, diagnostics, locations, runs, statuses,
  missing/null fields, and concurrent updates. Include both small fixtures for
  debugging and a larger fixture for pagination and contention.
- Compare stable BSON values exactly. For application-generated IDs, timestamps,
  and correlation IDs, define explicit correspondence rules that retain record
  relationships and audit meaning. Do not remove fields or weaken API payload
  assertions to make comparisons pass.
- Fix the HepQuant test-framework setup failure `Missing 'url' in db`, reproduce
  it in a small fixture, and establish which configuration tests pass on MongoDB.

**Exit condition:** one repeatable reference/candidate run reports every required
scenario, including failures and untested items, without touching the normal app
database or delivering external integration messages.

### 2. Implement sessions and real transactions

Refactor `src/handler.rs`, `src/server.rs`, and `src/pg.rs` so a logical session's
transaction retains one PostgreSQL connection across wire commands. Wire
connections from a driver's pool must resolve to the same session state.

- Add bounded session state keyed by session ID and transaction number, with
  per-session synchronization, expiration, and explicit cleanup. Reserve pool
  capacity for ordinary requests; avoid holding one global session lock while
  a database operation waits.
- Separate command-level atomic writes from the enclosing transaction. Existing
  `PgDb` helpers start their own transactions; they must never commit an outer
  entity transaction. Define error/rollback behavior against MongoDB before
  choosing nested-operation or savepoint mechanics.
- Implement transaction metadata, commit, abort, read-your-writes, isolation,
  write conflicts, duplicate-key failure, and cross-collection atomicity.
- Test commit retries, lost commit replies, restarts, stale transaction numbers,
  unsupported concerns, and session-bound cursors. Implement the corresponding
  error codes and labels so drivers retry only the appropriate operation.
- Verify the handshake and topology behavior with both installed Rust drivers.
  Advertise session/transaction capability only after the implementation passes;
  do not invent replica-set durability or accept unsupported concerns silently.

Use the [MongoDB transaction specification](https://specifications.readthedocs.io/en/latest/transactions/transactions/)
to distinguish transaction failures from an uncertain commit outcome.

**Exit condition:** MongoDB and Oxide agree on committed state, rollback,
visibility to other sessions, conflict outcomes, and retry outcomes, including
connection-pool exhaustion and interrupted clients.

### 3. Verify application writes and the real job queue

Extend the msuite probe or integration suite to execute the actual entity and
queue paths. Raw CRUD probes remain useful, but these callers determine whether
DxFlow can operate correctly.

- Create, update, and delete synthetic records through DxFlow. Verify hooks,
  sequence allocation, version checks, audit records, and entity history.
- Inject a failure between a business-record write and its history write. Both
  must roll back. Test conflicting versions from concurrent clients.
- Exercise the installed aide-de-camp scheduler and workers: scheduling,
  exclusive claims, acknowledgement, failure/retry, worker interruption, and
  transactional movement from `adc_queue` to `adc_dead_queue`.
- Inject a failure between deleting the active job and inserting its dead-letter
  record. Verify that a job cannot disappear or remain in both queues after the
  transaction resolves.
- Match the reference queue's delivery/recovery contract. Distinguish exclusive
  claiming from exactly-once external execution; a worker crash can require
  retrying work. Use deterministic local handlers for side effects.

**Exit condition:** real entity/history writes and queue state transitions match
MongoDB, including injected failures and worker restarts.

### 4. Implement durable change streams and cache invalidation

Extend `src/commands/aggregate/mod.rs`, `src/cursor.rs`, and `getMore` handling to
serve change streams separately from today's bounded snapshot cursors.

- First choose and prove the committed-event source. Evaluate PostgreSQL logical
  decoding against a durable journal with explicit commit-order guarantees.
  Plain sequence IDs allocated before commit are insufficient: a late commit
  must not fall behind a consumer's saved position. PostgreSQL's
  [logical decoding concepts](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html)
  describe the native stream and replication-slot lifecycle to evaluate.
- Capture all supported insert/update/replace/delete paths atomically with their
  writes. Aborted transactions must publish no committed events.
- Implement the watch options DxFlow uses, especially `fullDocument:
  updateLookup`, plus event/document keys, resume tokens, empty batches,
  `getMore` waiting, cursor cleanup, reconnect, and restart recovery.
- Define retention and expired-token behavior. Test consumers restarting after
  events have accumulated and concurrent transactions committing out of order.
  Follow the [MongoDB change-stream specification](https://specifications.readthedocs.io/en/latest/change-streams/change-streams/)
  for resumability and driver-visible behavior.
- Run the existing tenant settings watcher unchanged. Mutate tenant A while
  tenant B is active; verify updated settings, correct routing, reconnect
  invalidation, and absence of cross-tenant data exposure.

**Exit condition:** the real watcher observes committed changes, recovers after
disconnect/restart, and invalidates settings correctly with the watcher enabled.

### 5. Close query, BSON, index, and resource-limit gaps

Expand the captured workload to include detailed orders, nested insurance and
sample/storage relations, filters, search, dashboard aggregations, pagination,
and provider-portal queries. Fix the first failing generated command at a time.

- Test null versus missing, nested arrays, variable scope, invalid operand types,
  numeric comparisons, sort order, and duplicate/partial-index constraints.
- Implement the required single-field unique multikey semantics for
  `tenants.domains` before attempting candidate login again. Cover duplicate
  domains across tenants, repeats within one tenant, updates, and concurrent
  writes; preserve index enforcement rather than bypassing tenant indexing.
- Reduce and support the captured 92-stage detailed-order query with bounded
  execution. Do not only raise the stage limit: verify SQL complexity, nested
  operators, result parity, memory, and latency, then expose and fix any subsequent
  failure in the same real provider call.
- Audit documented restrictions: top-level output paths, lookup variables,
  expression operators, array projections, path depth, BSON ordering, and index
  features. Any restriction used by DxFlow remains a cutover blocker.
- Test document-order-sensitive equality explicitly. If the workload requires
  field order that JSONB discards, design an order-preserving representation and
  an explicit storage migration; do not claim that canonical hashes prove it.
- Exercise queries above 10,000 records and combined results above 8 MiB, plus
  large valid documents. Determine which app operations need streaming/paging
  support instead of only paging the validation script. Preserve bounded memory
  and explicit errors; do not hide incompatibility by dropping records.

**Exit condition:** the complete captured workload agrees with MongoDB on
results, types, ordering where meaningful, errors, and index enforcement. Every
remaining documented limitation has an explicit workload disposition.

### 6. Stress concurrency, recovery, and performance

Keep the current collection write lock while establishing correctness. Measure
whether it meets the application's needs before replacing it with narrower
locking and predicate rechecks.

- Stress concurrent claims, sequence increments, version conflicts, unique
  inserts, and multi-record writes. Retain the duplicate-claim regression.
- Test deadlocks, lock timeouts, transaction expiration, abandoned cursors,
  stalled stream consumers, and connection-pool exhaustion. Verify cleanup
  restores capacity and does not leave locks or partial writes.
- Compare latency and throughput for the same workload and dataset on each
  engine. Record p50/p95, query plans, memory, and pool usage, and set concrete
  interactive/job latency budgets from the reference measurements.
- Recheck the generated SQL materialization strategy and index usage. Remove
  collection-wide serialization only when narrower locking passes the same
  correctness, failure-injection, and load tests.

**Exit condition:** repeated workload runs preserve invariants, recover resources
after failures, and meet the recorded latency budgets without unexplained stalls.

### 7. Verify both applications and perform the local cutover

Run a separate candidate API on port 5556 and frontend on 5175. Add an isolated
tenant hostname and proxy configuration using the candidate tenant/environment
from stage 1. Confirm every routed database URL reaches Oxide rather than the
reference.

- Verify authenticated LIMS and provider-portal login, listings, detail views,
  forms, edits, workflow transitions, dashboards, and completed job results.
- Compare API responses and semantic database changes with MongoDB. Verify data,
  index definitions, committed writes, and watcher recovery after restarting
  Oxide, PostgreSQL, API, and workers.
- Replace the parity report's hard-coded `cutover_allowed: false` with a result
  derived from the complete acceptance matrix. Required untested cases or
  failures must keep it false.
- Retain the passing report, source/binary versions, fixture manifest, and
  rollback environment. Switch the regular local app to the verified candidate
  and repeat a focused smoke test before continuing development on Oxide.

**Exit condition:** every required data, behavior, recovery, and authenticated
application check passes; the app uses Oxide; MongoDB remains available for
reference comparisons and rollback.

## Next steps

- Retest the candidate endpoints and extend realistic workflow coverage.
- Complete tenant-cache isolation, worker interruption/recovery and workload
  sizing before changing the normal app database. The implementation sections
  above and compatibility contract describe current behavior; the original
  seven-stage list records the wider acceptance scope.
- Keep the [compatibility contract](compatibility.md) synchronized with each
  implemented capability and its regression evidence.
