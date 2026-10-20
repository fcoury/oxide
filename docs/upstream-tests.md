# Run upstream compatibility tests

Use `scripts/upstream-compat.py` to run original MongoDB JavaScript tests against an isolated MongoDB reference and Oxide. The runner uses the public `mongo` shell and the upstream test files and helper libraries without replacing assertions. It follows MongoDB's supported direct-shell execution path rather than embedding a partial shell or requiring a Resmoke installation.

The initial manifest contains 56 required tests and two explicit exclusions. Passing this baseline covers only those selected cases. It does not establish full MongoDB compatibility.

## Pinned inputs

`tests/upstream/manifest.json` records:

- MongoDB server-test corpus 5.0.31 at a fixed commit and archive SHA-256.
- The matching public 5.0.31 shell image, pinned by Docker digest.
- MongoDB 8.0.34 as the reference server.
- Each selected test's path, required/extended/excluded tier, and reason.

The 5.0 corpus aligns with Oxide's advertised wire version and has a publicly available matching test shell. MongoDB 8.0 release archives omit that internal shell; accessing its Evergreen build artifacts requires authentication. This version choice is explicit rather than substituting `mongosh` or changing tests to fit another interpreter. Expanding to a newer corpus requires verifying its shell and helpers first.

Only the source archive is cached under `target/upstream-cache/`. Every run verifies its checksum and extracts a fresh test tree, including upstream helpers and license files. Tests are not copied into Oxide's source or edited. Updates to the revision, shell image, reference version, or test selection belong in a reviewed PR.

## Prerequisites

- Python 3.9 or newer and Docker with host networking. Linux runners support this directly; local execution was verified with OrbStack on macOS. Docker Desktop installations need functioning host networking.
- A dedicated MongoDB 8.0.34 reference endpoint, without credentials, bound to loopback. The runner writes disposable databases to it.
- For a managed Oxide candidate: dedicated loopback PostgreSQL with a database named `oxide_upstream` or another `oxide_upstream*` name, `wal_level=logical`, and available replication slots.
- The Rust toolchain from `rust-toolchain.toml` if building Oxide locally.

Do not use a production/reference-data server. Each test gets a random `oxide_upstream_*` MongoDB database and the runner drops that database afterward with explicit `w:1`. The two excluded cases assume the literal database name `test` or perform a 100,000-document insertion outside the baseline time budget. The curated tests do not start replica sets or sharded clusters.

## Run the baseline

Build the native package and test its extracted executable. For example, on Apple Silicon using Fish:

```fish
bash scripts/package aarch64-apple-darwin
set -gx UPSTREAM_POSTGRES_URL postgres://oxide:oxide-local@127.0.0.1:55434/oxide_upstream
python3 scripts/upstream-compat.py \
  --reference mongodb://127.0.0.1:27023/ \
  --oxide-archive dist/oxide_0.1.7_aarch64-apple-darwin.tar.gz \
  --report-dir target/upstream-report
```

Use the actual version and target archive produced by `scripts/package`. On Linux, the native target is normally `x86_64-unknown-linux-gnu`. The runner starts and stops only its own candidate process. It checks that the reference is MongoDB at the pinned version and that the candidate identifies itself as Oxide.

For an existing isolated candidate, replace `--oxide-archive` with `--candidate mongodb://127.0.0.1:27024/`; `--postgres-url` is unnecessary in this mode. This is useful for debugging, but release validation uses the packaged executable.

Expected result: all 56 required tests pass, `gate_passed` is true, and the process exits zero. Empty selections, missing selected files, unexpected shell termination, timeouts, reference failures, source checksum failures, and cleanup failures cannot pass the gate. Timed-out shell containers are explicitly removed. A failed test still attempts database cleanup.

## Discover additional gaps

The initial 23 extended cases now pass unchanged and are part of the required baseline. `--mode discovery` currently executes the same 56 cases; it also runs any future `extended` entries. Every reference case must pass. Required failures and harness errors fail the gate; future extended compatibility failures remain visible without blocking the supported baseline.

The promoted cases cover document field order, numeric projection flags, `$push` and `$addToSet`, query types/options and validation, aggregation expressions/error codes, and multi-batch results. The original assertions stay intact, including order-sensitive assertions. Resolve a future gap before promoting its entire test to `required`; do not remove inconvenient assertions.

## Inspect results

The report directory contains:

- `results.json`: source revision, manifest and executable/package hashes, endpoint versions, test hashes, outcomes, exclusions, and the baseline gate result.
- `junit.xml`: test outcomes for CI/report consumers. Extended failures remain failures here even when the supported baseline gate passes.
- `summary.md`: baseline and discovery counts.
- Per-test reference/candidate transcripts and, for managed candidates, `oxide.log`.

Exit codes are 0 for a passing baseline gate, 1 for a failed required/reference comparison, and 2 for a harness/setup failure. Missing completion markers cannot turn a skipped test into a pass. Reports are written on setup failures too.

Run harness failure-path regressions with:

```fish
python3 -m unittest discover -s tests/upstream -p 'test_*.py' -v
```

## CI and release behavior

`.github/workflows/upstream-compatibility.yml` runs the required baseline on PRs and `main`, discovery nightly, and either mode on manual dispatch. It retains JSON/JUnit/log artifacts and a job summary. It is also reusable by the release workflow.

The release workflow now starts on a version-tag push or manual dispatch, rather than after publication:

1. Validate that the version tag matches `Cargo.toml` and resolve the candidate commit.
2. Run the Rust regression suites and the complete required upstream baseline against the packaged Linux executable from that commit.
3. Retain that exact tested Linux archive and build native macOS and Windows packages from the same commit.
4. Verify package checksums and that the tag still resolves to the tested commit.
5. Publish only after every prerequisite succeeds. An already published release is never replaced.

On manual dispatch, `publish` defaults to false: this tests the dispatch commit and builds all three platform packages without creating a tag or publishing a release. Supply a prospective matching version tag, such as `v0.1.7` for version 0.1.7. With `publish=true`, the tag must already exist and identify the tested commit. A pushed `v*` tag requests validation followed by publication.

These checks govern this repository's release workflow. They do not prevent a maintainer from manually publishing through GitHub outside that workflow. Run the workflow's dry-run path when validating changes to release automation; do not create a real release merely to test it.
