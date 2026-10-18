# OxideDB

OxideDB translates a supported subset of the MongoDB wire protocol into PostgreSQL queries. It is an experimental document store, written in Rust. It does not implement the complete MongoDB database contract.

The core server builds without a browser frontend, JavaScript engine, or MongoDB client dependency. See [compatibility and operational limits](docs/compatibility.md) before using it with an application. This storage version is incompatible with the original unversioned JSONB representation.

## Run the core

Use the Rust toolchain pinned in `rust-toolchain.toml` and a dedicated PostgreSQL database. CI tests PostgreSQL 16; local validation also covers PostgreSQL 18.

```sh
export DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/oxide'
cargo build --locked
cargo run --locked
```

The default listener is `127.0.0.1:27017`. `--listen-addr`, `--port`, and `--postgres-url` override defaults; `OXIDE_LISTEN_ADDR`, `OXIDE_PORT`, and `DATABASE_URL` can also be set in the environment or `.env`.

The wire server has no authentication or TLS. Keep it on loopback or behind an access-controlled development network. PostgreSQL connections currently use `NoTls`; use a local socket or trusted local connection. Use a dedicated PostgreSQL role and database, never a shared application schema.

Connect with [MongoDB Shell](https://www.mongodb.com/docs/mongodb-shell/connect/):

```sh
mongosh 'mongodb://127.0.0.1:27017/?directConnection=true'
```

An optional convenience command launches the installed `mongosh` executable:

```sh
cargo run --locked --features shell -- shell
```

The previous embedded Deno/V8 shell was removed. `mongosh` must be installed separately.

## Local pipeline workbench

The optional web feature builds a small workbench directly from [public/index.html](public/index.html). It previews parameterized SQL and executes supported read-only aggregation pipelines. It does not accept arbitrary SQL.

```sh
export OXIDE_WEB_TOKEN="$(openssl rand -hex 32)"
cargo run --locked --features web -- web
```

Open `http://127.0.0.1:8087` and enter the token. The workbench requires a token of at least 32 alphanumeric characters and refuses non-loopback binds. Its API requires a bearer token. The page keeps the token only in memory. Start the wire server separately if needed; the old `--web` and `--web-addr` flags were removed.

## Tests

Point these variables at a **disposable PostgreSQL instance**. The tests create databases and collections and exercise schema deletion. They never need your existing development database.

```sh
export DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432/postgres'
export TEST_DATABASE_URL='postgresql://postgres:postgres@127.0.0.1:5432'
cargo test --locked --all-features
cargo fmt --check
cargo clippy --locked --lib --all-features -- -D clippy::correctness
cargo audit --deny warnings
```

The optional differential suite compares the supported contract with a real MongoDB server. CI supplies one; locally:

```sh
export MONGODB_REFERENCE_URL='mongodb://127.0.0.1:27018'
cargo test --locked --test differential_test -- --ignored
```

Tests share one Oxide server and client pool per temporary PostgreSQL database. Collections remain isolated between tests.

## Packaging

```sh
./scripts/package aarch64-apple-darwin
```

Install the target and its linker first when cross-compiling. The script passes the exact target to Cargo, reads the matching output directory, and writes a target-labeled archive and SHA-256 checksum under `dist/`. It does not publish anything. Release CI packages native binaries on Linux, macOS, and Windows and uploads them only for a published GitHub release.

For a core-only container:

```sh
docker build -t oxide .
docker run --rm -p 127.0.0.1:27017:27017 \
  -e DATABASE_URL='postgresql://postgres:postgres@host.docker.internal:5432/oxide' oxide
```

The web workbench is local-only and no longer has a public-facing Docker image.
