# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OxideDB is a translation layer that works as a MongoDB database server while using PostgreSQL's JSON capabilities as the underlying data store. It provides MongoDB compatibility on top of PostgreSQL, aimed at users who want MongoDB's document-based API but prefer managing a PostgreSQL database.

## Build and Run Commands

### Setup and Installation

```bash
# Install dependencies
cargo install cargo-watch # Needed for development

# Set PostgreSQL connection URL
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/test"
```

### Build and Run Commands

```bash
# Check code without building
cargo check

# Run OxideDB with automatic restarting on code changes
make start

# Run with debug output
make debug

# Run the web interface
make web
```

### Testing

```bash
# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Example: Run just the find tests
cargo test --test find_test
```

### Command-line Options

```bash
# Run with custom PostgreSQL URL
./oxide --postgres-url "postgres://postgres:postgres@localhost:5432/test"

# Run with custom address and port
./oxide --listen-addr 0.0.0.0 --port 27017

# Run with web interface
./oxide --web

# Run with debug output
./oxide --debug
```

## Architecture

OxideDB consists of these primary components:

1. **Wire Protocol Layer** (`wire/` directory):
   - Handles MongoDB wire protocol messages (OP_MSG, OP_QUERY, OP_REPLY)
   - Translates between MongoDB's binary format and Rust structures

2. **Command Handlers** (`commands/` directory):
   - Implements MongoDB commands (find, insert, update, delete, etc.)
   - Each command has its own module implementing the MongoDB command specification

3. **PostgreSQL Backend** (`pg.rs`):
   - Translates MongoDB queries to PostgreSQL SQL
   - Manages connections to PostgreSQL via r2d2 connection pool

4. **Server and Request Handling** (`server.rs`, `handler.rs`):
   - Manages TCP connections
   - Routes commands to appropriate handlers
   - Provides concurrency via threadpool

5. **Query Language Translation** (uses external `mongodb-language-model` crate):
   - Parses MongoDB query syntax
   - Translates MongoDB query operators to SQL

6. **Web Interface** (`ui.rs`):
   - Provides a simple web UI for querying the database

## Testing Strategy

Tests use a dedicated PostgreSQL database and spin up a temporary OxideDB server for each test. The `common.rs` file provides test utilities that:

1. Create a clean test database
2. Start an OxideDB server instance
3. Provide MongoDB client connections
4. Helper methods for test assertions

Tests are organized by command type (e.g., find_test.rs, insert_test.rs) and focus on specific MongoDB command behaviors.