# Architecture

Endatabas is written in a mix of Rust and Common Lisp. This document aims to give you a high-level view of how it all hangs together.

## Overview

The Rust code lives under `lib` and is a C dynamic library, which is loaded into the Common Lisp process. Normal C FFI is used to talk between the languages. The bulk of the system is currently written in Common Lisp, and lives under `src`. The idea is to incrementally move more code into Rust over time as it stabilises.

The reason we use Common Lisp is twofold:

1. It's faster to grow the system organically this way initially for a small team.
2. Endatabas needs a strongly typed dynamic runtime for its SQL dialect. This is harder to replace with Rust, but one can imagine WASM, JavaScript, Lua or some other bespoke target for the query engine.

We keep all Common Lisp dependencies as submodules Under `_build/`, see `_build/setup.lisp` for more about this.

The main target is x86-64 SBCL and Rust, but the system can also be built for ECL which supports running on Emscripten and WASM, but without persistence or HTTP API.

## Entry Points

`src/core.lisp` contains the entry point of the system, but this quickly hands over to Rust.

`lib/endb_lib` contains the C FFI boundary, at startup it's also used to set up logging.

`lib/endb_server` is the crate that parses the command line and starts the HTTP server.

`src/http.lisp` implements the actual HTTP request handler used by the Rust server.

`src/sql.lisp` is the internal API, and is used directly by tests without running the full server.

## Code

### `lib`

Workspace crate for the entire Rust library.

### `lib/endb_arrow`

Uses Rust Arrow to read and write Arrow IPC (interprocess communication) files and buffers.

### `lib/endb_cst`, `lib/endb_proc_macro`

The parser for the Endatabas SQL dialect. It's an event-based PEG parser.

### `lib/endb_lib`

Contains the C FFI boundary and wraps the other crates. Also contains logging and utility functions not warranting their own crate.

### `lib/endb_server`

The Endatabas HTTP API server implemented using Tokio and Hyper. Also contains the command line parser and tracing configuration.

### `src/arrow.lisp`

Implementation of in-memory Arrow, with promotion of polymorphic vectors into Arrow dense unions vectors on demand. Maps scalar Common Lisp types to Arrow types.

### `src/bloom.lisp`

Implementation of the Split Block Bloom Filter, which is the algorithm used by Parquet.

### `src/core.lisp`

The main entry point.

### `src/http.lisp`

HTTP and WebSocket request handler. Executes transactions optimistically first, and then pessimistically if that fails due to conflicts. Takes the write lock to commit.

### `src/json.lisp`

Mapping of JSON-LD types to internal Common Lisp and Arrow types and a JSON Merge Patch implementation.

### `src/lib/`

Directory containing Common Lisp packages that use C FFI to talk to Rust. Uses callbacks to keep most things stack allocated to avoid moving ownership back and forth between Rust and Common Lisp.

### `src/queue.lisp`

A simple blocking task queue. Used by the indexer, snapshot and compaction threads.

### `src/sql.lisp`

Internal API for the query engine and compiler. Coordinates transaction commit but relies on the caller to hold the write lock.

### `src/sql/compiler.lisp`

The SQL compiler, takes the parsed query and turns it into Common Lisp.

### `src/sql/db.lisp`

Implements the DDL (data definition) and DML (data manipulation) parts of SQL. Also deals with column statistics and the compaction of Arrow files.

### `src/sql/expr.lisp`

Implements the expression language and relational algebra helpers used by the compiled SQL queries.

### `src/storage.lisp`

Handles the durable parts of the database like WAL replay, rotation and database snapshots.

### `src/storage/buffer-pool.lisp`

A simple buffer pool providing an in-memory view of Arrow files stored in the object store.

### `src/storage/object-store.lisp`

The object store stores the Arrow files, WAL backups and database snapshots. Contains directory system and memory object stores.

### `src/storage/wal.lisp`

The write ahead log is implemented as a tar file. This file contains newly written Arrow data and transaction metadata as JSON merge patches derived from the current and new database state.

## Cross-Cutting Concerns

### Data Model

The database state is an in-memory persistent data structure which is updated atomically on commit. It tracks the active Arrow files across all tables, and contains statistics such as min, max and bloom filters for each column. It also contains deletion vectors. This state is initialised by reading the latest snapshot and then replaying later WALs on startup.

The Arrow type system is used consistently across the database engine, but often in its Common Lisp scalar form. The Arrow type system is also mapped to JSON-LD using XML Schema Definition (XSD).

### FFI

The library aborts on Rust panics, so all panics should be considered bugs.

Care needs to be taken to avoid unwinding from Common Lisp across Rust, as this doesn't actually drop and unwind the Rust side properly, leading to leaks.

Passing ownership between Rust and Common Lisp is mostly avoided and callbacks are used instead. In some places more advanced data structures are simply passed as JSON.

Common Lisp can send pointers to arrays down into Rust, but the arrays need to be pinned to ensure that the garbage collector doesn't move them.
