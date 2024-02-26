# Endatabas Roadmap

## 2023 Q1

* `sqllogictest`
    * parser
    * compiler
    * execution engine
* rust parser spike

## 2023 Q2

* simple storage
    * arrow files
    * metadata
    * columnar execution
* transactionality

## 2023 Q3

* strongly-typed dynamic SQL
    * object literals
    * array literals
    * ISO literals
    * simple path access (SQL:2023)
    * refine error messages
* System Time `AS OF`, `BETWEEN` and `FROM`
* Non-recursive `WITH`
* Upsert using `INSERT ON CONFLICT`
* web server frontend
    * HTTP client?
    * SQL parameters
* docker image
* `endb-book`
* better errors

## 2023 Q4

* compiler and performance
* compaction, log rotation, erasure
* object store, local separation-of-storage-from-compute
* websockets
* transaction SAVEPOINT
* TPC-H 1.0
* SQL Acid Test

## 2024 Q1

* Beta Open Source Release
* TPC-C
* official clients
* monitoring
* vector functions / ops
* Wasm Playground

## 2024 Q2 / Q3 (tentative)

* refine SQL dialect
    * better control of schema-on-read
    * consolidate path languages
    * etc.
* cloud separation-of-storage-from-compute
* multi-node kubernetes
    * serverless
    * consensus
* API authentication
* window functions
* adaptive indexing
* Alpha DBaaS
