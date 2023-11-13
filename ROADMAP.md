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
* compaction, erasure
* object store, local separation-of-storage-from-compute
* Beta Open Source Release

## 2024 Q1

* multi-node kubernetes
    * serverless
    * consensus
* API authentication
* monitoring
* cloud separation-of-storage-from-compute
* refine SQL dialect
    * better control of schema-on-read
    * consolidate path languages
    * etc.

## 2024 Q2

* window functions
* adaptive indexing
* Alpha DBaaS
