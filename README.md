# go-sql-lsh

[![Build Status](https://travis-ci.org/ekzhu/go-sql-lsh.svg?branch=master)](https://travis-ci.org/ekzhu/go-sql-lsh)

This is an experimental implementation of Locality Sensitive Hashing (LSH)
index using relational databases.
This library does not implement any specific locality-sensitive hash function
family,
but provides a generic storage backend for the hash values.
See [Documentation](https://godoc.org/github.com/ekzhu/go-sql-lsh)
for details.

Currently only Sqlite and PostgreSQL are supported.

To install:

```
go get github.com/ekzhu/go-sql-lsh
```

To run the tests and benchmarks, you need to install the Go
libraries for PostgreSQL and Sqlite3:

```
go get github.com/lib/pq
go get github.com/mattn/go-sqlite3
```

A performance comparison is shown in the table below.
Numbers are average query times, in millisecond. 
There are 10,000 signatures in the index for all runs.

| Signature Size  | Sqlite  | PostgreSQL  |
|-----------------|---------|-------------|
| 128 (k=2, l=64) | 60.59   | 35.34       |
| 256 (k=4, l=64) | 76.92   | 36.09       |
| 512 (k=8, l=64) | 95.07   | 47.46       |
