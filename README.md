# go-sql-lsh

This is an experimental implementation of Locality Sensitive Hashing (LSH)
index using relational databases.
This library does not implement any specific locality-sensitive hash function
family,
but provides a generic storage backend for the hash values.
See [Documentation](https://godoc.org/github.com/ekzhu/go-sql-lsh)
for details.

To install:

```
go get github.com/ekzhu/go-sql-lsh
```

Currently only Sqlite and PostgreSQL are supported.

A performance comparison is shown in the table below.
Numbers are average query times, in millisecond. 
There are 10,000 signatures in the index for all runs.

| Signature Size  | Sqlite | PostgreSQL  |
|-----------------|--------|-------------|
| 128 (k=2, l=64) | 162.4  | 93.3        |
| 256 (k=4, l=64) | 171.5  | 117.6       |
| 512 (k=8, l=64) | 196.4  | 181.6       |
