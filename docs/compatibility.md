# Compatibility and limits

RustDB parses a documented DuckDB-flavored subset. It is not SQL- or
database-file-compatible with DuckDB.

## SQL support

| Area | v0.2 support |
| --- | --- |
| Query shape | `SELECT`, non-recursive CTEs, non-LATERAL derived tables |
| Filtering | `WHERE`, three-valued Boolean logic, comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE/UNKNOWN`, `LIKE`/`NOT LIKE` with `ESCAPE`, `IN` lists |
| Aggregation | `GROUP BY` expressions, aliases and ordinals; `HAVING` including projection aliases; `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` |
| Result shape | aliases, `DISTINCT` for non-aggregate projections, `ORDER BY`, `LIMIT`, `OFFSET` |
| Joins | equi `INNER` and `LEFT`; non-correlated `IN`/`EXISTS`/`NOT EXISTS` become `SEMI`/`ANTI` joins |
| Subqueries | non-correlated scalar, `IN`, and `EXISTS`; scalar requires one column and at most one row |
| Expressions | arithmetic, comparison, Boolean, `CASE`, `CAST`, exact Decimal128 arithmetic, date plus/minus DAY/MONTH/YEAR intervals |
| Session commands | `CREATE [OR REPLACE] TEMP VIEW`, `DROP VIEW [IF EXISTS]`, `REFRESH TABLE`, `SHOW TABLES`, `DESCRIBE`, `EXPLAIN`, `EXPLAIN ANALYZE` |
| File functions | `read_csv(...)`, `read_parquet(...)`, including inside CTEs and derived relations |

Non-DISTINCT queries may sort by a hidden input expression; DISTINCT queries
must sort by an output expression. `DISTINCT ON`, aggregate `DISTINCT`,
`NOT IN (subquery)`, recursive/correlated subqueries, LATERAL,
window functions, set operations, and RIGHT/FULL joins are rejected. General
string functions are not yet exposed; TPC-H string predicates used by the v0.1
acceptance set are covered by `LIKE`. Output order is unspecified unless the
query has an `ORDER BY`.

Parallel `SUM`/`AVG` over binary floating-point input can differ in the lowest
significant bits because partial states are merged in execution order. Exact
cross-thread checksums should use integer or Decimal input; the fixed v0.2
performance gate does so. Decimal aggregation remains exact and deterministic.

## Types and formats

Arrow `RecordBatch` is the execution boundary. Primitive Boolean, signed and
unsigned integer, floating-point, Decimal128 (precision up to 38), UTF-8,
Binary, Date, Timestamp, and supported Interval arrays pass through scans.
Decimal overflow, divide-by-zero, invalid casts, malformed input, and scalar
subquery cardinality errors fail the query. Nested Parquet arrays can be
projected to output but are not expression, group, sort, or join keys.

CSV is strict, UTF-8, and uncompressed. Schema inference uses a bounded sample;
an explicit Arrow schema is available through the Rust API. Quoted newlines
are supported. Files are decoded sequentially within one file and concurrently
across files. All matched files must have the same compatible schema between
explicit refreshes; an automatically discovered incompatible file fails with
its URI and column context. On `REFRESH TABLE`, surviving columns retain their
previous public order, removed columns disappear, and new columns are appended
in lexical name order. Projection and predicates continue to address the
logical order even when the refreshed CSV header has a different order.

Parquet supports projection, metadata-only unfiltered `COUNT(*)`, limit,
row-group min/max pruning, multi-file schema validation, `union_by_name`,
`schema_mode = 'strict|union|safe_widening'`, and Hive `key=value` partition
discovery and file pruning. Registered patterns are resolved at each query;
their visible schema changes only after `REFRESH TABLE`. Parquet metadata is
cached by URI plus object identity; data pages are never cached by RustDB.

## Deliberate exclusions

There are no native tables, writes, CTAS, DML, WAL, transactions, MVCC,
persistent Catalog, database file, server protocol, or distributed executor.
Compressed CSV, CSV byte-range splitting, Parquet writing, Bloom/page-index
optimization, and persistent data caching are outside v0.2.

S3 credentials come from the default credential chain or an application-owned
in-memory provider. Secrets are not accepted in SQL or endpoint URLs. See
[s3.md](s3.md) for consistency and endpoint details.
