# Compatibility and limits

RustDB parses a documented DuckDB-flavored subset. It is not SQL- or
database-file-compatible with DuckDB.

## SQL support

| Area | v0.3 support |
| --- | --- |
| Query shape | `SELECT`, non-recursive CTEs, non-LATERAL derived tables |
| Filtering | `WHERE`, three-valued Boolean logic, comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE/UNKNOWN`, `LIKE`/`NOT LIKE` with `ESCAPE`, `IN` lists |
| Aggregation | `GROUP BY` expressions, aliases and ordinals; `HAVING` including projection aliases; `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`; single-expression `COUNT/SUM/AVG(DISTINCT ...)` (`MIN/MAX DISTINCT` normalize to ordinary aggregates) |
| Result shape | aliases, `DISTINCT` for non-aggregate projections, `ORDER BY`, `LIMIT`, `OFFSET` |
| Joins | equi `INNER` and `LEFT`; subqueries decorrelate to internal `SEMI`, `ANTI`, `MARK`, null-aware anti, and single-row joins |
| Subqueries | non-correlated and one-level correlated scalar, `IN`, `NOT IN`, `EXISTS`, and `NOT EXISTS`; correlation requires an equality key; scalar requires one column and at most one row |
| Expressions | arithmetic, comparison, Boolean, `CASE`, strict `CAST`, exact Decimal128 arithmetic, typed `TIMESTAMP`, and Date/Timestamp plus/minus DAY/MONTH/YEAR intervals |
| Scalar functions | `substring`/`substr`, `length`/`char_length`, `lower`, `upper`, `trim`/`ltrim`/`rtrim`, `concat`, `replace`, `starts_with`, `ends_with`, `contains`, `coalesce`, `nullif`, `abs`, `ceil`, `floor`, `round`, `extract`/`date_part`, `year`, `month`, `day`, `date_trunc` |
| Session commands | `CREATE [OR REPLACE] TEMP VIEW`, `DROP VIEW [IF EXISTS]`, `REFRESH TABLE`, `SHOW TABLES`, `DESCRIBE`, `EXPLAIN`, `EXPLAIN ANALYZE` |
| File functions | `read_csv(...)`, `read_parquet(...)`, including inside CTEs and derived relations |

Non-DISTINCT queries may sort by a hidden input expression; DISTINCT queries
must sort by an output expression. `DISTINCT ON`, multi-argument DISTINCT,
ordered aggregates, aggregate `FILTER`, pure-inequality correlation, recursive
correlation, LATERAL, window functions, set operations, and RIGHT/FULL joins
are rejected. Output order is unspecified unless the query has an `ORDER BY`.
Timezone-aware Arrow timestamps may be compared, combined in `CASE`/`COALESCE`,
and used with supported intervals only when their timezone metadata matches.
Strict casts and `date_trunc` on timezone-aware timestamps remain rejected;
`AT TIME ZONE` and implicit timezone conversion are not performed.
Timezone-free `TIMESTAMP(p)` typed literals accept `p <= 6` and round to the
declared precision before entering the microsecond engine type. Precision-
qualified cast targets below six digits are rejected instead of silently
discarding `p`; use `TIMESTAMP` or `TIMESTAMP(6)` for strict casts.

Decimal addition, subtraction, multiplication, and modulo remain exact.
Following DuckDB, `/` returns Float64 even when both inputs are Decimal, and a
mixed Decimal/Float arithmetic, comparison, or CASE expression promotes to
Float64.

Parallel `SUM`/`AVG` over binary floating-point input can differ in the lowest
significant bits because partial states are merged in execution order. Exact
cross-thread checksums should use integer or Decimal input; the fixed v0.3
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
optimization, and persistent data caching are outside v0.3.

S3 credentials come from the default credential chain or an application-owned
in-memory provider. Secrets are not accepted in SQL or endpoint URLs. See
[s3.md](s3.md) for consistency and endpoint details.
