# Compatibility and limits

RustDB parses a documented DuckDB-flavored subset. It is not SQL- or
database-file-compatible with DuckDB.

## SQL support

| Area | v0.5 support |
| --- | --- |
| Query shape | `SELECT`, non-recursive CTEs, non-LATERAL derived tables, recursive parenthesized set-expression trees |
| Filtering | `WHERE`, three-valued Boolean logic, comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE/UNKNOWN`, `LIKE`/`NOT LIKE` with `ESCAPE`, `IN` lists |
| Aggregation | `GROUP BY` expressions, aliases and ordinals; `HAVING` including projection aliases; `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`; single-expression `COUNT/SUM/AVG(DISTINCT ...)` (`MIN/MAX DISTINCT` normalize to ordinary aggregates) |
| Result shape | aliases, `DISTINCT` for non-aggregate projections, `ORDER BY`, `LIMIT`, `OFFSET` |
| Set operations | `UNION ALL`, `UNION [DISTINCT]`, `INTERSECT ALL`, `INTERSECT [DISTINCT]`, `EXCEPT ALL`, `EXCEPT [DISTINCT]`; positional lossless type alignment; result `ORDER BY` by output name or ordinal |
| Joins | equi `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFT SEMI`, and `LEFT ANTI`, optional cross-side residual predicates, and `USING`; subqueries also decorrelate to `MARK`, null-aware anti, and single-row joins |
| Windows | `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist`, and `COUNT/SUM/AVG/MIN/MAX OVER`; `PARTITION BY`, `ORDER BY`, named `WINDOW`, and `QUALIFY`; default frames plus `ROWS`/`RANGE` from `UNBOUNDED PRECEDING` to `CURRENT ROW` or `UNBOUNDED FOLLOWING` |
| Subqueries | non-correlated and one-level correlated scalar, `IN`, `NOT IN`, `EXISTS`, and `NOT EXISTS`; correlation requires an equality key; scalar requires one column and at most one row |
| Expressions | arithmetic, comparison, Boolean, `CASE`, strict `CAST`, exact Decimal128 arithmetic, typed `TIMESTAMP`, and Date/Timestamp plus/minus DAY/MONTH/YEAR intervals |
| Scalar functions | `substring`/`substr`, `length`/`char_length`, `lower`, `upper`, `trim`/`ltrim`/`rtrim`, `concat`, `replace`, `starts_with`, `ends_with`, `contains`, `coalesce`, `nullif`, `abs`, `ceil`, `floor`, `round`, `extract`/`date_part`, `year`, `month`, `day`, `date_trunc` |
| Session commands | `CREATE [OR REPLACE] TEMP VIEW`, `DROP VIEW [IF EXISTS]`, `REFRESH TABLE`, `SHOW TABLES`, `DESCRIBE`, `EXPLAIN`, `EXPLAIN ANALYZE` |
| File functions | `read_csv(...)`, `read_parquet(...)`, including inside CTEs and derived relations |
| Rust parameter API | `Session::prepare` with positional `?` or numbered `$n` placeholders and typed `ParameterValue`; query and `EXPLAIN` statements only |

Non-DISTINCT queries may sort by a hidden input expression; DISTINCT queries
must sort by an output expression. Set-operation `ORDER BY` is limited to an
output name or position. Set rows compare NULL values as equal for DISTINCT,
intersection, and difference. `INTERSECT ALL` and `EXCEPT ALL` preserve
duplicate multiplicity. Set operations `BY NAME` and `MINUS` are rejected.

Window functions are allowed in projection and `QUALIFY`, after aggregation
and `HAVING`; they are rejected in `WHERE`, `GROUP BY`, and `HAVING`. Ranking
and aggregate windows share a sort when their specifications match. Bounded
offset frames, `GROUPS`, window `DISTINCT`/`FILTER`/ordered arguments/NULL
treatment, window inheritance or overrides, nested windows, and functions such
as `lead`, `lag`, `first_value`, and `last_value` are not supported. `ntile`
currently requires a positive integer constant.
An `ORDER BY` inside `OVER` defines window semantics but does not promise final
row order; use the query's outer `ORDER BY` for that.

Parser-visible joins require at least one equality key. Additional ON terms
may reference either side and are evaluated as residual predicates. `USING`
produces one visible key, then the left and right non-key columns; qualified
key references remain available internally for name binding. Parser-visible
`LEFT SEMI` and `LEFT ANTI` return only left-side columns and preserve duplicate
left rows. `NATURAL`, pure non-equi, and `CROSS` joins remain unsupported.

Prepared values are substituted into a parsed statement and then use the same
Binder and execution path as literal SQL. Parameters cannot replace identifiers
or `read_csv`/`read_parquet` arguments. SQL `PREPARE`/`EXECUTE`, server-side
prepared statements, and automatic parameter type inference remain outside
v0.5. Set operations `BY NAME`/`MINUS`, bounded or `GROUPS` window frames,
`lead`/`lag`/value windows, and `NATURAL`/pure non-equi/`CROSS` joins are the
explicitly delayed SQL capabilities.

`DISTINCT ON`, multi-argument aggregate DISTINCT, ordered aggregates,
aggregate `FILTER`, pure-inequality correlation, recursive correlation, and
LATERAL are rejected. Output order is unspecified unless the query has an
outer `ORDER BY`.
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
cross-thread checksums should use integer or Decimal input; the fixed v0.5
performance gate does so. Decimal aggregation remains exact and deterministic.

## Types and formats

Arrow `RecordBatch` is the execution boundary. Primitive Boolean, signed and
unsigned integer, floating-point, Decimal128 (precision up to 38), UTF-8,
Binary, Date, Timestamp, and supported Interval arrays pass through scans.
Decimal overflow, divide-by-zero, invalid casts, malformed input, and scalar
subquery cardinality errors fail the query. Nested Parquet arrays can be
projected to output but are not expression, group, sort, or join keys.

CSV is strict UTF-8 and supports raw, gzip, and zstd input. `Auto` compression
uses magic bytes rather than the filename and accepts concatenated gzip members
and zstd frames. Schema inference uses a bounded sample through the same
decompression path; an explicit Arrow schema is available through the Rust API.
One ordered source/decompress/framing producer preserves quoted newlines and
UTF-8 boundaries, then record-aligned morsels are decoded by bounded parallel
lanes. S3 objects are streamed once rather than split with unsafe random range
reads. All matched files must have the same compatible schema between explicit
refreshes; an automatically discovered incompatible file fails with its URI
and column context. On `REFRESH TABLE`, surviving columns retain their previous
public order, removed columns disappear, and new columns are appended in lexical
name order. Projection and predicates continue to address the logical order
even when the refreshed CSV header has a different order.

Parquet supports projection, metadata-only unfiltered `COUNT(*)`, limit,
row-group min/max pruning, same-column constant `IN`/`OR`, budgeted page-index
`RowSelection`, split-block Bloom-filter equality pruning, query runtime filters,
multi-file schema validation, `union_by_name`,
`schema_mode = 'strict|union|safe_widening'`, and Hive `key=value` partition
discovery and file pruning. Registered patterns are resolved at each query;
their visible schema changes only after `REFRESH TABLE`. Deep pruning is
configured through `EngineConfig::parquet_scan`; the `Auto` and `Disabled`
modes never replace the residual SQL filter. Footer and deep metadata are
singleflight-cached by URI plus object identity; data pages are never cached.
See
[parquet-pruning.md](parquet-pruning.md) for supported predicates and budgets.

## Deliberate exclusions

There are no native tables, writes, CTAS, DML, WAL, transactions, MVCC,
persistent Catalog, database file, server protocol, or distributed executor.
Unsafe CSV byte-range splitting, Parquet writing, persistent data caching, JSON,
ORC, Iceberg, Native Tables, and service protocols are outside v0.5.

S3 credentials come from the default credential chain or an application-owned
in-memory provider. Secrets are not accepted in SQL or endpoint URLs. See
[s3.md](s3.md) for consistency and endpoint details.
