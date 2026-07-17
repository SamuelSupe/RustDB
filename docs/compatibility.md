# Compatibility and limits

RustDB parses a documented DuckDB-flavored subset. It is not SQL- or
database-file-compatible with DuckDB.

## SQL support

| Area | v0.7 alpha development support |
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
| Scalar functions | `substring`/`substr`, `length`/`char_length`, `lower`, `upper`, `trim`/`ltrim`/`rtrim`, `concat`, `replace`, `regexp_replace`/`regex_replace`, `starts_with`, `ends_with`, `contains`, `coalesce`, `nullif`, `abs`, `ceil`, `floor`, `round`, `extract`/`date_part`, `year`, `month`, `day`, `date_trunc`, `to_timestamp_seconds` |
| Session commands | `CREATE [OR REPLACE] TEMP VIEW`, `DROP VIEW [IF EXISTS]`, `REFRESH TABLE`, `SHOW TABLES`, `DESCRIBE`, `EXPLAIN`, `EXPLAIN ANALYZE`; a persistent engine also accepts `CREATE [OR REPLACE] TABLE name AS SELECT` and `INSERT INTO name SELECT` |
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
v0.6 alpha. Set operations `BY NAME`/`MINUS`, bounded or `GROUPS` window frames,
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
Explicit integer-to-`DATE` casts interpret the input as epoch days, and
`to_timestamp_seconds(integer)` returns a timezone-free microsecond Timestamp
with checked overflow. A UTF-8 literal compared directly with a Date or
timezone-free Timestamp is strictly cast to that temporal type; arbitrary
UTF-8 columns are never converted implicitly.

Decimal addition, subtraction, multiplication, and modulo remain exact.
Following DuckDB, `/` returns Float64 even when both inputs are Decimal, and a
mixed Decimal/Float arithmetic, comparison, or CASE expression promotes to
Float64.

Parallel `SUM`/`AVG` over binary floating-point input can differ in the lowest
significant bits because partial states are merged in execution order. Exact
cross-thread checksums should use integer or Decimal input. Decimal aggregation
remains exact and deterministic.

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
Fixed multi-file providers may prefetch a query-local, lane-bounded metadata
prefix when no LIMIT is present. This changes planning overlap only: ordinary
registered external tables remain lazy, residual predicates are unchanged,
and memory pressure falls back to lazy planning. Fixed providers, including
Native snapshots, may also combine two through four row groups in one reader
without reducing the planned lane count. Dynamic registered Parquet and every
LIMIT scan retain one row group per reader. Per-file metadata and immutable
decode configuration are shared within the query, but mutable Arrow filter,
builder, and stream state remain reader-local. Strict exact-schema scans with
no Hive or dictionary columns can reuse their existing arrays during alignment.
See
[parquet-pruning.md](parquet-pruning.md) for supported predicates and budgets.

On Linux and macOS, local snapshots and Parquet metadata-cache keys additionally
pin device, inode, size, mtime, and ctime. Local Parquet readers for one
query/file share a descriptor, use positional reads, and validate both that
descriptor and the current path before and after each range. Same-size in-place
changes with restored mtime, truncation, deletion, and atomic replacement all
fail as object changes. Local CSV validates its opened descriptor before
reading and again before returning EOF, but an atomic replacement may complete
through the consistent old CSV descriptor. S3 behavior is unchanged and
continues to use size, ETag, and version when available.

`QueryMetricsSnapshot` exposes `parquet_reader_builds`,
`parquet_local_file_opens`, `parquet_narrow_decimal_columns`, coalesced range
bytes/time, decoder active time and polls, decoder compute-permit wait,
RowFilter compute/evaluations/input rows, and schema-alignment time. Narrow
Decimal decoding is an internal, exact Decimal64-to-Decimal128 widening for
eligible precision-at-most-18 Parquet predicate columns; unsupported encodings
fall back transparently. RowFilter compute is a subset of decoder active time;
both are cumulative across lanes and may exceed query wall time. Coalesced range
bytes describe requested positional-read spans, not physical media traffic.
These values are additive diagnostics, not performance guarantees.

## Persistent Native alpha

`Engine::open(path, config)` persists immutable Native segments and a versioned
Catalog. Bulk CTAS, append-from-query, and whole-table replacement are atomic
at the Catalog generation boundary. Queries pin immutable snapshots, and local
backup/restore copies and validates one reachable generation without replacing
an existing destination. A post-rename durability failure is reported as
`CommitOutcomeUnknown`. `Engine::new` continues to provide the v0.5-style
ephemeral engine. Native table names are limited to 255 UTF-8 bytes; Catalog
generation encoding is checked against its per-table disk reservation before a
write starts.

Native table-manifest v2 may bind an optional `.rdbpred` companion to each
Parquet segment. Legacy v1 manifests and v2 segments without a companion remain
readable and use the ordinary Parquet predicate path; they are not upgraded in
place. Re-import or rewrite the source data to generate eligible sidecars.
Sidecars are query-neutral. The file format can index `Int8/16/32/64`,
`Date32`, and `Decimal128(precision <= 18)`; the first query slice accelerates
complete exact AND trees made from direct `=`, `<>`, `<`, `<=`, `>`, `>=`,
and `IS [NOT] NULL` predicates over `Int64`, `Date32`, or
`Decimal128(precision <= 18)`. Narrow integers, `OR`, `IN`, `LIKE`, functions,
unsupported types, and row groups without every required block fall back to
Parquet.

Sidecar metadata and only the required blocks are range-read; the query does
not load the whole companion. Their buffers and exact selections are charged
to query memory. Projected scans use a companion only when its required blocks
are at most half the compressed Parquet predicate-only bytes they replace;
zero-column metadata scans admit up to a 2x ratio. A cost or memory-admission
miss falls back to Parquet, and import may
omit the optional file when its reserved 64 MiB construction ceiling or the
Native disk budget cannot admit it. Sidecar bytes count toward the existing
final 2x source-size plus metadata allowance. Once a manifest declares a
sidecar, however, a missing file, size/checksum/format or segment-binding
mismatch, and an object identity change are corruption/consistency errors, not
fallbacks.

The Native directory is private engine state. RustDB validates segment identity
and checksums when opening; scans cache that full verification only while the
immutable segment's device, inode, length, mtime, and ctime remain unchanged.
The just-verified fingerprint is reused only in process memory, so first query
use does not repeat the open/commit hash. Other targets conservatively repeat
the full check. The process lock does not exclude arbitrary external filesystem
mutation. Applications must not replace or edit files in an open Native
database.

Integer `SUM` returns `Decimal128(38, 0)` and Decimal input returns precision 38
at its input scale. This avoids narrowing a wide partial accumulator at the
public Arrow boundary.

## Deliberate exclusions

There are no row-level updates/deletes, schema alteration, WAL transactions,
row MVCC, replication, server protocol, or distributed executor. Remote Native
backup is not yet included in alpha.1. Unsafe CSV byte-range splitting,
Parquet export, persistent data-page caching, JSON, ORC, Iceberg, and service
protocols remain outside v0.6 alpha.

S3 credentials come from the default credential chain or an application-owned
in-memory provider. Secrets are not accepted in SQL or endpoint URLs. See
[s3.md](s3.md) for consistency and endpoint details.
