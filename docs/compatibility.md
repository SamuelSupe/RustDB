# Compatibility and limits

RustDB parses a documented DuckDB-flavored subset. It is not SQL- or
database-file-compatible with DuckDB.

## SQL support

| Area | v0.8 alpha.1 development support |
| --- | --- |
| Query shape | `SELECT`, non-recursive CTEs, non-LATERAL derived tables, recursive parenthesized set-expression trees |
| Filtering | `WHERE`, three-valued Boolean logic, comparisons, `IS [NOT] NULL`, `IS [NOT] TRUE/FALSE/UNKNOWN`, `LIKE`/`NOT LIKE` with `ESCAPE`, `IN` lists |
| Aggregation | `GROUP BY` expressions, aliases and ordinals; `GROUPING SETS`, `ROLLUP`, `CUBE`, `GROUPING`/`GROUPING_ID`; `HAVING` aliases; `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`; aggregate `FILTER`; multi-expression `COUNT(DISTINCT ...)`; ordering clauses on the order-insensitive built-ins |
| Result shape | aliases, `DISTINCT` for non-aggregate projections, `ORDER BY`, `LIMIT`, `OFFSET` |
| Set operations | `UNION ALL`, `UNION [DISTINCT]`, `INTERSECT ALL`, `INTERSECT [DISTINCT]`, `EXCEPT ALL`, `EXCEPT [DISTINCT]`; positional lossless type alignment; result `ORDER BY` by output name or ordinal |
| Joins | equi `INNER`, `LEFT`, `RIGHT`, `FULL`, `LEFT SEMI`, and `LEFT ANTI`, optional cross-side residual predicates, and `USING`; subqueries also decorrelate to `MARK`, null-aware anti, and single-row joins |
| Windows | `row_number`, `rank`, `dense_rank`, `ntile`, `percent_rank`, `cume_dist`, `lead`, `lag`, `first_value`, `last_value`, and aggregate windows; `PARTITION BY`, `ORDER BY`, named `WINDOW`, `QUALIFY`, bounded `ROWS`, one-key numeric bounded `RANGE`, and `GROUPS` frames |
| Subqueries | non-correlated and one-level correlated scalar, `IN`, `NOT IN`, `EXISTS`, and `NOT EXISTS`; correlation requires an equality key; scalar requires one column and at most one row |
| Expressions | arithmetic, comparison, Boolean, `CASE`, strict `CAST`, exact Decimal128 arithmetic, `DATE`, `TIME(p)`, `TIMESTAMP(p)`, `TIMESTAMPTZ`, IANA `AT TIME ZONE`, UUID, and standard compound intervals through nanoseconds |
| Scalar functions | `substring`/`substr`, `length`/`char_length`, `lower`, `upper`, `trim`/`ltrim`/`rtrim`, `concat`, `replace`, `regexp_replace`/`regex_replace`, `starts_with`, `ends_with`, `contains`, `coalesce`, `nullif`, `abs`, `ceil`, `floor`, `round`, `extract`/`date_part`, `year`, `month`, `day`, `date_trunc`, `to_timestamp_seconds` |
| Session commands | transaction control, temp/persistent views, `CREATE`/`ALTER`/`DROP`/`TRUNCATE` Native tables, `INSERT`, `UPDATE FROM`, `DELETE USING`, DML `RETURNING`, `COPY`, `COMPACT`, `VACUUM`, `ANALYZE`, `CHECKPOINT`, `REFRESH`, `SHOW`, `DESCRIBE`, and `EXPLAIN [ANALYZE]` |
| File functions | `read_csv(...)`, `read_parquet(...)`, including inside CTEs and derived relations |
| Rust parameter API | `Session::prepare` with positional `?` or numbered `$n` placeholders and typed `ParameterValue`; query and `EXPLAIN` statements only |

Non-DISTINCT queries may sort by a hidden input expression; DISTINCT queries
must sort by an output expression. Set-operation `ORDER BY` is limited to an
output name or position. Set rows compare NULL values as equal for DISTINCT,
intersection, and difference. `INTERSECT ALL` and `EXCEPT ALL` preserve
duplicate multiplicity. Set operations `BY NAME` and `MINUS` are rejected.

Window functions are allowed in projection and `QUALIFY`, after aggregation
and `HAVING`; they are rejected in `WHERE`, `GROUP BY`, and `HAVING`. Matching
specifications share a sort. General frames are evaluated from a query-scoped,
memory-accounted file index; the common whole-partition, ROWS-prefix, and
RANGE-peer-prefix cases retain their streaming fast paths. Bounded RANGE
currently requires exactly one numeric ORDER BY expression and an integer
offset. Window DISTINCT, IGNORE/RESPECT NULLS, named-window inheritance,
nested windows, and non-integer frame offsets remain unsupported. `ntile`
requires a positive integer constant.
An `ORDER BY` inside `OVER` defines window semantics but does not promise final
row order; use the query's outer `ORDER BY` for that.

Parser-visible joins require at least one equality key. Additional ON terms
may reference either side and are evaluated as residual predicates. `USING`
produces one visible key, then the left and right non-key columns; qualified
key references remain available internally for name binding. Parser-visible
`LEFT SEMI` and `LEFT ANTI` return only left-side columns and preserve duplicate
left rows. `NATURAL`, pure non-equi, and `CROSS` joins remain unsupported.

Prepared values are substituted into a parsed AST and then use the normal
Binder, object snapshot, and optimizer path. They cannot replace identifiers,
file patterns, or table-function options. v0.8 parameter values include TIME,
arbitrary timestamp precision, TIMESTAMPTZ, UUID, and all three Arrow interval
families. SQL `PREPARE`/`EXECUTE`, server-side plan caching, and inferred
parameter types remain outside the embedded API.

Current aggregate functions are mathematically order-insensitive. Argument
`ORDER BY` and `WITHIN GROUP` expressions are bound and validated, then
normalized away; order-sensitive list/string/percentile aggregates are not yet
provided. `DISTINCT ON`, pure-inequality correlation, recursive correlation,
and LATERAL remain rejected. Output order is unspecified unless the query has
an outer `ORDER BY`.

Timezone-aware timestamps are normalized to an absolute UTC instant for
comparison and common-type resolution. `AT TIME ZONE` attaches an IANA zone to
a naive timestamp or returns local wall time from a zoned timestamp. A
nonexistent DST local time fails; an ambiguous fall-back time deterministically
chooses the later instant. TIME/TIMESTAMP precision 0 through 9 is retained in
the Arrow physical unit, and strict conversion checks range and malformed text.
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
Binary, Date, Time, Timestamp/TIMESTAMPTZ, UUID as FixedSizeBinary(16), and all
Arrow Interval families pass through scans and the Native store.
Decimal overflow, divide-by-zero, invalid casts, malformed input, and scalar
subquery cardinality errors fail the query. Nested Parquet arrays can be
projected to output but are not expression, group, sort, or join keys.

Because Arrow/Parquet 59 cannot write every interval family directly, Native
segments use a private little-endian fixed-width encoding and reconstruct the
logical Arrow interval arrays at scan time. This encoding is internal to the
Native format and is not exposed by `COPY TO PARQUET`.

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

`Engine::open(path, config)` persists immutable base/delta segments, versioned
delete vectors, schemas, tables, and views in a versioned Catalog. CTAS,
INSERT, UPDATE, DELETE, TRUNCATE, and transactional DDL publish atomically at a
Catalog generation boundary. `Engine::new` remains ephemeral. Local and
S3/MinIO backup publish a validated manifest last; restore refuses to replace
an existing destination. The CLI exposes `backup` and `restore` subcommands.

v0.8 database marker v2 adds a checksummed, contiguous-LSN WAL. Commit intent is
synced before Catalog `CURRENT` publication; open replays a durable unpublished
generation and rejects corrupt records or generation gaps. Database marker v1
remains readable but is write-protected until `rustdb migrate PATH` creates a
matching v0.7 backup and atomically enables WAL.

Transactions use optimistic multi-writer snapshot isolation. Read-only and
read-write handles, prepared statements, SQL transaction control, read-your-
writes, automatic rollback on drop, and active-result checks are supported.
Disjoint row changes rebase; concurrent writes to the same stable row or
Catalog object use first-committer-wins. Write skew is permitted by the stated
isolation level.

Transaction commit errors preserve their durable boundary. A known
post-publication failure leaves the handle committed and exposes `commit_info`;
an unreconciled WAL/Catalog publication leaves it indeterminate. Neither state
can be retried or rolled back on the original handle. Reopen recovery is the
required reconciliation boundary.

Persistent schema namespaces are catalog state. `main` is the default;
`CREATE SCHEMA`, `DROP SCHEMA`, `SHOW SCHEMAS`, and
`information_schema.schemata` are supported, and qualified names are accepted
by query, DML, DDL, COPY, maintenance, temporary-view, and
external-registration paths. Catalog manifest v1 is read as `main`; new
generations use manifest v2. Cancelling, abandoning, or failing a transaction
result after it staged a mutation rolls back the whole transaction because
statement savepoints are not implemented.

`NativeStorageConfig` exposes optional hard limits for the complete Native
database directory, a default per-table limit, and named table overrides. The
CLI exposes all three and accepts repeatable `table=size` overrides. Limits are
reopen-time policy rather than persisted format state. Quota rejection uses
`Error::NativeDiskQuotaExceeded` with the scope and current/new/peak/limit byte
counts, before durable publication.

Local COPY publishes by synced rename and remote COPY publishes its manifest
last. If output is durable but result delivery fails, RustDB returns the
structured `CopyPostCommitFailure` outcome; retrying the same destination is
not safe. A process crash can leave a UUID-named local staging file or a remote
child object before publication. RustDB never deletes those objects
automatically and refuses to append another attempt to the same destination;
inspect and remove the incomplete output before retrying.

Remote backup work is Engine-owned once its consistent local snapshot has been
created. Dropping the public future does not strand an in-process upload: the
worker finishes a valid manifest or aborts multipart uploads and removes
unmanifested keys. Abrupt process loss is outside in-process cleanup and should
be covered by the bucket's incomplete-multipart lifecycle policy. That policy
does not cover an object completed immediately before a manifest-less crash;
RustDB refuses to reuse a non-empty destination without a manifest so retries
cannot amplify the orphan. Operators must inspect and remove that dedicated
prefix before retrying. Local snapshot/download temporaries are independently
owned and locked; expired crash orphans are reclaimed conservatively at Engine
startup (and before remote restore), while unknown or active paths are kept.

Native table-manifest v3 retains the optional v2 `.rdbpred` companion and adds
stable physical row identity plus versioned, checksummed `.rdbdel` delete
vectors. Scans subtract deleted rows before exposing a batch and charge retained
bitmaps and filter workspace to query memory. Legacy v1/v2 manifests remain
readable and are not rewritten in place. Re-import or rewrite source data to
produce current-format snapshots.
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

Serializable isolation, predicate locks, savepoints, public time travel,
`MERGE`/upsert, relational constraints, secondary indexes, recursive CTEs,
LATERAL/UNNEST, nested LIST/STRUCT/MAP/JSON execution, replication, service
protocols, and distributed execution are excluded from v0.8. ORC, Iceberg,
JSON scan and persistent data-page caching are also outside this release.

S3 credentials come from the default credential chain or an application-owned
in-memory provider. Secrets are not accepted in SQL or endpoint URLs. See
[s3.md](s3.md) for consistency and endpoint details.
