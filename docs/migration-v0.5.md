# Migrating from v0.4 to v0.5

`v0.5.0-alpha.1` keeps RustDB read-only and embedded. The public `Engine`,
`Session`, `QueryResult`, table registration/refresh, cancellation, and Arrow
streaming result shapes remain available. Arrow/Parquet `59.1.0`, object-store
`0.13.2`, and sqlparser `0.62.0` remain pinned.

## CSV options

`CsvOptions` is now non-exhaustive and includes `compression`. Applications
that previously constructed it with a struct literal must use its builder or
start from `CsvOptions::default()`:

```rust
use rustdb::{CsvCompression, CsvHeader, CsvOptions};

let options = CsvOptions::builder()
    .header(CsvHeader::Present)
    .compression(CsvCompression::Auto)
    .build();
```

`Auto` inspects the object bytes and supports raw, gzip, and zstd CSV. The
engine reads each object once and creates record-aligned morsels from the
ordered decompressed stream; it does not issue speculative S3 range reads to
split a quoted CSV record.

`EngineConfig::csv_scan` controls single-file parallel parsing. The default
enables it with 8 MiB target morsels. `EngineConfig::execution` contains the
new adaptive Spill and runtime-filter budgets. Both configurations are
non-exhaustive and have builder methods.

## Parameterized queries

Use `Session::prepare` for application-owned parameters. A prepared statement
caches parsing only: every execution binds the current session catalog,
discovers external files, freezes a fresh object snapshot, and optimizes again.
Parameters are expression values and cannot replace identifiers, locations, or
table-function options.

## SQL additions

- `INTERSECT ALL` and `EXCEPT ALL`, with streaming multiplicity output.
- Parser-visible `LEFT SEMI JOIN` and `LEFT ANTI JOIN`.
- `ntile`, `percent_rank`, and `cume_dist` window functions.

The default output order remains unspecified without an outer `ORDER BY`.
`CROSS`, Right Semi/Anti, bounded window frames, `GROUPING SETS`, and SQL
`PREPARE` commands remain unsupported.

## Metrics

`QueryMetricsSnapshot` is non-exhaustive in v0.5. Prefer reading named fields
instead of exhaustive destructuring. New counters distinguish active and
cumulative Spill, repartitioning, Join short-circuit/runtime-filter activity,
CSV source and decompressed bytes, parser morsels, and metadata-cache activity.

## Unchanged boundaries

v0.5 does not add Iceberg, JSON, ORC, native tables, writes, transactions,
persistent catalogs, a server protocol, or distributed execution.
