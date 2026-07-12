# Migrating from v0.3 to v0.4

`v0.4.0-alpha.1` keeps the public `Engine`, `Session`, `QueryResult`, table
registration/refresh, cancellation, and Arrow streaming APIs compatible with
`v0.3.0-alpha.1`. Existing applications that use `EngineConfig::default()` or
its builder do not need a source migration. Arrow/Parquet `59.1.0`,
`object_store` `0.13.2`, and sqlparser `0.62.0` remain pinned.

## Parquet scan configuration

`EngineConfig` now contains a public `parquet_scan: ParquetScanConfig` field.
Both deep-pruning features default to `ParquetPruningMode::Auto`:

```rust
use rustdb::{EngineConfig, ParquetPruningMode};

let config = EngineConfig::builder()
    .parquet_page_index(ParquetPruningMode::Auto)
    .parquet_bloom_filter(ParquetPruningMode::Auto)
    .parquet_pruning_metadata_bytes(64 * 1024 * 1024)
    .build();
```

`Auto` may issue optional page-index or Bloom-filter range reads for a
supported constant predicate. It does not change SQL results: the residual
filter is retained, and absent or unsupported metadata falls back to ordinary
row-group pruning. Set one or both features to `Disabled` to preserve the
v0.3 request profile. The effective query pool is also capped at 64 MiB and
one sixteenth of the query memory limit; page-index metadata for one file is
capped at 16 MiB. A configured value of zero disables the optional metadata
budget without making the query fail.

The CLI exposes the same controls as `--parquet-page-index`,
`--parquet-bloom-filter`, and `--parquet-pruning-metadata`. Query metrics add
page-index/Bloom bytes, pruned pages/rows/row groups, and metadata-budget skips.
`EngineConfig` and the new Parquet configuration types are non-exhaustive, so
consumers should not exhaustively destructure them.

## SQL additions

- Ranking windows: `row_number`, `rank`, and `dense_rank`.
- Aggregate windows: `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` with `OVER`.
- `PARTITION BY`, window `ORDER BY`, named `WINDOW` clauses, and `QUALIFY`.
- `UNION ALL`, `UNION [DISTINCT]`, `INTERSECT [DISTINCT]`, and
  `EXCEPT [DISTINCT]`, including parenthesized recursive combinations.
- Equi `RIGHT` and `FULL` joins, cross-side residual ON predicates, and
  `JOIN ... USING` for Inner/Left/Right/Full joins.

Set inputs align by position to a lossless common type. DISTINCT set semantics
treat NULL values as equal. `USING` emits one visible key before the two sides'
non-key columns; for a full join that key is the coalesced left/right value.
Use an outer query `ORDER BY` when final row order matters.

## Intentional boundaries

The first window surface supports the SQL default frame and explicit `ROWS` or
`RANGE` from `UNBOUNDED PRECEDING` to `CURRENT ROW` or `UNBOUNDED FOLLOWING`.
Bounded offsets, `GROUPS`, `lead`/`lag`, value/navigation windows, window
DISTINCT/FILTER/ordered arguments, and named-window inheritance remain
unsupported. `INTERSECT ALL`, `EXCEPT ALL`, set `BY NAME`, `MINUS`, `NATURAL`
joins, and pure non-equi joins also remain unsupported.

v0.4 does not add Iceberg, JSON, ORC, compressed or byte-split CSV, native
tables, writes, transactions, a service protocol, or distributed execution.
