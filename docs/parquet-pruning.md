# Parquet deep pruning

RustDB can use Parquet page indexes and split-block Bloom filters before it
decodes data pages. Both optimizations are best-effort and preserve the normal
residual filter, so missing metadata or a pruning-budget rejection changes I/O
cost rather than query results.

## Configuration

`EngineConfig::parquet_scan` controls both features:

```rust
use rustdb::{EngineConfig, ParquetPruningMode};

let config = EngineConfig::builder()
    .parquet_page_index(ParquetPruningMode::Auto)
    .parquet_bloom_filter(ParquetPruningMode::Auto)
    .parquet_pruning_metadata_bytes(64 * 1024 * 1024)
    .build();
```

`Auto` uses metadata only for a supported constant predicate. `Disabled`
prevents the corresponding metadata reads. An unfiltered zero-column
`COUNT(*)` reads neither page indexes nor Bloom filters.

The effective per-query pruning pool is the minimum of the configured value,
64 MiB, and one sixteenth of the query memory limit. Page-index metadata for a
single file is capped at 16 MiB. If either budget is unavailable, RustDB skips
that optimization and continues with row-group pruning and the residual
filter.

## Supported pruning

Page indexes support comparisons and NULL predicates over Boolean, integer,
finite floating-point, Decimal128, UTF-8/Binary, Date, and Timestamp columns.
The column and offset indexes are combined into an Arrow `RowSelection`, which
is installed before the official Parquet reader fetches column data.

Bloom filters are checked after row-group min/max pruning and only for equality
predicates. The first version supports integer, UTF-8/Binary, Date, and
Timestamp values. A negative Bloom result can remove a row group; a positive
result always keeps the residual filter. Older files that advertise a Bloom
offset without the optional encoded length are retained without a Bloom read:
discovering their bitset size would otherwise bypass the pre-read metadata
reservation. This conservative fallback increments the budget-skip metric.

Footer-only and footer-plus-page-index metadata use distinct cache entries
keyed by URI, size, ETag, and version. Index and Bloom reads use the same
query-fixed object snapshot and conditional requests as data reads. Large
ranges are split into requests of at most 4 MiB. Metadata advertised by a file
but found to be malformed fails with the URI, row group, and column in the
error.

## Metrics

`QueryMetricsSnapshot` reports:

- `parquet_page_index_bytes_read`
- `parquet_bloom_filter_bytes_read`
- `parquet_pages_pruned`
- `parquet_page_rows_pruned`
- `parquet_bloom_row_groups_pruned`
- `parquet_pruning_budget_skips`
