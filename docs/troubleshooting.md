# Troubleshooting

## A query reports `resource exhausted`

Increase `EngineConfig::memory_limit` or CLI `--memory-limit`, or reduce
`batch_size` for very wide rows. Sort and Aggregate spill automatically;
Hash Join first uses Grace partitioning. A single row or group that cannot fit
the minimum operator state returns the required and available memory instead
of retrying forever.

The same budget also covers resolved file metadata, query snapshots, active
Parquet footers, Spill file paths, and buffered Spill writers. Very broad
globs, unusually large Parquet footers/schemas, or a limit too small to open
the minimum partition writers fail explicitly. Narrow the pattern or raise the
limit; lowering `batch_size` only helps row-sized working state.

`compute_threads` is automatically capped by the query memory budget at one
lane per 32 MiB. This preserves forward progress for nested bounded queues;
manually requesting more threads does not override the cap. Raise the memory
limit if a low-memory query needs more parallel lanes.

Confirm `EngineConfig::spill.directory` exists on a filesystem with enough
free space and supports owner-only permissions. Spill rejects writes that
would violate its engine/query quota or leave less than both the configured
ratio and byte reserve. Files are LZ4-compressed Arrow IPC, mode `0600`, and
are removed after success, error, cancellation, or consumer abandonment.

Do not manually remove a live query directory. Its `.rustdb-active` advisory
lock is held for the query Spill manager's lifetime, and startup
scavenging skips the directory while that lock is held even when the marker is
older than the orphan TTL. Cleanup only targets UUID-named query directories
with an old, valid `.rustdb-spill` marker; unrelated and invalidly marked
directories are preserved. A deletion or malformed activity-lock error is
reported instead of being silently ignored.

## S3 requests fail

- Verify the bucket is part of the `s3://` URI and that the region is correct.
- For MinIO, set the endpoint, path-style mode, and explicit HTTP opt-in.
- Do not place credentials in the endpoint or SQL. Use the default environment
  chain or an in-memory credential provider.
- An "object changed during query" error is intentional consistency
  protection; retry after the writer has finished publishing the object.

## CSV input, schema, or memory errors

CSV remains strict UTF-8. Supply an explicit Arrow schema when sampling would
infer an unwanted type. All matched files must have compatible columns; errors
identify the URI and mismatched column. Use `REFRESH TABLE name` only when the
visible schema should be re-inferred. Refresh keeps surviving columns in their
previous order and appends new columns by name.

`CsvCompression::Auto` and `read_csv(..., compression='auto')` inspect magic
bytes rather than the filename. Use an explicit `none`, `gzip`, or `zstd` value
when a producer or gateway changes the initial bytes. Truncated members/frames,
bad checksums, or invalid compressed data are reported against the object URI.
Concatenated gzip members and multi-frame zstd are supported.

A single object is fetched and decompressed in order, then split only after a
complete CSV record; quoted newlines are safe. The default 8 MiB target is not
a maximum record size. A larger record remains one morsel and must fit the
query memory budget. Raise the memory limit, reduce simultaneous lanes, or
disable single-file parallel parsing when the source has unusually wide
records.

When that allocation fails, the error includes the object URI and decompressed
offset of the buffered record, followed by the original query-limit and
available-memory context.

Configure a smaller morsel target or disable this parallel path with the public
builders:

```rust,no_run
use rustdb::{CsvCompression, CsvOptions, EngineConfig};

let config = EngineConfig::builder()
    .csv_target_morsel_bytes(4 * 1024 * 1024)
    .csv_parallel_single_file(false)
    .build();
let options = CsvOptions::builder()
    .compression(CsvCompression::Auto)
    .build();
```

For the CLI, use `--csv-target-morsel-bytes` or
`--no-csv-parallel-single-file`. `--metrics` reports `csv_source_bytes`,
`csv_decompressed_bytes`, `csv_morsels`, and `csv_parser_lanes`; a large
decompressed/source ratio is expected for highly compressible input. See the
[v0.5 migration guide](migration-v0.5.md) when replacing `CsvOptions` struct
literals with the builder.

## Adaptive execution settings

`EngineConfig::execution` and its builder methods control the adaptive Spill
partition target, maximum repartition depth, optional write-amplification
limit, and Join runtime-filter budget. The corresponding CLI flags are
`--spill-partition-target-bytes`, `--max-repartition-depth`,
`--max-spill-write-amplification`, and `--runtime-filter-bytes`. A value of zero
for the runtime-filter budget disables runtime filters; runtime filtering is
only a pruning hint and does not replace residual SQL predicates.

When diagnosing resource pressure, inspect active/peak Spill bytes and files,
repartition bytes/depth, maximum partition size, quota rejections, runtime
filter hits, metadata cache hits/misses, and singleflight wait alongside the
CSV counters. These metrics distinguish source decompression pressure from a
skewed blocking operator or concurrent metadata load.
Singleflight followers count as cache misses with wait time; only a persistent
LRU lookup counts as a hit.

## Reproduce a failure

Run the same command in the pinned OrbStack development image and enable Rust
logs without exposing credentials:

```sh
docker compose run --rm -e RUST_LOG=debug dev cargo run -- -c '<query>'
```
