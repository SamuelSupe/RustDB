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

## CSV schema or UTF-8 errors

CSV input is strict, UTF-8, and uncompressed in v0.2. Supply an explicit Arrow
schema when sampling would infer an unwanted type. All matched files must have
compatible columns; errors identify the URI and mismatched column. Use
`REFRESH TABLE name` only when the visible schema should be re-inferred.
Refresh keeps surviving columns in their previous order and appends new columns
by name. Quoted newlines are supported, but one large CSV file is decoded
sequentially; parallelism is across files.

## Reproduce a failure

Run the same command in the pinned OrbStack development image and enable Rust
logs without exposing credentials:

```sh
docker compose run --rm -e RUST_LOG=debug dev cargo run -- -c '<query>'
```
