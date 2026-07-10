# Troubleshooting

## A query reports `resource exhausted`

Increase `EngineConfig::memory_limit` or CLI `--memory-limit`, or reduce
`batch_size` for very wide rows. Sort and Aggregate spill automatically;
Hash Join first uses Grace partitioning. A single row or group that cannot fit
the minimum operator state returns the required and available memory instead
of retrying forever.

Confirm the configured temporary directory exists on a filesystem with enough
free space and supports owner-only permissions. Spill files are LZ4-compressed
Arrow IPC, mode `0600`, and are removed after success, error, or cancellation.

## S3 requests fail

- Verify the bucket is part of the `s3://` URI and that the region is correct.
- For MinIO, set the endpoint, path-style mode, and explicit HTTP opt-in.
- Do not place credentials in the endpoint or SQL. Use the default environment
  chain or an in-memory credential provider.
- An "object changed during query" error is intentional consistency
  protection; retry after the writer has finished publishing the object.

## CSV schema or UTF-8 errors

CSV input is strict, UTF-8, and uncompressed in v0.1. Supply an explicit Arrow
schema when sampling would infer an unwanted type. All matched files must have
compatible columns. Quoted newlines are supported, but one large CSV file is
decoded sequentially; parallelism is across files.

## Reproduce a failure

Run the same command in the pinned OrbStack development image and enable Rust
logs without exposing credentials:

```sh
docker compose run --rm -e RUST_LOG=debug dev cargo run -- -c '<query>'
```
