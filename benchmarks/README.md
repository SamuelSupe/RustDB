# Benchmarks

Benchmark SQL should use `read_parquet(...)` or `read_csv(...)` so a run is
self-contained. The benchmark driver performs warmups, consumes every output
batch, and emits per-run plus p50/p95 JSON including memory, scan, S3, and spill
metrics. Each run also records first-batch latency, row throughput, and the
process RSS sampled after completion.

Run the repository-local smoke workload before using an external dataset:

```sh
docker compose run --rm dev cargo run --release --bin rustdb-bench -- \
  --query benchmarks/queries/smoke.sql --warmup 0 --iterations 1 \
  --memory-limit 67108864
```

```sh
docker compose run --rm dev cargo run --release --bin rustdb-bench -- \
  --query benchmarks/queries/scan_aggregate.sql \
  --warmup 2 --iterations 7 --memory-limit 1073741824 \
  > benchmarks/results/scan-aggregate.json
```

For CPU-specific local comparisons, build both RustDB and the fixed DuckDB
version with the same thread count and run RustDB with
`RUSTFLAGS="-C target-cpu=native"`. Record the input files, compression,
row-group size, hardware, cache state, and exact executable versions beside the
result. Do not compare a warm run from one engine with a cold run from another.

## DuckDB correctness checksum

`compare_duckdb.sh` renders a query template containing `__TPCH_ROOT__`, runs
RustDB in the pinned OrbStack development image and DuckDB v1.4.3, sorts the
CSV result rows, and compares SHA-256 checksums:

```sh
benchmarks/compare_duckdb.sh benchmarks/tpch/q06.sql data/tpch-sf1
```

Override the expected reference version only when deliberately updating the
baseline: `DUCKDB_VERSION=vX.Y.Z`. Keep the DuckDB version, dataset checksum,
Parquet compression/row-group settings, hardware, thread count, memory limit,
and cache state beside stored benchmark JSON. Use SF1 for daily correctness
and SF10 with a 64/128 MiB memory cap for spill and parallelism checks.
The dataset argument is workspace-relative so the script can render the host
path for DuckDB and the `/workspace/...` bind-mounted path for RustDB.
