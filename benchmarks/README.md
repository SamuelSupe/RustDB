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

The suite entrypoints build `rustdb-bench` separately with
`RUSTFLAGS="-C target-cpu=native"`; portable CI release builds remain generic.
Every suite manifest and report records the Cargo profile, Rust flags, and
compiler version. The runners reject a report unless its memory, thread,
batch, I/O, and metadata-cache configuration matches the requested matrix
entry. Do not compare a warm run from one engine with a cold run from another.

## DuckDB correctness checksum

`compare_duckdb.sh` is a compatibility wrapper around the pinned TPC-H
reference container. It renders a query template containing `__TPCH_ROOT__`,
runs RustDB and DuckDB 1.4.3, canonicalizes and sorts the result rows, and
compares SHA-256 checksums:

```sh
benchmarks/compare_duckdb.sh benchmarks/tpch/q06.sql data/tpch-sf1
```

The host does not need a DuckDB installation. Updating the reference version
requires changing the checked-in image asset checksums and acceptance docs.
Use SF1 for daily correctness and SF10 with a 64/128 MiB memory cap for Spill
and parallelism checks.

## Low-memory acceptance suite

The SF10 resource gate runs full Sort, high-cardinality Aggregate, Inner Join,
and Left Join workloads at both 64 MiB and 128 MiB:

```sh
benchmarks/run_low_memory.sh data/tpch-sf10
```

Every measured query is fully consumed. `rustdb-bench` fails the run when peak
engine reservation exceeds the configured limit, no Spill occurs, or the
query-scoped Spill directory remains. For each 64/128 MiB setting, the suite
first reruns the same query with the same thread, batch, and I/O settings,
requires non-zero Spill, and compares its complete result with DuckDB. The
limit is an engine reservation limit, not a
claim that allocator-retained process RSS equals 64/128 MiB; RSS after each
run is recorded separately. Join inputs use explicit derived projections so
the Spill state contains only required keys and payload columns. Only plumbing
diagnostics may opt out explicitly
with `SKIP_CHECKSUM=1`; the generated manifest then records
`"verified": false`. For an S3 dataset, supply the matching local reference:

```sh
REFERENCE_DATASET_ROOT=data/tpch-sf10 \
  benchmarks/run_low_memory.sh s3://rustdb-tests/tpch-sf10
```

Results are written below `benchmarks/results/low-memory/<timestamp>/` as a
suite `manifest.json`, rendered SQL, checksum records, and one benchmark JSON
per case and memory limit. `MEMORY_LIMITS_BYTES`, `THREADS`, `BATCH_SIZE`,
`IO_CONCURRENCY`, `WARMUP`, and `ITERATIONS` are available for deliberate
non-release experiments. Manifests include the RustDB build identifier,
benchmark executable SHA-256, and dataset-manifest digest; reports include the
same executable digest, host CPU model, and engine config.
An output directory must not already exist, preventing a failed rerun from
leaving an older successful manifest in place.

## Local and MinIO baseline matrix

Run the fixed-hardware local/MinIO matrix with matching datasets:

```sh
benchmarks/run_baseline.sh \
  --local-root data/tpch-sf1 \
  --minio-root s3://rustdb-tests/tpch-sf1 \
  --output benchmarks/results/baseline/my-machine
```

The default matrix covers one and four compute threads, 4096/8192-row batches,
and metadata-cold/warm runs. `THREADS_LIST`, `BATCH_SIZES`, and `CACHE_MODES`
override those dimensions. “Cold” deliberately means a new engine with a zero
metadata cache and no warmup; the runner does not claim to flush the OS page
cache, and records that fact in its manifest. MinIO data must already be
uploaded at `--minio-root`; the runner starts and initializes the repository's
MinIO service by default (`START_MINIO=0` disables that behavior). It compares
the remote and local dataset manifests. Before timing, every distinct
target/thread/batch configuration is executed independently and compared with
DuckDB; t1 and t4 entries therefore reference different checksum artifacts.
The manifest records SHA-256 digests of the runner, helper library, and
checksum runner used to produce the evidence. An explicitly supplied build ID
or CPU model is accepted only when it matches the current worktree or detected
host, so an environment override cannot silently relabel a run.

For the v0.3 M5 Max gate, capture the local SF10 candidate and compare it with
the clean `v0.2.0-alpha.1` SF10 manifest:

```sh
THREADS_LIST="1 4" BATCH_SIZES=8192 CACHE_MODES=warm \
MEMORY_LIMIT_BYTES=1073741824 WARMUP=2 ITERATIONS=5 START_MINIO=0 \
benchmarks/run_baseline.sh --local-root data/tpch-sf10 \
  --output benchmarks/results/baseline/<candidate-run>

python3 -B benchmarks/check_parallel_gate.py \
  --candidate benchmarks/results/baseline/<candidate-run>/manifest.json \
  --baseline benchmarks/results/baseline/<v02-sf10-run>/manifest.json
```

The checker reads only local, metadata-warm, batch-8192 reports for
`scan-filter` and `aggregate` at one and four threads. It requires M5 Max,
native release, 1 GiB, two warmups, five measured runs, matching build/data
fingerprints and checksums, at least 2.0x four-thread throughput, and no more
than 10% one-thread regression. The candidate manifest must name the exact
40-character commit of the current clean worktree, and every selected thread
configuration must have its own checksum path. Any missing or inconsistent
field fails. The host-side checker reads the CPU model from `sysctl`, rebuilds
the candidate native-release executable in OrbStack, compares its digest with
every candidate report, and re-hashes both repositories' harness and dataset
manifest files rather than trusting stored digest strings.

For the repository-generated TPC-H datasets, upload the matching scale first:

```sh
tools/tpch/upload_minio.sh 1
```

The uploader verifies the local dataset manifest and replaces the matching
prefix in the test-only `rustdb-tests` bucket. It reads both remote manifest
files back and compares them byte-for-byte. Benchmark manifests contain the S3
URI and run parameters, but never the fixed MinIO test credentials.

Before a long matrix, verify the runner and JSON output with the checked-in CSV
fixture:

```sh
THREADS_LIST=1 BATCH_SIZES=1024 CACHE_MODES=cold \
  benchmarks/run_baseline.sh --smoke \
  --output benchmarks/results/smoke-forward
```

Both runners invoke `tools/tpch/compare_query.sh` by default. Override
`CHECKSUM_RUNNER` only with another executable accepting three arguments: the
query template, a workspace-relative local DuckDB reference root, and the
actual RustDB root (workspace-relative or `s3://`). It must fail on a mismatch
and emit exactly one lowercase SHA-256 on standard output.
