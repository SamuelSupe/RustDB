# Benchmarks

## ClickBench functionality pass

The active v0.7 acceptance workload is one complete 43-query ClickBench pass
on the official 1M Parquet partition with a 4-CPU/16-GiB OrbStack container. It
does not compare RustDB with another engine and does not repeat measurements. See
[`clickbench/README.md`](clickbench/README.md) for the resource contract and
runner.

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

## Legacy differential tooling

The comparison-oriented runners below are retained for historical regression
investigation, but they are not part of the active v0.7 release target.

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

The SF10 resource gate runs full Sort, high-cardinality Aggregate,
Inner/Left/Right/Full/Semi/Anti Join, DISTINCT and ALL set operations, and
Window workloads at both 64 MiB and 128 MiB:

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

The Semi and Anti cases deliberately keep parser-visible `LEFT SEMI JOIN` and
`LEFT ANTI JOIN` in the RustDB templates. `tools/tpch/compare_query.sh` detects
the matching `semi_join.duckdb.sql` and `anti_join.duckdb.sql` companions and
uses their equivalent `EXISTS`/`NOT EXISTS` forms only for DuckDB 1.4.3. RustDB
still executes the original Join syntax, and the canonical checksums must
match.

## v0.5 SF10 resource evidence

Collect the fixed-hardware resource reports only after creating a clean
candidate commit. Q17, Q21, and all selected Join reports must name that exact
40-character commit and the same native-release `rustdb-bench` SHA-256. First
run the complete low-memory suite from that commit, then use its run directory:

```sh
LOW='benchmarks/results/low-memory/<candidate-low-memory-run>'
benchmarks/run_v05_resource_gate.sh \
  --dataset-root data/tpch-sf10 \
  --low-memory-run "$LOW" \
  --output benchmarks/results/v05/<candidate-resource-run>
```

The runner records candidate Q17 and Q21, runs both queries through
`tools/tpch/compare_query.sh` at the same 128 MiB / four-lane settings, builds
and records Q21 from a detached local `v0.4.0-alpha.1` worktree, writes
enclosing dataset provenance, and invokes the strict checker. It fixes batch
8192, I/O concurrency 32, and metadata cache 0. The output path must not exist.
Its temporary worktree has an isolated target directory and is removed on
success, failure, or interruption; it never creates a tag or pushes.
Candidate Spill cleanup is checked immediately after Q17/Q21 and again after
the long baseline run, so delayed shared-mount residue remains a hard failure.
The detached v0.4 process uses a separate Spill root; legacy ghost directories
are counted in the manifest and removed only after that process exits.
Store the candidate reports and rendered SQL below the generated run directory:

```text
benchmarks/results/v05/<candidate-resource-run>/
  manifest.json
  q17.json
  q17.checksum.txt
  q21.json
  q21.checksum.txt
  baseline/q21.json
  baseline/q21.sql
  rendered/q17.sql
  rendered/q21.sql
```

The enclosing `manifest.json` must contain the same complete `dataset` object
as the clean candidate low-memory manifest, including SF10 generation metadata,
the workspace-relative `data/tpch-sf10/manifest.sha256` path, and that file's
SHA-256. The gate resolves this enclosing object for Q17/Q21 and resolves the
low-memory suite manifest for Join reports. Keep every rendered SQL file named
by `query_file`; the checker compares its full contents with the canonical
template and verifies that all candidate queries use one dataset root.

The checker consumes the complete low-memory manifest, not only six loose JSON
files. It requires the exact 12-case by 64/128 MiB matrix (24 unique entries),
verified correctness and cleanup assertions, one existing report and one
single-line checksum per entry, and one candidate build/binary/config across
all reports. Every constrained report must stay within its memory limit, end
with zero retained resources, and show non-zero Spill read and write bytes.
The six `--join` arguments must be exactly the 128 MiB Join entries named by
that manifest. To replay only the final check without rerunning measurements:

```sh
V05='benchmarks/results/v05/<candidate-resource-run>'
LOW_RUN='benchmarks/results/low-memory/<candidate-low-memory-run>'
LOW="$LOW_RUN/reports"
python3 -B benchmarks/check_v05_resource_gate.py \
  --q17 "$V05/q17.json" \
  --q17-checksum "$V05/q17.checksum.txt" \
  --q21-candidate "$V05/q21.json" \
  --q21-checksum "$V05/q21.checksum.txt" \
  --q21-baseline "$V05/baseline/q21.json" \
  --low-memory-manifest "$LOW_RUN/manifest.json" \
  --join "$LOW/inner-join-134217728.json" \
  --join "$LOW/left-join-134217728.json" \
  --join "$LOW/right-join-134217728.json" \
  --join "$LOW/full-join-134217728.json" \
  --join "$LOW/semi-join-134217728.json" \
  --join "$LOW/anti-join-134217728.json"
```

The checker requires an Apple M5 Max and rejects a dirty worktree. It resolves
the local `v0.4.0-alpha.1` tag, rebuilds the clean candidate with
`-C target-cpu=native`, hashes the authoritative SF10 manifest, and requires
128 MiB, four compute lanes, batch size 8192, and I/O concurrency 32. The Q21
baseline and candidate must also agree on metadata-cache size, use no warmup
and one measured run, and match compiler, native flags, OS, architecture, and
CPU.
The candidate and low-memory manifests must contain the same complete SF10
generation object and manifest digest; dataset path spelling is not compared.

Prefer a v0.4 Q21 report with an enclosing SF10 `dataset` object. If the
original v0.4 JSON cannot be enriched, retain the canonical rendered `q21.sql`
next to the preserved `q21.json` and pass
`--allow-legacy-v04-missing-dataset`. This explicit compatibility switch
waives only the old baseline dataset field; it never permits missing candidate
or Join provenance. A passing result emits a warning and records
`legacy_v04_missing_dataset: true`, which must be disclosed in release notes.

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
cache, and records that fact in its manifest. `metadata-warm` remains the
stable name for a positive metadata-cache budget; the release gate performs no
explicit warmup before its one measured run. MinIO data must already be
uploaded at `--minio-root`; the runner starts and initializes the repository's
MinIO service by default (`START_MINIO=0` disables that behavior). It compares
the remote and local dataset manifests. Before timing, every distinct
target/thread/batch configuration is executed independently and compared with
DuckDB; t1 and t4 entries therefore reference different checksum artifacts.
The manifest records SHA-256 digests of the runner, helper library, and
checksum runner used to produce the evidence. An explicitly supplied build ID
or CPU model is accepted only when it matches the current worktree or detected
host, so an environment override cannot silently relabel a run.

For the v0.5 M5 Max gate, capture the local SF10 candidate and compare it with
the clean `v0.4.0-alpha.1` SF10 manifest:

```sh
THREADS_LIST="1 4" BATCH_SIZES=8192 CACHE_MODES=warm \
MEMORY_LIMIT_BYTES=1073741824 WARMUP=0 ITERATIONS=1 START_MINIO=0 \
benchmarks/run_baseline.sh --local-root data/tpch-sf10 \
  --output benchmarks/results/baseline/<candidate-run>

python3 -B benchmarks/check_parallel_gate.py \
  --candidate benchmarks/results/baseline/<candidate-run>/manifest.json \
  --baseline benchmarks/results/baseline/v04-9e1b98c-sf10-strict/manifest.json
```

The checker defaults to `0.5.0-alpha.1` candidate reports, a
`0.4.0-alpha.1` baseline, and the local `v0.4.0-alpha.1` tag. Explicit
`--candidate-engine-version`, `--baseline-engine-version`, and
`--baseline-tag` arguments allow the same strict harness to be reused by a
future release without weakening report validation. Both manifests and every
selected report must carry the matching benchmark executable SHA-256; the
checker prints the exact version pair in both human and JSON output.

The checker reads only local, metadata-warm, batch-8192 reports for
`scan-filter` and `aggregate` at one and four threads. It requires M5 Max,
native release, 1 GiB, no warmup, one measured run, matching build/data
fingerprints and checksums, at least 2.0x four-thread throughput, and no more
than 10% one-thread regression. The candidate manifest must name the exact
40-character commit of the current clean worktree, and every selected thread
configuration must have its own checksum path. Any missing or inconsistent
field fails. The host-side checker reads the CPU model from `sysctl`, rebuilds
the candidate native-release executable in OrbStack, compares its digest with
every candidate report, and re-hashes both repositories' harness and dataset
manifest files rather than trusting stored digest strings.

The v0.5 single-file CSV gate is separate from the TPC-H matrix:

```sh
benchmarks/run_csv_scaling.sh
```

The default is the release gate, not a configurable benchmark. It requires a
clean 40-character candidate commit on an Apple M5 Max, version
`0.5.0-alpha.1`, the fixed 10 GiB deterministic 64-byte-record fixture, one
native-release executable SHA-256 for both reports, 1 GiB, batch 8192, I/O 32,
an 8 MiB CSV morsel, no metadata cache, no warmup, one measurement, and at least
1.8x four-thread/one-thread throughput. Release settings cannot be lowered with
environment variables.

Every measured run must return the expected rows, scan the complete source, and
agree on source/decompressed byte counts. The summary records each thread
configuration's physical batch count separately because record-aligned morsel
boundaries may differ. Schema sampling may make byte counters exceed the fixture
size. For a configurable, non-release harness check, opt in explicitly:

```sh
TARGET_BYTES=67108864 WARMUP=0 ITERATIONS=1 MINIMUM_SPEEDUP=1.0 \
  benchmarks/run_csv_scaling.sh --smoke \
  data/csv-scaling-smoke.csv benchmarks/results/csv-scaling/smoke
```

The smoke summary always records `"mode": "smoke"` and
`"release_qualified": false`; it cannot be presented as release evidence.

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
