# Acceptance and CI

RustDB keeps routine code quality, native portability, TPC-H correctness, and
resource-heavy performance work as separate gates. This prevents every
platform job from downloading or generating the same analytical dataset while
still requiring one real S3-compatible integration run.

## Local quality gate

The supported local entrypoint uses the pinned development image and live
MinIO services through OrbStack:

```sh
scripts/ci/orbstack.sh all
```

It runs formatting, strict Clippy, all Cargo targets, required MinIO tests, and
the portable release build. `scripts/ci/check.sh` contains the same Cargo
commands without depending on a particular CI product. To run the live MinIO
test gate directly on a Linux Docker host:

```sh
scripts/ci/with_minio.sh scripts/ci/check.sh test
```

`with_minio.sh` creates an isolated Compose project, provisions the private and
anonymous test buckets, exports only fixed test credentials, and removes the
project and volumes when the command exits. `check.sh test` refuses to run
unless MinIO is marked as required and has a configured endpoint, preventing
the S3 integration suite from being silently skipped.

## Hosted runner selection

The hosted workflow uses explicit image labels rather than `*-latest`:

| Gate | Runner label | Architecture | Scope |
| --- | --- | --- | --- |
| Quality, live MinIO, release | `ubuntu-24.04` | Linux x64 | Required |
| Native portability | `ubuntu-24.04-arm` | Linux arm64 | Required |
| Native portability | `macos-15` | macOS M1 arm64 | Required |

GitHub's current standard-runner table lists these labels for public and
private repositories. It also lists 14 GB of runner storage; public Linux jobs
receive 4 CPUs/16 GB RAM, private Linux jobs receive 2 CPUs/8 GB, and standard
arm64 macOS jobs receive 3 M1 CPUs/7 GB RAM. See the
[GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Linux arm64 became available to private repositories in January 2026; GitHub's
[official announcement](https://github.blog/changelog/2026-01-29-arm64-standard-runners-are-now-available-in-private-repositories/)
also confirms the `ubuntu-24.04-arm` label.

GitHub documents that arm64 macOS hosted runners do not support nested
virtualization. Consequently, the Linux x64 job is the single authoritative
live-MinIO integration gate. Linux arm64 and macOS arm64 run all targets
natively, allowing the S3-only test to skip, and then build every release
target. This split exercises the supported architectures without pretending a
containerized service ran on macOS. GitHub-provided Actions are documented as
arm64 compatible; the workflows therefore use only `actions/checkout` and
`actions/cache`, not community setup Actions.

Cargo caches contain registry and Git dependency sources only, are separated
by operating system and architecture, and do not cache `target/`. GitHub notes
that cache entries are immutable and must not contain secrets in its
[dependency caching reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching).

## TPC-H correctness

Run the focused DuckDB 1.4.3 SQL-semantics differential before TPC-H:

```sh
tools/sql/differential.sh
```

It compares truth predicates, projection aliases, grouping ordinals, HAVING
aliases, and hidden sort expressions through canonicalized result checksums.

The seven-query checksum gate is deliberately a separate, single Linux x64
job. Run the fast development gate with:

```sh
tools/tpch/run.sh 0.01
```

Run the release correctness gate with SF1:

```sh
tools/tpch/run.sh 1
```

The `TPC-H acceptance` workflow can be started manually with either scale. Its
weekly schedule runs SF1. Generated Parquet data is cached under
`data/tpch-sf<scale>` with a key containing the pinned DuckDB version and the
generation/query scripts. No platform matrix repeats the download or
generation. A cache miss must regenerate and validate the dataset rather than
being treated as a failure.

Acceptance requires sorted checksums to match DuckDB 1.4.3 for Q1, Q3, Q6,
Q11, Q12, Q13, and Q14. A release record should include the dataset manifest,
query checksums, RustDB commit, and workflow run URL.

## Resource and performance gates

SF10 and fixed-hardware measurements are not routine hosted-CI jobs: the 14 GB
hosted-runner disk limit and shared hardware make them unsuitable for a stable
performance baseline. After generating SF10, run the 64/128 MiB spill suite on
the nominated machine:

```sh
benchmarks/run_low_memory.sh data/tpch-sf10
```

The command writes a timestamped machine-readable manifest and per-case JSON
reports below `benchmarks/results/low-memory/`. Before an alpha is promoted,
verify:

- Sort, Aggregate, Inner Join, and Left Join checksums are correct.
- Each checksum execution uses its stated 64/128 MiB limit and must itself
  report non-zero Spill before the separately measured run is accepted.
- Peak engine reservation stays within the configured memory limit.
- Spill bytes are non-zero for the forced-spill cases.
- Every query-scoped spill directory is empty after success, error, or cancel.
- Startup orphan tests preserve an old directory while `.rustdb-active` is
  locked, then remove it after lock release; unknown directories stay intact.
- The suite manifest records the RustDB build identifier and generated dataset
  manifest digest.
- Local NVMe and MinIO benchmark results record hardware, cache state,
  executable versions, p50/p95, first-batch latency, RSS, throughput, S3
  requests, and spill metrics.

Use a small forward run to validate benchmark plumbing before spending time on
the full matrix:

```sh
THREADS_LIST=1 BATCH_SIZES=1024 CACHE_MODES=cold \
  benchmarks/run_baseline.sh --smoke \
  --output benchmarks/results/smoke-forward
```

The release-blocking parallel performance sample is local SF10 on an Apple M5
Max. Capture the candidate with only the required matrix dimensions (the
baseline suite may still emit its other query cases; the gate ignores them):

```sh
THREADS_LIST="1 4" BATCH_SIZES=8192 CACHE_MODES=warm \
MEMORY_LIMIT_BYTES=1073741824 WARMUP=2 ITERATIONS=5 START_MINIO=0 \
benchmarks/run_baseline.sh \
  --local-root data/tpch-sf10 \
  --output benchmarks/results/baseline/<candidate-run>

docker compose run --rm --no-deps --no-TTY dev \
  python3 benchmarks/check_parallel_gate.py \
  --candidate benchmarks/results/baseline/<candidate-run>/manifest.json \
  --baseline benchmarks/results/baseline/<alpha2-sf10-run>/manifest.json
```

The baseline must come from a clean `v0.1.0-alpha.2` build over the same SF10
manifest. The gate resolves that local tag and rejects a different baseline
build identifier. The candidate must be the exact 40-character commit of the
current clean worktree. Each target/thread/batch configuration is checksum-run
independently; sharing one checksum path between t1 and t4 is rejected. Runner,
helper-library, and checksum-runner digests are stored in each manifest. The
gate also fails on missing or duplicate matrix entries,
unverified checksums, a dataset mismatch, non-M5-Max hardware, a non-release or
non-native build, settings other than local metadata-warm / batch 8192 / 1 GiB
/ warmup 2 / five measurements, or a report whose build/config does not match
its manifest. Missing evidence is a failure, never a pass.

For both `scan-filter` and `aggregate`, the candidate must satisfy:

- `t1_p50 / t4_p50 >= 2.0` (the four-thread throughput multiplier);
- candidate one-thread p50 no more than 10% slower than alpha.2;
- identical result checksums for candidate/baseline at one and four threads.

The runner refuses an existing output directory, builds `rustdb-bench` with
`-C target-cpu=native`, records the build and dataset fingerprints, and
checksum-validates results before timing. Portable CI release builds do not
use native CPU flags, and hosted CI checks parallel correctness without using
this hardware timing gate.

SF1 remains the routine TPC-H correctness gate. SF10 is the separately recorded
resource/performance release gate; absence of its dataset, alpha.2 baseline, or
nominated hardware must be reported as not run, never as a pass.
