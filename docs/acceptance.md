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

The fixed-hardware SF1 local/MinIO baseline is then run explicitly:

```sh
tools/tpch/upload_minio.sh 1
benchmarks/run_baseline.sh \
  --local-root data/tpch-sf1 \
  --minio-root s3://rustdb-tests/tpch-sf1 \
  --output benchmarks/results/baseline/<run>
```

The runner refuses an existing output directory, builds the benchmark binary
with `-C target-cpu=native`, records that build configuration, verifies the
remote manifest, and checksum-validates the actual output from both storage
targets before collecting timings. Portable CI release builds do not use
native CPU flags.

Only SF1 correctness is a release-blocking TPC-H gate. SF10 is a separately
recorded resource/performance acceptance run; absence of the required dataset
or hardware must be reported as not run, never as a pass.
