# Integration result

Status: completed. Engineering gates, clean-commit evidence, and independent
read-only audits passed for implementation
`a9688d65d792d683fa899491b2b8e4118cb8df0b`.

## Correctness

- Deterministic SF1 and SF10 dataset manifests were verified against all eight
  expected TPC-H Parquet files.
- SF1 Q1, Q3, Q6, Q11, Q12, Q13, and Q14 matched the pinned DuckDB 1.4.3
  reference by strict sorted-result SHA-256 checksum.
- The seven-query SF1 gate was rerun on the accepted implementation after all
  Decimal, join planning, Spill, memory-accounting, and benchmark fixes.

## Low-memory acceptance

The accepted artifact
`benchmarks/results/low-memory/20260711-alpha2-clean-r2/manifest.json` records
eight SF10 runs: Sort, Aggregate, Inner Join, and Left Join at both 64 and
128 MiB. All eight matched DuckDB, stayed within their configured reservation
limit, produced non-zero Spill, reported cleanup, and left no Spill or query
files behind. Detailed metrics and checksums are recorded in `low_memory.md`.

## Performance baseline

The accepted artifact
`benchmarks/results/baseline/20260711-m5max-sf1-alpha2-clean-r2/manifest.json`
contains 64 SF1 reports across local Parquet and MinIO, metadata-cold and warm
modes, four query shapes, 1/4 compute threads, and 4096/8192 batch sizes. It
names the exact accepted build, native release flags, SF1 dataset digest, and
records p50/p95, first-batch, RSS, throughput, S3, memory, and Spill metrics.

Representative p50 values below use the fixed 4-thread, 8192-row batch
configuration. Cold is one measured run with a new zero-cache engine; warm is
the median of five runs after two warmups. The OS page cache was not flushed.

| Case | Local cold ms | Local warm ms | MinIO cold ms | MinIO warm ms |
| --- | ---: | ---: | ---: | ---: |
| Scan/filter | 238.359 | 258.612 | 458.997 | 483.413 |
| Aggregate | 557.851 | 505.839 | 841.855 | 1,099.661 |
| Inner Join | 2,119.321 | 2,432.249 | 2,547.252 | 3,294.957 |
| Top-K | 87.015 | 85.596 | 124.076 | 108.346 |

Every local measured run reported zero S3 requests and bytes; every MinIO run
reported non-zero requests and transferred bytes. All four local checksums
equal their MinIO counterparts, all peak reservations stayed below 1 GiB, and
every run reported clean Spill teardown.

The earlier `20260711-m5max-sf1-alpha2-clean` candidate was rejected because a
shell-scope defect caused warm reports to retain
`metadata_cache_bytes=0`. The benchmark helper now isolates each invocation
and validates all five engine settings before accepting a report. In this r2
artifact, all 32 cold reports use zero bytes and all 32 warm reports use
67,108,864 bytes.

## Final engineering gates

- `scripts/ci/orbstack.sh all`: format, strict all-target Clippy, 166 tests
  including live MinIO, and all-target release build passed.
- All project shell scripts passed `sh -n`; TPC-H Python helpers compiled and
  their canonicalizer tests passed.
- Compose validation, Actionlint 1.7.7, `git diff --check`, and the workflow
  artifact verifier passed.
- The locked dependency tree contains Arrow/Parquet 59.1.0,
  object_store 0.13.2, and sqlparser 0.62.0, with no DataFusion dependency.

Hosted CI was not run because this repository has no configured remote. No
remote success is claimed, and no credentials are stored in result artifacts.
