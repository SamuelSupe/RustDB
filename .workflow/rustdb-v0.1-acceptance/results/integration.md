# Integration result

Status: audit fixes integrated; final engineering gates and independent
read-only audits passed. Clean-commit SF1, SF10, and performance reruns remain.

## Correctness

- Deterministic SF1 and SF10 dataset manifests were verified against all eight
  expected TPC-H Parquet files.
- SF1 Q1, Q3, Q6, Q11, Q12, Q13, and Q14 matched the pinned DuckDB 1.4.3
  reference by strict sorted-result SHA-256 checksum.
- The SF1 checksum gate was rerun after the final Decimal, join planning, and
  Spill implementation changes.

## Low-memory acceptance

The superseded diagnostic artifact
`benchmarks/results/low-memory/20260710T225721Z/manifest.json` records eight
SF10 runs: Sort, Aggregate, Inner Join, and Left Join at both 64 and 128 MiB.
All eight runs matched DuckDB, stayed within their configured reservation
limit, produced non-zero Spill, reported cleanup, and left no `query-*`
directory behind. It is not final acceptance evidence because it predates the
implementation commit and the last memory-accounting fixes.

## Performance baseline

The superseded candidate artifact
`benchmarks/results/baseline/20260711-m5max-sf1-final/manifest.json` records 64
SF1 measurements across local Parquet and MinIO, metadata-cold and warm modes,
four query shapes, 1/4 compute threads, and 4096/8192 batch sizes. It records
the dataset manifest digest, build identifier, CPU, p50/p95, first-batch, RSS,
throughput, S3, memory, and Spill metrics. Local runs made no S3 requests;
every MinIO run transferred data through S3. Representative warm p50 values
were 189.699 ms local versus 209.737 ms MinIO for scan-filter, and 43.068 ms
local versus 53.944 ms MinIO for Top-K.

The candidate build identifier is intentionally reported as
`74194ab2707d2c5e4f1e2239b64e9e918b1b0841-dirty`. After the implementation
commit, the SF10 suite and complete 64-run matrix will be repeated from clean
output directories so the durable artifacts name the exact source commit.

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
