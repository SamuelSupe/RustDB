# RustDB v0.1 acceptance and baseline workflow

Goal: turn the verified v0.1 engine into a reproducible correctness,
low-memory, and performance baseline that can be enforced in CI.

Success criteria:

- Preserve the existing implementation as local tag `v0.1.0-alpha.1`.
- Generate deterministic TPC-H Parquet datasets with pinned DuckDB tooling.
- Compare Q1, Q3, Q6, Q11, Q12, Q13, and Q14 against DuckDB by sorted
  checksums, with SF1 as the release correctness gate.
- Provide an SF10 64/128 MiB spill suite for Sort, Aggregate, Inner Join, and
  Left Join, with result, memory, spill, and cleanup assertions.
- Capture machine-readable benchmark metadata and p50/p95/first-batch/RSS/S3/
  spill metrics for local and MinIO runs.
- Add a platform-neutral CI entrypoint plus hosted CI configuration where the
  repository context supports it.
- Pass formatting, strict Clippy, all tests, script checks, release build, and
  workflow verification through OrbStack.

Constraints:

- Keep DuckDB pinned to 1.4.3 and dataset generation deterministic.
- Keep generated datasets and benchmark result files out of Git.
- Do not push, publish, or use production credentials.
- Full SF10 generation is a resource gate: inspect disk/time first and do not
  fabricate a completed baseline when only the harness was exercised.

Current baseline: commit `74194ab`, tag `v0.1.0-alpha.1`.
