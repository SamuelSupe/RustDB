# RustDB v0.1 acceptance report

Status: completed. Final engineering gates, clean-commit evidence, workflow
verification, and independent read-only audits passed.

The initial engine baseline is frozen at `74194ab` and tagged
`v0.1.0-alpha.1`. The accepted implementation is
`a9688d65d792d683fa899491b2b8e4118cb8df0b`; this evidence commit is tagged
locally as `v0.1.0-alpha.2`.

Delivered and verified:

- deterministic DuckDB 1.4.3 TPC-H Parquet generation and manifest checks;
- strict SF1 checksum parity for Q1, Q3, Q6, Q11, Q12, Q13, and Q14;
- clean SF10 Sort/Aggregate/Inner Join/Left Join Spill acceptance at 64/128
  MiB, with all eight runs correct, spilling, within reservation, and clean;
- a clean 64-report SF1 local/MinIO performance matrix with exact build ID,
  cold/warm configuration checks, paired checksums, and machine-readable
  metrics;
- platform-neutral CI entrypoints, hosted workflow definitions, operator
  documentation, and reproducible OrbStack commands;
- OrbStack format, strict Clippy, 166-test live-MinIO suite, and release build.

Accepted generated evidence remains intentionally ignored:

- `benchmarks/results/low-memory/20260711-alpha2-clean-r2/manifest.json`
- `benchmarks/results/baseline/20260711-m5max-sf1-alpha2-clean-r2/manifest.json`

Known alpha limitations remain: reservations constrain engine-accounted Arrow
and operator memory rather than process RSS; hosted CI was not run without a
remote; and the full explicit S3 retry/network-fault injection matrix is still
future work. Nothing was pushed or published.
