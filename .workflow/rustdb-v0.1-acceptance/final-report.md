# RustDB v0.1 acceptance report

Status: final engineering gates and independent read-only audits passed;
clean-commit SF1, SF10, and performance reruns pending.

The initial engine baseline is frozen at `74194ab` and tagged
`v0.1.0-alpha.1`.

Delivered and verified:

- deterministic DuckDB 1.4.3 TPC-H Parquet generation and manifest checks;
- strict SF1 checksum parity for Q1, Q3, Q6, Q11, Q12, Q13, and Q14;
- SF10 Sort/Aggregate/Inner Join/Left Join Spill acceptance at 64/128 MiB;
- a 64-run SF1 local/MinIO performance matrix with machine-readable metrics;
- platform-neutral CI entrypoints, hosted workflow definitions, operator
  documentation, and reproducible OrbStack commands;
- OrbStack format, strict Clippy, 166-test live-MinIO suite, and release build.

The SF10 and 64-run measurements above are pre-commit candidates, not final
acceptance evidence. Generated datasets and measurements are intentionally
ignored. The final acceptance tag will be created locally only after a
separate read-only audit and clean-commit correctness/resource/performance
reruns. Nothing is pushed or published.
