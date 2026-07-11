# Packet: tpch_harness

Objective: provide pinned, deterministic TPC-H Parquet generation and a
seven-query DuckDB checksum suite.

Ownership: `tools/tpch/**`, `benchmarks/tpch/**`, and a dedicated benchmark
container definition. Do not edit engine code, CI, or low-memory scripts.

Verification: generate a small scale factor, run all seven queries in RustDB
and DuckDB, and fail on a checksum mismatch.
