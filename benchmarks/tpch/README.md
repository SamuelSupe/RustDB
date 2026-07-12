# TPC-H query fixtures

`q01.sql` through `q22.sql` are the deterministic RustDB correctness suite.
The query parameters and semantics track DuckDB 1.4.3's checked-in TPC-H
queries at
[`extension/tpch/dbgen/queries`](https://github.com/duckdb/duckdb/tree/v1.4.3/extension/tpch/dbgen/queries).
The fixtures replace catalog table names with `read_parquet` calls and use
explicit equality joins so the same SQL can run unchanged in RustDB and the
pinned DuckDB reference container.

The checked-in case lists are:

- `queries.txt`: compatibility default containing Q1-Q22.
- `cases/sf1-local.txt`: complete local Parquet SF1 gate.
- `cases/sf1-minio.txt`: complete MinIO SF1 gate.
- `cases/sf10-128m.txt`: Q2/Q16/Q17/Q20/Q21/Q22 constrained-memory gate.

Rows are compared without depending on execution order. `canonicalize.py`
requires and retains the result header, including for zero-row results,
normalizes finite numerics to six decimal places, sorts rows, and hashes the
canonical JSONL stream.
