# RustDB v0.1 implementation workflow

Goal: build the planned read-only, single-node Rust OLAP engine with a Rust API and CLI.

Success criteria:

- A clean checkout builds and tests in the OrbStack-backed development container.
- Local CSV and Parquet files can be registered and queried through SQL.
- `read_csv(...)` and `read_parquet(...)` work from SQL, including S3-compatible object storage.
- Projection, filtering, grouping, sorting, limits, inner/left joins, explain, cancellation, metrics, memory accounting, and disk-backed blocking operators are implemented and covered by tests.
- No DataFusion runtime dependency is introduced.

Constraints:

- Keep modules focused and reasonably small; avoid speculative abstractions.
- Use Arrow `RecordBatch` as the execution batch.
- Keep credentials out of repository state and diagnostics.
- Run default integration verification with OrbStack and MinIO.

Known risk: the requested scope is database-engine sized. Deliver vertical, tested behavior first, then widen SQL/operator coverage without leaving placeholders in advertised behavior.
