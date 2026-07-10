# RustDB

RustDB is a read-only, single-node OLAP engine written in Rust. It queries CSV
and Parquet files from local storage or S3-compatible object stores and returns
Apache Arrow record batches. The SQL planner, optimizer, execution operators,
memory accounting, and spill behavior live in this repository; DataFusion is
not a runtime dependency.

## Development

RustDB's repeatable development environment runs through OrbStack's Docker
engine:

```sh
docker compose build dev
docker compose run --rm dev cargo fmt --check
docker compose run --rm dev cargo clippy --all-targets -- -D warnings
docker compose run --rm dev cargo test
```

`compose.yaml` also starts MinIO for S3 integration tests. The credentials in
that file are test-only values.

## CLI

Run one query:

```sh
docker compose run --rm dev cargo run --release -- \
  -c "SELECT count(*) FROM read_parquet('/data/events/*.parquet')"
```

Start the interactive shell by omitting `-c` and `-f`. Results can be written
as a table, CSV, or JSON Lines.

## Library

```rust,no_run
use futures::StreamExt;
use rustdb::{Engine, EngineConfig, ParquetOptions};

# async fn example() -> rustdb::Result<()> {
let engine = Engine::new(EngineConfig::default())?;
let session = engine.session();
session
    .register_parquet("events", ["/data/events/*.parquet"], ParquetOptions::default())
    .await?;

let mut result = session.execute("SELECT count(*) FROM events").await?;
while let Some(batch) = result.stream().next().await {
    println!("{:?}", batch?);
}
# Ok(())
# }
```

## Supported scope

- Read-only external CSV and Parquet data.
- Local paths, globs, `file://`, AWS S3, and custom S3 endpoints.
- Vectorized projection, filter, aggregation, sort, limit, and equi-joins.
- Query cancellation, execution metrics, bounded memory, and local spill.

RustDB does not currently provide managed tables, transactions, DML, a server
protocol, distributed execution, or DuckDB compatibility. CSV is streamed and
parallelized across files; a single compressed or multiline CSV is not split
into byte ranges.

See [architecture.md](docs/architecture.md) for the execution model and
[compatibility.md](docs/compatibility.md) for the SQL and format contract.
[s3.md](docs/s3.md) covers AWS and MinIO configuration, and
[troubleshooting.md](docs/troubleshooting.md) covers resource, spill, and input
errors. The repeatable benchmark workflow is documented in
[benchmarks/README.md](benchmarks/README.md).
