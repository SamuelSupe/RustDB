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
as a table, CSV, or JSON Lines. `--csv-null TOKEN` supplies an unambiguous SQL
NULL marker for machine comparisons; the default remains an empty CSV field.

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
- Raw, gzip, and zstd CSV selected by content magic, including concatenated
  gzip members and multi-frame zstd streams.
- Record-aligned single-file CSV morsels decoded across bounded compute lanes;
  quoted newlines are never split between decoders, and oversized-record
  errors identify the URI and decompressed offset.
- Vectorized projection, filter, aggregation, sort, limit, and equi-joins.
- Typed string, NULL, numeric, and temporal scalar functions; aggregate
  `DISTINCT`; and set-at-a-time, one-level correlated subqueries.
- Ranking, distribution, and aggregate window functions with named windows and
  `QUALIFY`; DISTINCT and multiset set operations; and equi
  `RIGHT`/`FULL`/Left Semi/Anti joins with `USING` where applicable.
- Parsed-only prepared statements with typed Rust parameters; every execution
  rebinds the live catalog and fixes a fresh external-object snapshot.
- Query-time file discovery, atomic schema refresh with stable CSV column order,
  and safe Parquet widening.
- Budgeted Parquet page-index/Bloom pruning, bounded same-column `IN`/`OR`,
  query runtime filters, and singleflight metadata loads, with residual
  predicates retained for correctness.
- Multi-lane pipelines, cancellation-aware memory backpressure, query-scoped
  optimizer statistics, workspace-aware accounting, and
  quota/free-space-governed Spill.

RustDB does not currently provide managed tables, transactions, DML, a server
protocol, distributed execution, or DuckDB compatibility. During data scan,
each CSV object is read once in source order, optionally decompressed, and
framed only at complete record boundaries before parsing in parallel; RustDB
does not issue speculative S3 ranges across quoted records. `compute_threads`
is an execution upper bound:
actual lanes also depend on available Scan tasks, operator eligibility, and
memory budget. Result order is unspecified without `ORDER BY`.

See [architecture.md](docs/architecture.md) for the execution model and
[compatibility.md](docs/compatibility.md) for the SQL and format contract.
[s3.md](docs/s3.md) covers AWS and MinIO configuration, and
[troubleshooting.md](docs/troubleshooting.md) covers resource, spill, and input
errors. [parquet-pruning.md](docs/parquet-pruning.md) documents page-index and
Bloom-filter policy. [migration-v0.5.md](docs/migration-v0.5.md) covers the
`CsvOptions` builder migration, compressed/parallel CSV behavior, prepared
statements, and new metrics. [migration-v0.4.md](docs/migration-v0.4.md),
[migration-v0.3.md](docs/migration-v0.3.md), and
[migration-v0.2.md](docs/migration-v0.2.md) cover earlier releases.
The repeatable benchmark workflow is documented in
[benchmarks/README.md](benchmarks/README.md), and the release gates are listed
in [acceptance.md](docs/acceptance.md).
