# TPC-H correctness harness

Run the complete deterministic data generation and seven-query checksum gate:

```sh
tools/tpch/run.sh 0.01
```

Use `1` for the daily SF1 gate. Data is cached under
`data/tpch-sf<scale>/`; every reuse verifies the eight Parquet SHA-256 values.
Delete that scale directory to regenerate it. Query diagnostics and canonical
checksums are written under the dataset's `results/latest/` directory.

To compare one workspace-local query template against any workspace-relative
dataset, use:

```sh
tools/tpch/compare_query.sh benchmarks/tpch/q06.sql data/tpch-sf1
```

It prints the single canonical SHA-256 value on success.

To validate the actual S3 execution path against that local DuckDB reference,
pass a third root:

```sh
tools/tpch/compare_query.sh benchmarks/tpch/q06.sql data/tpch-sf1 \
  s3://rustdb-tests/tpch-sf1
```

Upload a verified generated dataset to the repository's test-only MinIO
service before running the local/S3 performance matrix:

```sh
tools/tpch/upload_minio.sh 1
benchmarks/run_baseline.sh \
  --local-root data/tpch-sf1 \
  --minio-root s3://rustdb-tests/tpch-sf1
```

The upload replaces only `rustdb-tests/tpch-sf<scale>`, verifies every remote
Parquet object with `mc stat`, reads both manifest files back for byte-for-byte
comparison, and prints the resulting S3 URI. It uses the fixed development
credentials already declared in `compose.yaml`; these are test-only
credentials and are not written to benchmark reports.

The reference executable is DuckDB 1.4.3. The container build downloads the
official amd64 or arm64 CLI release asset, verifies its release SHA-256, and
installs the version-matched TPC-H extension into the image. The host does not
need DuckDB. Docker with Compose v2, Python 3, and a POSIX shell are required.

The generator uses one thread, explicit primary-key ordering, zstd level 3,
and a row-group size of 122,880. Comparisons parse both CSV streams, retain the
column order and text values, round non-integral numeric results to 1e-6, sort
rows, and compare SHA-256 checksums.

Official references:

- [DuckDB v1.4.3 release](https://github.com/duckdb/duckdb/releases/tag/v1.4.3)
- [DuckDB TPC-H extension and `dbgen`](https://duckdb.org/docs/stable/core_extensions/tpch.html)
- [DuckDB `COPY` statement](https://duckdb.org/docs/stable/sql/statements/copy.html)
- [DuckDB Parquet writes](https://duckdb.org/docs/stable/data/parquet/overview.html#writing-to-parquet-files)
