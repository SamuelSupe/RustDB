# TPC-H correctness harness

Run deterministic data generation and the complete Q1-Q22 checksum gate:

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

The suite runner accepts a checked-in query list and a distinct RustDB data
root. The report mode keeps per-query stderr and writes `status.tsv` plus
`provenance.json`. The provenance binds the report to the Git commit/worktree
state, release-binary hash, query-list and query-file hashes, dataset manifest,
data root, and effective execution configuration. The runner remains a strict
gate: an unsupported query, execution error, or checksum mismatch makes the
command fail after all selected queries have run.
After an explicit release build, `TPCH_SKIP_BUILD=1` avoids rebuilding the
same binary for independent local, MinIO, or constrained-memory runs.

```sh
tools/tpch/compare.sh --report --queries benchmarks/tpch/cases/sf1-local.txt 1

tools/tpch/compare.sh --report \
  --queries benchmarks/tpch/cases/sf1-minio.txt \
  --rustdb-root s3://rustdb-tests/tpch-sf1 1
```

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
and a row-group size of 122,880. Comparisons require and retain the CSV header
even for zero-row results, retain column order and text values, round
non-integral numeric results to 1e-6, sort rows, and compare SHA-256 checksums.

The SF10 constrained SQL set is Q2/Q16/Q17/Q20/Q21/Q22. Run it with the same
strict runner:

```sh
TPCH_MEMORY_LIMIT_BYTES=134217728 TPCH_REQUIRE_SPILL=1 \
  tools/tpch/compare.sh --report \
    --queries benchmarks/tpch/cases/sf10-128m.txt 10
```

`TPCH_REQUIRE_SPILL=1` additionally requires non-zero Spill metrics, a peak
reservation no greater than the configured limit, and no residual query
directory. It is not an allow-failure switch.

Official references:

- [DuckDB v1.4.3 release](https://github.com/duckdb/duckdb/releases/tag/v1.4.3)
- [DuckDB TPC-H extension and `dbgen`](https://duckdb.org/docs/stable/core_extensions/tpch.html)
- [DuckDB `COPY` statement](https://duckdb.org/docs/stable/sql/statements/copy.html)
- [DuckDB Parquet writes](https://duckdb.org/docs/stable/data/parquet/overview.html#writing-to-parquet-files)
