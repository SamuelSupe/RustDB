# ClickBench functional acceptance

This suite runs all 43 official ClickBench queries once. It is a RustDB
functionality and reliability check, not a comparison or repeated performance
gate.

The default `functional` profile downloads ClickHouse's official
122,446,530-byte, one-million-row Parquet partition. It builds `rustdb-bench`
in OrbStack and executes it in a container limited to four CPUs and 16 GiB.
RustDB receives a 12-GiB engine memory budget; the remaining memory is reserved
for the allocator, Parquet decoder, and container runtime. Every query fully
consumes its streamed result and records its typed checksum, execution metrics,
stderr, rendered SQL, and cleanup status.

The official Parquet file intentionally has no logical date/time annotations.
Following the official ClickBench DataFusion adapter, the runner treats
`EventDate` as epoch days and `EventTime` as epoch seconds. This adaptation is
visible in every retained rendered SQL file.

The historical partitioned fixture additionally exposes text as Parquet
`Binary`. The functional profile enables `run.py --binary-as-string`, applying
the same compatibility interpretation used by Arrow's own ClickBench reader
benchmark.

Run it from the repository root:

```sh
benchmarks/clickbench/run.sh
```

The official 100M, 14,779,976,446-byte single-file dataset remains available
as an explicit, non-gating performance profile:

```sh
CLICKBENCH_PROFILE=full benchmarks/clickbench/run.sh
```

That download is resumable. When `aria2c` is available the runner uses 16
ranges and sparse in-place writes; otherwise the bounded fallback keeps at
most 32 128-MiB parts in flight. Staging never creates another complete dataset
copy.

Results are written to a new timestamped directory below
`benchmarks/results/clickbench/`. A failed query does not stop later queries,
so one pass always identifies the complete compatibility gap. The process exits
non-zero unless all 43 queries pass.

Useful deliberate overrides are `CLICKBENCH_RUN_NAME`,
`CLICKBENCH_QUERY_TIMEOUT_SECONDS`, `CLICKBENCH_DATA_DIR`, and
`CLICKBENCH_RESULT_ROOT`. Resource limits stay at the documented defaults for
the 4-core/16-GiB acceptance run.

The retained 2026-07-17 functional-pass manifest is in
[`evidence/20260717-functional-1m-4c16g.json`](evidence/20260717-functional-1m-4c16g.json).
It records 43/43 successful queries. Its timings are diagnostic only.
