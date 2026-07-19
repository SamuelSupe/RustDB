# ClickBench functional acceptance

RustDB keeps two pinned ClickBench query identities:

- ClickHouse's 43 canonical queries remain the immutable source workload and
  the query set selected by the explicit `full` performance profile. The usual
  file/table and physical-type adapter is still applied when RustDB renders SQL.
- Beta functional acceptance executes the versioned
  [`queries-rustdb.sql`](queries-rustdb.sql) derivative. It preserves the
  canonical operations while adding complete tie-breakers to every bounded
  result whose canonical SQL leaves tied rows unspecified. The manifest binds
  both query-file SHA-256 identities, so the derivative cannot silently drift
  away from its source.

The functional suite runs those 43 deterministic queries once. It is a RustDB
functionality and reliability check, not a comparison or repeated performance
gate.

The default `functional` profile uses ClickHouse's official
122,446,530-byte, one-million-row Parquet partition. It builds `rustdb-bench`
in OrbStack and executes it with four CPUs, a 12-GiB container limit, a 4-GiB
RustDB engine budget, batch size 8192, and I/O concurrency 16. Every query fully
consumes its streamed result and records its typed checksum, execution metrics,
stderr, rendered SQL, and cleanup status.

The pass is bound to the pinned `functional-oracle-v2.json`. The oracle names
the canonical-query, deterministic-query, and dataset SHA-256 identities and
records the expected row count plus `rustdb-typed-multiset-sha256-v1` checksum
for each query. The runner rejects a mismatch in its manifest, and Beta
finalization independently compares all 43 results with the preflight copy of
the oracle.

The v2 oracle is sealed from exactly one independently generated rowset produced
by a digest-pinned `clickhouse-local` image. That engine is used only while
creating and reviewing the correctness oracle; the ordinary functional pass
and Beta release gate do not start ClickHouse. Neither its execution time nor
RustDB's execution time is used for a performance comparison.

Q4 uses the exact Int128 `sum(UserID) / count()` ratio as its reference because
the direct ClickHouse `avg(Int64)` path overflows this fixture. Q24 also records
that `(EventTime, WatchID)` has no duplicate group inside its filter before the
pair is accepted as the stable raw-row tie-break.

The official Parquet file intentionally has no logical date/time annotations.
Following the official ClickBench DataFusion adapter, the runner treats
`EventDate` as epoch days and `EventTime` as epoch seconds. This adaptation is
visible in every retained rendered SQL file.

The historical partitioned fixture additionally exposes text as Parquet
`Binary`. The functional profile enables `run.py --binary-as-string`, applying
the same compatibility interpretation used by Arrow's own ClickBench reader
benchmark.

Oracle regeneration is deliberately separate from the daily gate. The two
small scripts verify all three input identities, require the immutable
ClickHouse image digest recorded in the oracle, compare all 43 rowsets, and
only then seal RustDB's typed checksums:

```sh
python3 -B benchmarks/clickbench/reference_generate.py --help
python3 -B benchmarks/clickbench/reference_seal.py --help
```

Run it from the repository root:

```sh
benchmarks/clickbench/run.sh
```

The canonical queries and official 100M, 14,779,976,446-byte single-file
dataset remain available as an explicit, non-gating performance profile:

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
`CLICKBENCH_RESULT_ROOT`. A release acceptance run keeps the documented
four-CPU / 12-GiB-container / 4-GiB-engine profile; an overridden diagnostic
run is not interchangeable with release evidence.

The retained 2026-07-17 v0.7 functional-pass manifest is in
[`evidence/20260717-functional-1m-4c16g.json`](evidence/20260717-functional-1m-4c16g.json).
It records 43/43 successful canonical queries under the historical v0.7
resource profile. Its timings are diagnostic only and are not Beta acceptance
evidence.
