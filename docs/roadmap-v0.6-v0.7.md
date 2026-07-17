# RustDB v0.6-v0.7 Development Goals

Status: v0.6 foundation implemented; v0.7 active
Baseline date: 2026-07-15

## Current delivery goal

The active delivery goal is `v0.7.0-alpha.1`. It preserves the v0.6 Native
storage and reliability foundation while completing the vector execution,
global scheduling, and memory-efficiency work below.

The current release acceptance target is functionality-first: run all 43
ClickBench queries once on ClickHouse's official one-million-row Parquet
partition in a 4-CPU, 16-GiB OrbStack container. RustDB receives a 12-GiB
engine budget. Every result is fully consumed and checksummed, and all terminal
resources must be released. This target does not compare with another engine
and does not require repeated timing runs. The official 100M single-file
dataset remains an opt-in performance workload, not a release requirement.
Low-memory Spill remains a safety check rather than the primary workload.

## North-star goal

RustDB remains a read-optimized, single-node, embedded OLAP engine for local
files, S3/MinIO, and persisted Native data. The combined v0.6 and v0.7 goal is
complete analytical-query behavior, predictable resource ownership, and useful
multi-core execution without making cross-engine performance a release gate.

The next two versions prioritize storage layout, cost-based planning, vectorized hot paths, and multi-query resource efficiency. Additional file formats and broad SQL expansion are not priorities for this cycle.

## v0.7 functional acceptance contract

The active v0.7 workload is one complete ClickBench pass:

- Use the official 43-query SQL file without dropping unsupported queries.
- Use the official one-million-row Parquet partition for the routine functional
  profile. Its historical physical Binary and integer date/time columns are
  adapted explicitly in retained SQL, following the official Arrow/DataFusion
  benchmark adapters.
- Run in an OrbStack container capped at four CPUs and 16 GiB. Configure four
  RustDB compute threads and a 12-GiB engine memory limit.
- Use zero warmups and one measured execution. Continue after failures so the
  manifest reports the complete SQL-surface gap in one pass.
- Fully consume every Arrow result and retain the typed checksum, row count,
  rendered SQL, engine metrics, stderr, build identity, dataset identity, and
  cgroup limits.
- Pass only when all 43 queries finish, no timeout occurs, and query-scoped
  tasks, reservations, I/O jobs, and Spill state quiesce.
- Keep the official 100M single-file profile available as
  `CLICKBENCH_PROFILE=full`; it is optional and has no latency threshold.

TPC-H, MinIO, recovery, cancellation, and focused resource tests remain part of
ordinary correctness work. Repeated timing rounds, a comparative score, SF100,
and broad low-memory soak testing are not v0.7 release requirements.

## v0.6.0-alpha.1: Persistent Native column store and CBO foundation

### Objective

Build a durable, locally persisted Native column store that can become the authoritative copy after CSV or Parquet import. Establish reliable internal measurement plus the statistics and physical-planning foundation needed by v0.7.

v0.6 is a reliability-first foundation milestone without a cross-engine timing
or throughput gate.

### Native storage

- Add a persistent engine-open path while retaining the existing ephemeral engine mode.
- Support atomic bulk import from CSV and Parquet, bulk append, and whole-table replacement.
- Store immutable column segments with a versioned on-disk format.
- Select compact encodings per column/page, including plain, dictionary, RLE, delta/bit-packed, and a general compression fallback.
- Store checksums and query-neutral statistics with each segment: row count, byte size, null count, min/max, approximate distinct count, Top-K, and bounded histograms where applicable.
- Support column and segment pruning without materializing unneeded columns.
- Keep the implementation split into focused format, encoding, segment, manifest, recovery, and backup modules.

### Catalog, commit, and recovery

- Persist the Catalog and versioned table manifests locally.
- Publish new immutable segments through an atomic, fsync-backed manifest commit.
- Pin an immutable manifest snapshot for every query.
- Queries already running during append or replacement continue to see the old snapshot; queries started after commit see the new snapshot.
- A failed or interrupted commit leaves the previous version readable.
- Recovery selects the latest completely valid generation and removes only recognized, uncommitted files.
- No row-level WAL or MVCC is introduced; a small metadata commit journal or equivalent generation protocol is allowed.
- Provide consistent backup/export and restore from another local disk or S3.
- Guarantee recovery from process failure and operating-system restart after a successful commit. Disk-device loss, replication, and high availability are out of scope.

### Disk limits

- Final Native data, indexes, and Catalog occupy no more than 2.0 times the source CSV or Parquet size, plus a bounded 64 KiB format-metadata allowance per table.
- Import is streaming and does not create another complete temporary copy.
- Peak import usage, including the source files, is no more than 3.0 times the source size, plus the same bounded per-table metadata allowance.
- After source deletion, usage falls back to the Native limit of at most 2.0 times the source size.
- Highly compressed inputs that cannot satisfy the final 2.0-times limit are rejected with a resource error.
- Append and replacement preflight includes retained old snapshots, source bytes, and new segments. The operation is rejected rather than silently exceeding the 3.0-times peak limit.

### Optimizer and correctness foundation

- Separate logical optimization from physical planning.
- Add column statistics and selectivity estimation for Native tables and bounded sampling/statistics for external files.
- Use dynamic-programming join enumeration for join graphs up to eight relations and a greedy fallback for larger graphs.
- Cost scans and joins using estimated CPU, I/O, memory, and Spill pressure.
- Show estimated and actual cardinalities in `EXPLAIN ANALYZE` for cost-model calibration.
- Fix analytical SUM result widening instead of narrowing wide accumulators at output.
- Add a compact `IN` set representation instead of expanding large lists into OR trees.
- Introduce compact physical column maps so projection pruning does not retain full logical schemas or synthesize unnecessary NULL arrays.
- Add reusable CTE materialization when repeated evaluation is more expensive than a shared result.

### v0.6 acceptance

- Focused local and MinIO fixtures verify CSV/Parquet import, Native scans, supported SQL, and deterministic result checksums without using DuckDB as an oracle.
- Native imports, appends, replacements, restart recovery, and snapshot visibility pass deterministic fault-injection checks.
- Backup and restore reproduce schemas, row counts, and query checksums.
- Native and peak-import disk limits are enforced and reported.
- Memory reservations, active tasks, snapshots, temporary files, and staging files return to zero after success, error, cancellation, and restart recovery.
- Run one lightweight 2 GiB resource/Spill smoke scenario and one small 1/4-thread performance sanity check. Neither is compared with DuckDB and neither is repeatedly soaked.
- Run the complete OrbStack verification once when the v0.6 release candidate is ready.

## v0.7.0-alpha.1: Vector execution core and ClickBench completeness

### Objective

Remove per-row object allocation, unnecessary Arrow batch reconstruction, and
query-local scheduling bottlenecks. Complete the SQL and type behavior needed
to execute every ClickBench query under the fixed functional resource profile.

### Internal vector execution

- Keep Arrow `RecordBatch` as the public input/output boundary.
- Use an internal compact vector batch with selection vectors, constant vectors, dictionary references, and physical column identifiers.
- Fuse Scan, Filter, and Project without repeatedly rebuilding full `RecordBatch` values.
- Evaluate common typed predicates and arithmetic through compact vector programs and specialized kernels.
- Materialize only final output columns or columns required by a pipeline breaker.

### Join and aggregation hot paths

- Replace row-wise `CellValue` and `HashMap<Vec<CellValue>, ...>` join keys.
- Use specialized fixed-width key layouts and a compact contiguous arena/dictionary representation for variable-width keys.
- Store join payloads column-wise and avoid copying unused pass-through columns.
- Preserve early termination for Semi and Anti joins.
- Publish compact min/max plus Bloom/Xor-style runtime filters where beneficial.
- Replace objectized aggregate states with key-to-group-id tables and contiguous typed state arrays.
- Keep adaptive partitioning and victim-partition Spill as resource fallbacks, not default execution paths.

### Native and external scans

- Evaluate predicates against Native dictionary/encoded pages where possible.
- Use late materialization after zone-map, dictionary, and selection pruning.
- Continue improving direct Parquet row-selection and runtime-filter pruning without hidden persistent data caches.
- Keep CSV framing ordered while decoding record-aligned morsels across compute lanes.

### Global scheduling and memory

- Introduce a global work-stealing scheduler across all active queries.
- Enforce fair CPU shares under the fixed four/eight-thread engine budget.
- Prevent long analytical queries from indefinitely starving short queries.
- Keep object I/O, decode, compute, and Spill I/O in separate bounded pools.
- Allocate weighted memory permits across queries and operators using actual retained bytes.
- Reserve RSS headroom for allocator and decoder overhead; reduce lanes, batch size, and cache use before process RSS reaches the configured limit.
- Report scheduler wait, query quiescence, retained memory, RSS, and throughput-per-GiB metrics.

### Adaptive planning and reuse

- Feed actual pipeline-breaker cardinalities into downstream strategy and lane decisions.
- Choose broadcast, partitioned hash, or sort-merge joins from estimated and observed footprints.
- Cache bound/physical plans for stable Native prepared statements and invalidate them by Catalog generation.
- Reuse compatible CTE/build results within a query without introducing general cross-query result caching.

### v0.7 acceptance

- All correctness checks from v0.6 remain green.
- All 43 ClickBench queries pass once on the official 1M Parquet partition in
  the documented four-CPU/16-GiB container and 12-GiB engine budget.
- The manifest retains per-query typed checksums and complete resource/build
  provenance; no external-engine comparison is required.
- Local CSV/Parquet, MinIO, and Native correctness checks remain green.
- All tasks, reservations, queues, I/O jobs, snapshots, and temporary files return to zero after success, error, cancellation, or consumer abandonment.
- Run the complete OrbStack verification once at release-candidate completion; avoid repeated extreme low-memory soak runs.

## Explicitly deferred

- Iceberg, JSON, ORC, and additional lake formats.
- Row-level `UPDATE` and `DELETE`, row-level MVCC, and general transactions.
- Replication, high availability, distributed execution, and service protocols.
- Workload-specific physical design in the primary benchmark.
- Large SQL-surface expansion that is not required by TPC-H, ClickBench, or JOB.

## Implementation constraints

- Keep modules small and single-purpose; avoid large aggregate files.
- Prefer direct, simple implementations over framework-style abstraction layers.
- Continue to prohibit project-owned `unsafe` code and do not introduce DataFusion.
- Use OrbStack for the final build, Clippy, unit, integration, recovery, MinIO, and benchmark verification.
