# RustDB v0.7 comparison harness

This directory contains the new comparison path. It does not replace the
DuckDB 1.4.3 correctness oracle under `tools/tpch`.

The two workers remain alive across warmups and measured iterations. Query
timing starts at `execute` and ends only after every result batch has been
consumed into the same streaming multiset checksum. The coordinator alternates
engine order and records elapsed time, time to first result batch, peak process
RSS, threads, memory, concurrency, cache state, dataset identity, and storage
track.

Concurrent groups use a two-stage harness gate: every query first reports
ready, then one parent release timestamp starts all query clients. RustDB uses
a persistent multi-thread Tokio orchestration runtime in addition to its
separately configured engine compute pool; DuckDB keeps one persistent
connection per fixed client thread. Every query records its zero-based slot,
run-scoped harness ID, start and finish offsets, while the run records the
observed start skew. Current contract reports require those identities and a
zero terminal RustDB reservation for concurrency greater than one. A retained
single-query v3 report remains readable when it contains none of the new group
protocol fields; if any such field is present, the complete protocol is
required for that group.

Current-protocol runs also record the constraints observed inside each worker:
the process CPU affinity, effective cgroup cpuset, CPU quota/period, cgroup
memory maximum, and visible physical memory. Hello and every measured run must
agree, and RustDB and DuckDB must observe identical constraints. A finite CPU
or memory cgroup must cover the configured engine budget. RustDB additionally
records the Engine root memory pool after the whole concurrent group has
finished: current reservation must be zero and the lifetime peak must stay
within the one shared 2/4 GiB limit. Per-query peaks are never added because
they need not occur simultaneously. For concurrency one, the historical v3
shape is accepted only when the entire report omits every new protocol field;
once any new field appears, both engines and all runs must use the full shape.

For RustDB, `elapsed_ms` still includes query execution, complete result-stream
consumption and checksum finalization; `ttfb_ms` remains the time from
`execute` to the first returned batch. After those timed boundaries are closed,
the runner drops `QueryResult` and only then snapshots
`current_reservation_bytes`. Dropping the result is intentionally outside
`elapsed_ms`: the terminal reservation is a cleanup diagnostic, not timed query
work. This prevents result-owned state from being mislabeled as a query-end
leak. Only reports produced after this runner change have that terminal
reservation meaning; older nonzero values must not be reinterpreted as leaks or
compared directly with the new field semantics.

DuckDB is pinned to 1.5.4 and PyArrow to 24.0.0 with hashed arm64/x86_64 Python
wheels. It consumes `RecordBatch` values through `to_arrow_reader`; it does not
materialize DB-API tuples. Every fixed executor thread owns one persistent
connection to the same temporary file database. Connections and threads are
created before the first command, so timed groups include dispatch and the
simultaneous-start barrier but not connection, thread, or executor startup.

Current reports use contract `rustdb-v07-benchmark-v3` and checksum
`multiset-sha256-v2`. Each value carries a canonical logical
type family, including NULL values; finite floats use canonical f64 IEEE bits,
NaNs and signed zero are normalized, and timestamps are normalized to exact
microseconds. The checksum remains row-order independent and duplicate
sensitive. Contracts v1 and v2 are historical and readable only with
`--allow-historical`; they are labeled non-gate evidence. Reports may never mix
checksum versions across engines, runs, or queries. Current records also name
the engine-specific checksum backend and its measured compute time.
Every current hello/run pair also carries a lowercase SHA-256 build ID. The
provided smoke script hashes the exact Rust worker executable and records the
exact DuckDB benchmark image ID, so two reports from the same alpha version can
still be attributed to different dirty-worktree artifacts. The Rust worker
hashes its own running executable before emitting hello and rejects a mismatched
`--build-id`; manual independent-runner builds must use
`CARGO_TARGET_DIR=/workspace/target`, matching the provided scripts. Reports also record
the host OS/release, architecture, CPU model, logical CPU count and physical
memory. `storage_medium` is mandatory and distinguishes local NVMe from MinIO.
RustDB query records additionally carry reservation, scheduler, Spill, Join,
CSV source/decompressed bytes, morsels, parser lanes, and per-operator counters
so RSS and CSV scaling gaps can be traced to an execution stage. Typed additive
timings separate global compute-permit wait, full-queue backpressure, execution
barriers, raw CSV source I/O, quote-aware framing, and Arrow CSV decode. These
are cumulative lane/task times and may exceed wall-clock latency; the older
`scheduler_wait_ms` remains a mixed compatibility counter.

RustDB records may additionally attribute full-queue waits to the CSV morsel,
scan pipeline output, Aggregate lane-dispatch, and Aggregate partial-output
edges as `csv_morsel_queue_wait_ms`,
`scan_pipeline_output_queue_wait_ms`,
`aggregate_lane_dispatch_queue_wait_ms`, and
`aggregate_partial_output_queue_wait_ms`. These optional cumulative task/lane
waits can overlap with each other and with compute, so they must not be summed
and interpreted as wall-clock latency. Contract v3 validates each value when
present and continues to accept retained reports that predate these fields.

Join phase profiles are timer-only children of `Join`. `JoinBuild`,
`JoinProbe` and `JoinSpillExecution` describe wall-clock paths. Eligible
multiplicity builds additionally report cumulative `JoinBuildInputPoll`,
`JoinBuildPermitWait`, `JoinBuildKeyEval` and `JoinBuildHashTable` attribution
under `JoinBuild`. Input poll elapsed time includes time waiting for the RHS
stream. Permit wait is recorded in `wait_ms`, is a typed subset of the global
compute-permit wait, and keeps `elapsed_ms` zero. Hash-table elapsed time
includes hashing, lookup, reservation and insertion. `JoinKeyWork` remains
cumulative active build plus probe key work and therefore overlaps the two
build compute children. These values must not be summed as independent wall
time. Phase nodes intentionally carry zero row/batch/byte counters. In the
fused direct Join-Aggregate path,
`Join.output_batches` counts non-empty internal probe morsels, not public
result batches; public stream batches remain in `queries[].batches`.

Parquet attribution additionally reports coalesced range bytes/time, decoder
active time and polls, decoder-specific compute-permit wait, RowFilter
compute/evaluations/input rows, and schema-alignment time. RowFilter compute is
a subset of decoder active time and must not be added to it. Range bytes are
coalesced requested spans rather than physical storage traffic. Contract v3
validates these additive fields when present. Retained single-query reports
created before the concurrency identity/start-gate fields remain readable.

`parquet_narrow_decimal_columns` counts eligible file-columns decoded with the
query-local Decimal64 hint and widened exactly to the public Decimal128 schema.
It is an attribution counter, not a promise that a particular query or file
encoding will use the optimization.

Native predicate-sidecar attribution reports
`native_predicate_sidecar_bytes_read`, rows evaluated and selected, exact
RowFilter bypasses, fallbacks to the ordinary Parquet predicate path, direct
full-projection bypasses, and directly projected rows. The remaining fields
share that prefix and end in `rows_evaluated`, `rows_selected`,
`exact_bypasses`, `fallbacks`, `full_projection_bypasses`, and
`full_projection_rows`; safe full-projection declines additionally use
`native_predicate_sidecar_full_projection_fallback_row_groups`. These counters
are RustDB-only diagnostics; they do not change the cross-engine timed
boundary. Contract v3 validates them when present, while keeping them optional
for reports that satisfy the current concurrency identity protocol.

The `.rdbpred` file is a query-neutral companion understood for compatibility
with Native snapshots that already declare one; production Native imports do
not create it. Its format can index `Int8/16/32/64`, `Date32`, and
precision-at-most-18 Decimal values. Exact AND predicates over `Int64`,
`Date32`, and those Decimal values can use the compatibility reader; narrower
integers currently fall back. RustDB range-reads only the declared index and
required blocks, while new imports, snapshots without a companion, and
uncovered predicates use the ordinary Parquet path. A resource fallback is
visible in the counters above, while a declared companion that is missing,
corrupt, rebound, or changes identity invalidates the run instead of silently
falling back. A benchmark intended to exercise this reader must use an explicit
retained sidecar fixture and record nonzero sidecar reads or fallbacks. A fresh
production import is expected to report zero sidecar counters.

Legacy-sidecar attribution must account for two distinct cost gates. When
Parquet still decodes the projected payload, required predicate blocks must be
no more than half the compressed Parquet predicate-only bytes they avoid;
zero-column scans admit up to twice those bytes. Direct full projection
requires complete blocks for the union of predicate and output columns and at
least a 2x byte advantage: total required sidecar bytes must be no more than
half the corresponding compressed Parquet bytes. The compatibility reader may
plan a bounded chunk of up to four row groups atomically and falls back before
range I/O when any group is uncovered or the whole chunk misses the gate. A
valid report with nonzero sidecar counters proves attribution, not a speedup.
Reports may attribute safe full-projection declines with
`native_predicate_sidecar_full_projection_fallback_row_groups`; failures are
not counted as fallback row groups.

The July 2026 unconditional-sidecar SF1 Q6 diagnostic matched checksums but
regressed RustDB steady p50 to 98.690 ms versus DuckDB 5.244 ms. A subsequent
contract-valid direct-projection comparison used four threads, a 2 GiB limit,
concurrency one, zero warmup, and ten alternating rounds. Checksums matched;
RustDB/DuckDB steady p50 was 46.241733/5.100391 ms, query peak RSS was
60,604,416/161,484,800 bytes, and final storage was
346,305,523/261,894,548 bytes from 226,756,631 source bytes. The first RustDB
query read 19,415,612 sidecar bytes, evaluated 3,101,247 rows, selected 59,313,
directly projected 25 row groups/59,313 rows, then built eight Parquet readers,
range-read 22,837,761 bytes, and spent 37.36 ms decoding the remainder. This
improved the unconditional experiment but regressed the retained no-sidecar
RustDB p50 of 25.810 ms. Consequently the forced per-row-group chunking and 2:1
direct-projection admission were rejected.

A later RustDB-only SF1 Q6 diagnostic used bounded multi-row-group projection
without forced per-row-group tasks. Its steady p50 was still 45.250696 ms and
final storage was 346,311,971 bytes, versus the retained no-sidecar 25.810 ms
and about 268.37 MB. Production Native imports therefore default to no
predicate sidecar. The bounded multi-row-group implementation and 2x byte-
benefit gate remain only as protection when reading an existing declared
sidecar; they are not a default optimization or a speedup claim.

The RustDB-only default-off follow-up at the ephemeral path
`/tmp/rustdb-v07-native-sf1-default-no-sidecar.json` used 4 threads, a 2 GiB
limit, concurrency one, batch 8,192, zero warmups, and ten rounds. Checksum
prefix `a973df...` matched; steady/overall p50 was
23.371626/23.4836615 ms, first query was 30.992136 ms, and load was
4,360.106355 ms. Query peak RSS was 58,449,920 bytes with a 28,930,048-byte
delta. Final/peak storage was 268,366,819/268,368,524 bytes from 226,756,631
source bytes (1.1835x). Every round used four lanes with zero sidecar counters,
zero terminal reservation, and no Spill.

Relative to the rejected bounded-sidecar diagnostic, steady query time fell
48.35%, load time fell 48.29%, and final storage fell 22.51%. The diagnostic
p50 is about 9.45% below the retained 25.810 ms no-sidecar formal result, but
the protocols differ. This validates the default-off implementation only; it
is not a new DuckDB comparison or release-gate result.

A subsequent global file-aware row-group fan-in experiment is developer
history only and was fully withdrawn. In the same RustDB-only SF1 Q6 shape it
reduced reader builds from 20 to 5, but local opens remained 5, decode compute
was effectively unchanged, and steady p50 regressed from 23.371626 to
24.631957 ms (+5.39%). Its ephemeral report is
`/tmp/rustdb-v07-native-sf1-global-fanin.json`; no DuckDB run was made and no
fan-in source or dedicated test remains.

A Native main-Parquet low-level predicate-filter experiment is also developer
history only and was fully withdrawn. It decoded eligible fixed-width
predicate pages directly, built a row selection, and reduced ordinary
RowFilter evaluations to zero without adding a sidecar. Against the same
default-off RustDB-only baseline, however, steady p50 regressed from
23.371626 to 48.619134 ms (+108.02%), overall p50 rose from 23.4836615 to
49.4506895 ms, and range bytes rose from 46,656,573 to 49,336,259 (+5.74%).
Candidate decode compute was 157.398099 ms on the first round and 143.903464 ms
on the last versus approximately 64--74 ms in the baseline. The checksum,
four-lane execution, zero terminal reservation and zero Spill remained intact;
peak RSS was 57,618,432 bytes and final storage was 268,368,869 bytes. The
ephemeral candidate report is `/tmp/rustdb-v07-native-sf1-main-filter.json`.
No DuckDB run was made because the RustDB-only retention gate failed.

A Native lane-local fused global-Aggregate experiment is likewise developer
history only and was fully withdrawn. It accumulated eligible Aggregate state
inside each scan lane and merged the four partial states after scanning.
Against the same default-off RustDB-only baseline, steady p50 regressed from
23.371626 to 29.660108 ms (+26.91%), overall p50 rose from 23.4836615 to
29.991224 ms (+27.71%), and first query time rose from 30.992136 to
33.386618 ms. Parquet range bytes were effectively unchanged at 46,654,344,
while cumulative steady decode compute rose from approximately 64--74 ms to
mostly 80.789--94.930 ms. Peak RSS fell to 52,043,776 bytes and peak
reservation to 3,474,041 bytes, but the candidate lost scan/downstream overlap
and failed the latency retention gate. The checksum, four-lane execution, zero
terminal reservation and zero Spill remained intact; final storage was
268,325,184 bytes. The ephemeral report is
`/tmp/rustdb-v07-native-sf1-fused-global-agg.json`. The implementation and its
dedicated tests were removed, and no DuckDB run was made.

The retained streaming Composite multiplicity build has two ephemeral reports.
`/tmp/rustdb-v07-composite-counted-after.json` is a RustDB-only external
Parquet diagnostic: relative to the prior arena build, one measured query fell
from 23.656896 to 20.784128 ms, Join from 21.379134 to 17.530640 ms, peak
reservation from 50,410,112 to 43,907,078 bytes, and absolute peak RSS from
98,238,464 to 83,554,304 bytes with all correctness/I/O/resource invariants
unchanged. `/tmp/rustdb-v07-composite-counted-duckdb.json` is the subsequent
contract-v3 comparison; it measured RustDB/DuckDB 1.5.4 query time at
21.940506/11.686795 ms and absolute peak RSS at
87,810,048/128,913,408 bytes. The optimization is retained, but RustDB remains
about 1.88x slower in that one sample and the external Parquet gate is open.

The direct byte-pair follow-up is retained from
`/tmp/rustdb-v07-byte-pair-rustdb.json`: versus the row-encoded counted build,
one RustDB-only measurement reduced query/Join from
20.784128/17.530640 to 17.100970/14.362954 ms and peak reservation from
43,907,078 to 35,799,176 bytes with unchanged invariants. Its subsequent
contract-v3 report `/tmp/rustdb-v07-byte-pair-duckdb.json` measured
RustDB/DuckDB query time at 21.990907/19.197138 ms and absolute RSS at
70,684,672/134,447,104 bytes. RustDB remained 14.6% slower; the same DuckDB
image also varied sharply from the preceding one-shot report, so neither result
is promoted to a stable cross-engine gate.

Reservation-safe probe morselization is retained from
`/tmp/rustdb-v07-probe-morsels-yield.json`. Against its phase baseline, one
RustDB-only measurement reduced query/Join/JoinProbe from
22.536598/17.014958/9.675982 ms to 15.927041/13.089965/4.195114 ms and raised
peak active lanes from two to four without changing the 35,799,176-byte peak
reservation. The subsequent contract-v3 report
`/tmp/rustdb-v07-probe-morsels-duckdb.json` measured RustDB/DuckDB 1.5.4 query
time at 17.473810/13.291847 ms and absolute RSS at
84,303,872/127,086,592 bytes. This is one warmup plus one measured iteration,
not a stable p50 or release-gate result; RustDB remains slower.

A shared-shard parallel build was then rejected from
`/tmp/rustdb-v07-parallel-build.json`: query/Join/JoinBuild regressed to
23.420063/20.372513/14.218040 ms while correctness and resource invariants
held. Per-row shard locking and loss of scan/build overlap made it slower; the
implementation was removed without a repeated sample or DuckDB run.

The retained single-probe byte-pair insertion is recorded in
`/tmp/rustdb-v07-single-probe.json`. Relative to the retained probe-morsel
sample, one RustDB-only measurement reduced query/Join/JoinBuild from
15.927041/13.089965/8.892060 ms to 13.884914/10.936665/6.442499 ms. Peak
reservation rose 7.85% to 38,609,218 bytes, absolute RSS fell 10.64%, and all
checksum, scan, candidate, Spill and terminal-reservation invariants held. The
implementation is retained on that causal screen. Its one contract-v3 report
`/tmp/rustdb-v07-single-probe-duckdb.json` measured RustDB/DuckDB 1.5.4 query
time at 21.914247/12.333998 ms and absolute RSS at
83,898,368/132,980,736 bytes. RustDB was 77.7% slower; the large RustDB
cross-run variance means this is evidence that the external Parquet gate is
still open, not evidence of a stable cross-engine trend.

A later one-slot CSV read-ahead candidate was rejected without a DuckDB run.
Using the same temporary release binary for its internal off/on screen, query
time regressed from 43.624504 to 53.768943 ms, peak reservation from
19,411,904 to 24,791,520 bytes and absolute RSS from 62,038,016 to
82,784,256 bytes. Cumulative Scan work improved, but the total latency and
memory gates take precedence. The candidate and its diagnostic environment
switch were removed; `/tmp/rustdb-v07-csv-read-ahead-{off,on}.json` are
ephemeral negative evidence only.

The subsequent partial-batch carry is retained after 13/13 focused tests and
independent review with no P0/P1. Its one RustDB-only causal sample reduced
query time 43.624504 to 33.118906 ms, TTFB 42.542233 to 32.151090 ms, Scan
batches 143 to 131 and peak reservation 19,411,904 to 19,038,016 bytes. One
current contract-v3 comparison measured RustDB/DuckDB 1.5.4 at
37.664323/31.999285 ms and 60,329,984/197,476,352 bytes absolute RSS: RustDB
remained 17.7% slower while using 69.4% less RSS, so the latency gate stays
open. The same protocol also passed one tiny concurrency-two environment smoke
with equal worker constraints and zero terminal RustDB root reservation; that
fixture is resource evidence, not performance evidence. A separate Native
`PLAIN + ZSTD` candidate was fully rolled back after steady SF1 Q6 regressed
106.31% despite 21.69% lower RSS. All corresponding `/tmp` JSON files are
ephemeral diagnostics, not persistent benchmark artifacts.

Adaptive CSV source reads are retained at
`min(target_morsel_bytes, 4 MiB, operation_limit / 8)` after 15/15 focused
tests. The sole RustDB-only sample improved query/TTFB/source-I/O from
33.118906/32.151090/21.622067 ms to 26.657224/24.078814/14.889478 ms, raised
parser lanes three to four and kept 131 Scan batches, while peak reservation
rose from 19,038,016 to 29,524,832 bytes. Checksum, bytes, 16 morsels, zero
Spill and zero terminal root reservation matched. Its one contract-v3
comparison measured RustDB/DuckDB 1.5.4 at 35.654805/28.803281 ms and
66,928,640/200,945,664 bytes absolute RSS. RustDB remained 23.79% slower but
used 66.69% less RSS and reached 2.426x throughput per absolute RSS in this
sample. The `/tmp` reports are ephemeral, the comparison was run once, and the
CSV/release gates remain open.

New RustDB records may also contain `execute_return_ms`, query-admission wait,
plus SQL parse, table-function preparation, bind, provider preparation,
optimizer and Native verification timings. Admission and parse happen before
the existing query-metrics `elapsed` clock, while Native verification is nested
inside provider preparation. `execute_return_ms` is the internal eager boundary
where `Session::execute` returns; it must not be compared directly with another
engine's eager API boundary. Contract v3 accepts these fields as optional so
the existing bounded SF1 report remains valid, while validating their values
whenever they are present.

Run the tiny contract smoke through OrbStack:

```sh
benchmarks/v07/run_smoke.sh /tmp/rustdb-v07-smoke.json
```

The smoke worker and coordinator settings share the same environment values;
for example, one tiny eight-thread/four-query protocol check is:

```sh
THREADS=8 MEMORY_LIMIT=4294967296 CONCURRENCY=4 \
  benchmarks/v07/run_smoke.sh /tmp/rustdb-v07-concurrency-smoke.json
```

Validate an existing report without running either engine:

```sh
python3 -B benchmarks/v07/contract.py /tmp/rustdb-v07-smoke.json
```

Screen a CSV or Parquet implementation candidate without launching DuckDB:

```sh
python3 -B benchmarks/v07/rustdb_external_only.py \
  --query benchmarks/v07/queries/parquet-composite-utf8-join-aggregate.sql \
  --dataset data/tpch-sf1/customer \
  --storage-track parquet --storage-medium local-nvme \
  --output /tmp/rustdb-v07-external-only.json \
  --threads 4 --memory-limit 2147483648 --concurrency 1 --batch-size 8192 \
  --warmup 1 --iterations 1 \
  --rustdb-command-json "$RUSTDB_COMMAND"
```

`RUSTDB_COMMAND` is the same JSON worker command assembled by `run_smoke.sh`.
This runner validates full result consumption, checksum consistency, RSS and
terminal reservation, but labels its report `diagnostic_only` and
`comparison_gate_eligible=false`. Use it to reject a local candidate before a
cross-engine run, never as DuckDB or release-gate evidence.

Historical reports require explicit read-only validation:

```sh
python3 -B benchmarks/v07/contract.py --allow-historical old-v2-report.json
```

Run the bounded local Native SF1 reliability comparison once:

```sh
benchmarks/v07/run_native_sf1.sh /tmp/rustdb-v07-native-sf1.json
```

For a RustDB-only implementation diagnostic that does not invoke DuckDB or
write a cross-engine contract report, use:

```sh
benchmarks/v07/run_native_rustonly_sf1.sh /tmp/rustdb-v07-native-rustonly.json
```

This path creates one fresh bounded Native workspace, imports once, reopens it,
checks ten no-warmup Q6 rounds by default, and removes the temporary database
afterward. For a bounded single-sample diagnostic, pass `--rounds 1` to the
underlying `rustdb_only.py`; the default ten-round workflow remains unchanged.
`CARGO_BUILD_JOBS=1` can be set for low-memory OrbStack hosts to reduce the
release-runner build peak.

For a short RustDB/DuckDB Native comparison, set
`DIAGNOSTIC_ONLY=1 ITERATIONS=1`. The resulting report is validated and
explicitly marked `comparison_gate_eligible=false`; formal release reports
continue to require ten measured iterations.
The harness also accepts `BATCH_SIZE=<rows>` for diagnostic experiments; the
engine default remains 8192.
It is useful for rejecting or retaining a local implementation candidate; it
does not satisfy a DuckDB comparison or release gate.

The Native track freezes the exact local Parquet files selected by its
canonical manifest. The report records every relative path, absolute worker
location, size and SHA-256; both workers verify those bytes before and after
running the same sorted, wildcard-free CTAS statements. Each worker fully
consumes all imports, writes an atomic versioned setup marker bound to the exact
table-name set, closes, and reopens the same single database before the first
query. Each worker also rejects CTAS input paths that do not match the frozen
per-file manifest exactly. Native reports require zero
warmups and ten measured rounds. They expose load time separately and compute
`(load + sum(query_rounds)) / 10`; the first round is called
`first-post-reopen`, not cold, because ordinary development runs do not clear
the OS page cache. Each engine has one bounded workspace, capped at 2x source
bytes plus 64 KiB per table and 1 MiB of harness metadata, and no database is
copied per query or round. Native MinIO source identity needs a later object
version/ETag contract; v3 intentionally accepts only local source files.

RustDB database open performs its full Native integrity verification before the
worker emits `hello`; first-post-reopen query time therefore does not represent
database-open cost. The query verifier now reuses the process-local identities
produced by that mandatory open verification instead of hashing the same
unchanged segments again. A future startup gate must time worker/database open
separately rather than moving it into or out of query latency.

The workspace limit is checked around every CTAS, sampled at 2 ms while setup
is active, and checked exactly at completion. This is a practical fail-closed
benchmark guard, not a filesystem quota capable of proving that a transient
shorter than the sampling interval never existed.

The smoke fixture validates orchestration and evidence, not performance. Until
both engines share one native result consumer, the current gate rejects queries
returning more than 65,536 rows; comparison workloads should normally return a
small aggregate result so engine-specific checksum work cannot dominate. Any
performance evidence produced before Arrow result consumption, independent
DuckDB connections, and checksum v2 is historical only and must be rerun before
it can satisfy a v0.7 gate. The bounded Native SF1 Q6 path uses four threads,
2 GiB and concurrency one; one run is reliability and attribution evidence,
not a release performance claim. MinIO configuration and the full 4/8-thread,
concurrency 1/2/4 matrix remain later gated v0.7 work.
