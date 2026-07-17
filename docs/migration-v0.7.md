# Migrating from v0.6 to v0.7 alpha

`v0.7.0-alpha.1` is currently under development. It retains the v0.6 public
`Engine`, `Session`, Native storage, registration, prepared-statement, and
streaming Arrow result APIs.

The active release target is now a functionality-first, single ClickBench pass
under a four-CPU/16-GiB container profile. Cross-engine timings later in this
document are retained development history, not a current release gate or claim.
The ClickBench compatibility slice adds `regexp_replace`/`regex_replace`,
`to_timestamp_seconds`, checked integer-to-Date casts, and strict temporal
literal comparison. These additions require no Rust API migration.

The current changes are internal execution and diagnostics improvements:

- `Engine::memory_snapshot()` exposes a non-exhaustive engine-wide accounting
  snapshot with current bytes, lifetime peak bytes, and the configured root
  limit. This is additive; existing callers require no source change;

- one engine-wide, query-aware compute-slot scheduler;
- short-lived compute admission for Window input and output CPU phases; permits
  are released before memory waits, Spill I/O, stream yields, and queue sends,
  so concurrent Window queries cannot bypass the engine thread budget;
- compact projected Scan/Filter/Project pipelines;
- batch-compiled literal `LIKE` matchers. Exact and simple boundary patterns
  use direct string operations, arbitrary multi-`%` patterns use ordered
  literal segments, and `_` patterns reuse one Unicode-character DP workspace.
  NULL, `NOT LIKE`, custom `ESCAPE`, and lazy invalid-pattern errors are
  unchanged; dynamic pattern expressions retain the generic path;
- row-count-preserving zero-column expression projections, so residual filters
  followed by metadata-only `COUNT(*)` remain valid Arrow batches;
- specialized fixed-width and single-column `Utf8`/`LargeUtf8` Join keys, plus
  bounded runtime filters; the UTF-8 table owns each distinct build key once,
  keeps duplicate row ids in a separately accounted sidecar, and probes with
  borrowed Arrow string values instead of allocating a row key per probe;
- a matching single-column `Binary`/`LargeBinary` Join table that owns each
  distinct build key once and probes borrowed Arrow byte slices without UTF-8
  conversion, including embedded NUL and arbitrary bytes. NULL, deduplication,
  reservation rollback, ordinary Join and direct Join-Aggregate runtime-filter
  semantics are preserved; summary and Spill paths remain on the generic
  implementation;
- an Arrow row-encoded primary Hash Join table for eligible exact-type tuples
  of at least two flat keys. Build encoding is chunked, probe encoding happens
  once per batch, and distinct encoded keys share one checked contiguous byte
  arena instead of one heap allocation per key. Hash collisions still compare
  the complete encoded tuple; retained rows, arena bytes, table allocations,
  duplicate ids, and encoder metadata are reservation-backed. Float,
  Dictionary, nested, existence-summary,
  Grace, and Spill execution remain generic; failed primary admission goes
  directly to Spill instead of constructing a second Generic table;
- exact post-coercion type validation for Hash Join keys. An incompatible wide
  Decimal equality stays residual when another safe equality key exists;
  otherwise `ON`/`USING` reports that an explicit common `CAST` is required,
  and the physical verifier rejects any mismatched key pair;
- direct temporal `CellValue` reads from Arrow primitive arrays for Date32,
  Date64, Time32/64, Timestamp and Duration, including all supported units and
  timezone-bearing Timestamp arrays. Values retain their physical unit and
  NULL behavior while avoiding the previous whole-column cast for every row;
- batch updates for global numeric aggregates;
- borrowed persistent lookup for Arrow row-encoded `GROUP BY` keys. Existing
  groups no longer allocate a key on every row; only a new group copies its row
  bytes. Float/`CellValue` equality and Aggregate Spill/remap behavior are
  unchanged;
- local Parquet reader clones for one query/file share a lazily opened
  descriptor and use positional reads. Overlapping or adjacent logical ranges
  are coalesced into exact physical spans; results preserve request order
  through zero-copy `Bytes` slices, and ranges with a gap are never merged.
  Both the opened descriptor and current canonical path are validated before
  and after I/O, so replacement, deletion, and truncation fail as object
  changes. Cancellation is checked between chunks no larger than 4 MiB, and
  the query waits for an admitted blocking job to finish before cleanup. S3
  conditional reads are unchanged;
- fixed Parquet providers, including Native snapshots, may group two through
  four row groups per reader while preserving the planned lane count. Dynamic
  registered Parquet and LIMIT scans stay at one row group per reader. One
  query-local immutable plan per file shares metadata, projection, the filter
  description, and reader template; mutable Arrow `RowFilter`, builder, and
  stream state stay per reader. Exact strict-schema scans without Hive or
  dictionary columns use a zero-copy identity alignment fast path;
- query-scoped Parquet dictionary grouped-Aggregate decoding, with bounded
  internal batch enlargement capped at 64K and conservative memory fallback;
- a private Parquet decode hint for direct, unfiltered, fusable inputs at
  matched global Join-plus-Aggregate boundaries, using 64K at configured
  concurrency one and 32K above one without changing public result batches;
- a bounded UInt32 dictionary group-key direct-slot fast path, with the generic
  Arrow row-key implementation retained as fallback;
- an internal `DenseDictionaryBatchAggregate` path for one or two
  `Dictionary<UInt32, Utf8|Binary>` group columns with at most 16 Cartesian
  slots and non-DISTINCT `COUNT(*)` or direct Decimal128 SUM aggregates; its
  raw slots canonicalize duplicate logical values and both forms of NULL;
  Decimal SUM preserves input-order prefix overflow semantics with lazy `i256`
  promotion, and fixed plus variable-width transient payloads are reserved
  before allocation or cloning;
- strict-schema Parquet RowFilter fusion for complete conjunctions of at least
  three supported fixed predicates, with conservative per-column fallback and
  a narrowly admitted Arrow Auto threshold of 16 for projected Decimal payloads
  outside the predicate columns; the same narrow sparse-Decimal shape may use
  Arrow's decoded predicate cache only when at least two predicate columns are
  also projected and its full estimated array and bookkeeping footprint is
  reserved. A single projected/predicate overlap explicitly uses a zero-byte
  cache because retaining a whole decoded column for sparse survivors regressed
  Q6. Reservation failure and all non-admitted RowFilter shapes also use a
  zero-byte cache; a general budgeted cache policy remains future work;
- strict-schema Parquet Exact v1 lowering for complete, safely typed AND trees
  of direct comparisons and `IS [NOT] NULL`. The optimizer removes the SQL
  residual only after complete optimizer and provider capability checks;
  unsupported types or shapes retain the residual path, while a later schema
  remap or reader mismatch returns a structured error rather than silently
  applying a partial predicate. Exact Q6 scans project only `discount` and
  `extendedprice` instead of four columns, exact `COUNT(*)` uses a true
  zero-column projection, and query runtime filters are not installed on an
  exact scan. Native snapshots reuse this Exact path through their fixed
  strict-schema Parquet provider, with execution-time schema and capability
  revalidation before a residual-free request is delegated;
- compatibility reading for query-neutral `.rdbpred` companions already
  declared by Native Parquet snapshots. Production imports no longer create
  these companions. The retained format stores range-indexed
  row-group/column blocks for `Int8/16/32/64`, `Date32`, and
  `Decimal128(precision <= 18)`, and the first scan slice evaluates complete
  exact AND trees of direct comparisons and `IS [NOT] NULL` for `Int64`,
  `Date32`, and those Decimal values. Narrower integer blocks currently use the
  fallback path. Sidecar I/O runs in the scan lane, and the reader fetches only
  its header/directory and required blocks, intersects the exact bitmap with
  page-index selections, and bypasses Parquet `RowFilter` only for fully
  covered row groups. When Parquet still reads the projected payload,
  predicate blocks must be at most half the compressed predicate-only Parquet
  bytes they avoid; zero-column scans may admit up to twice those bytes. A
  separate direct full-projection compatibility path requires complete
  predicate-plus-output blocks and at least a 2x byte advantage: sidecar bytes
  must be no more than half the corresponding compressed Parquet bytes. It
  atomically plans bounded chunks of up to four row groups, decodes only
  selected values into the table's projected schema, and falls back before
  sidecar range I/O when any row group is uncovered or the whole chunk misses
  the gate. Unsupported or costlier shapes, missing eligible blocks, or
  resource admission fall back to the ordinary Parquet exact path;
  declared-file corruption, binding mismatch, and object changes fail the
  query;
- query-wide Exact filtered LIMIT accounting shared by all row-group tasks.
  Workers claim rows with an atomic compare-and-swap only after a complete
  RowFilter batch is produced, cap the shared claim at `LIMIT + OFFSET`, and
  slice only the terminal batch. Budget exhaustion ends remaining tasks
  naturally without cancelling the query, and workers recheck after acquiring
  an I/O permit to avoid unnecessary decoding. Best-effort filters never use a
  reader-level limit; unfiltered scans retain raw planning-time decrementing,
  and the upper Limit operator remains the final correctness guard;
- parallel single-file CSV scans now feed parser lanes from one shared bounded
  morsel queue, reuse one Arrow decoder per lane and admit decode work through
  the engine-wide scheduler. The producer derives a private morsel target from
  the query object snapshot: approximately four morsels per scan task, with a
  1 MiB adaptive floor and the configured target as the ceiling. Sub-MiB user
  settings remain exact; ordered source reads, decompression, quote-aware
  framing, reservations and cancellation semantics are unchanged;
- safe observed smaller-side selection for eligible Inner joins, including
  residual-column remapping and restoration of the original output order;
- engine-wide per-batch Aggregate compute admission and synchronized
  concurrency starts in the comparison runner;
- typed multiset checksums, sampled process peak RSS, per-operator metrics, and
  a separate DuckDB 1.5.4 comparison harness.

No source migration is currently required. Benchmark consumers should treat
new JSON fields as additive and continue accepting unknown fields. The active
v0.7 release gate is the documented ClickBench functional manifest;
development SF1 comparison numbers below are historical diagnostics and are
not release claims.

Existing Native tables require no compatibility migration. Table-manifest v1
and v2 snapshots without `.rdbpred` continue through the Parquet path. A
snapshot that already declares a companion remains readable and retains its
fail-closed identity, checksum, schema, row-group-layout, reservation, and
query-memory checks. RustDB does not synthesize a sidecar during open or query,
and production re-import or rewrite also does not create one. New imports
therefore use only the Native Parquet segment plus ordinary bounded metadata.

`QueryMetricsSnapshot` also adds query-admission wait and additive SQL parse,
table-function preparation, bind, provider preparation, optimizer and Native
verification durations, plus the count of Native segments fully verified by
the query. `parquet_reader_builds` counts constructed readers and
`parquet_local_file_opens` counts lazy local descriptor opens.
`parquet_narrow_decimal_columns` counts file-columns using the query-local
Decimal64 decode hint before exact widening to the public Decimal128 schema.
The additive `native_predicate_sidecar_*` counters report legacy-companion
range-read bytes, rows evaluated/selected, exact `RowFilter` bypasses,
ordinary-Parquet fallbacks, direct full-projection bypasses/rows, and safe
full-projection fallback row groups. They are diagnostics only and do not
alter the benchmark timing boundary. A newly imported production table is
expected to report zero for them.
Admission and
top-level `Session::execute` parsing happen before the existing `elapsed`
clock. A `CREATE TEMP VIEW` query retained as source text is parsed inside its
command context and contributes to both `elapsed` and `sql_parse_time`;
executing a prepared AST reports zero SQL parse time. Native verification is
nested inside provider preparation. Native table
load/commit now seeds query integrity checks from process-local fingerprints
produced by the mandatory full snapshot verification; fingerprints are not
persisted and an identity change still forces full verification. No Rust API
call-site migration is required because the metrics snapshot is non-exhaustive.

Ordinary `Session::execute` now parses its single statement once and reuses the
AST for command dispatch, table-function preparation and binding. This is an
internal preparation-path optimization; prepared statement and public API
behavior are unchanged.

Local Parquet snapshots and metadata keys now include device, inode, size,
mtime, and ctime on Linux and macOS. Range reads validate both the shared open
descriptor and current canonical path before and after I/O, closing the
same-size/restored-mtime cache and snapshot gap. Atomic replacement, deletion,
and truncation therefore fail. Local CSV validates its opened descriptor before
reading and before EOF, so an in-place mid-read mutation also fails; unlike
Parquet, an atomic CSV replacement after open may complete through the
consistent old descriptor. S3 conditional semantics are unchanged.

Fixed multi-segment Parquet providers now overlap a bounded query-local prefix
of footer and eligible page-index loads. The preloader shares the existing
pruning and query-memory budgets, transfers entries to planning once, and
falls back to the previous lazy path on resource admission failure. LIMIT and
non-fixed providers remain lazy. This is internal and requires no API change.

`QueryMetricsSnapshot` now separates cumulative compute-permit wait,
full-queue backpressure, execution-barrier wait, CSV source I/O, CSV framing,
and CSV decode compute time. `scheduler_wait` is retained but remains a mixed
legacy counter. Benchmark reports now use contract
`rustdb-v07-benchmark-v3` and emit `multiset-sha256-v2`; v1 and v2 reports
require explicit `--allow-historical` validation and are never gate evidence.
A report must never mix checksum versions. Current reports include
checksum backend/time and absolute plus baseline-delta RSS; their summaries are
recomputed by the validator. Until both engines use one common native result
consumer, current reports are limited to 65,536 result rows and should normally
use small aggregate outputs. The DuckDB comparison worker requires pinned
PyArrow 24.0.0, consumes Arrow batches and uses one persistent connection per
executor thread against a shared temporary file database. It verifies effective
thread, memory and cache settings before accepting work.

Contract v3 adds the persisted Native setup boundary. A canonical manifest
freezes explicit local Parquet paths, sizes and SHA-256 values; both workers
verify those bytes before and after receiving the same sorted, wildcard-free
CTAS statements. Every import result is fully consumed before an atomic setup
marker is published. The workers are then closed and reopened on the same
database; all ten Native runs must echo the same setup id. Reports record load
RSS, workspace baseline/peak/final bytes, the first post-reopen round,
steady-state p50 and the exact ten-round amortized load formula. The workspace
contract rejects limits above 2x selected source bytes plus bounded table and
harness metadata. This is benchmark tooling only and does not change the
public Rust API; an S3/MinIO Native source gate still needs an object-version
identity extension.

The first bounded Native SF1 contract-v3 run completed successfully with eight
explicit source files and matching ten-round checksums. Both engines stayed
well below the 2x workspace ceiling. RustDB used less setup and query RSS, but
its steady Native Q6 p50 was about 5.2x slower and its ten-round amortized time
about 3.1x slower than DuckDB 1.5.4, so this is reliability evidence and the
performance gate remains open. No Rust API migration follows from the
benchmark contract.

A later one-run Native Q6 diagnostic exercised `.rdbpred` before its cost
gate. Checksums matched, terminal reservation was zero, and RustDB files stayed
at 1.53x source bytes, but partial sidecar coverage read 9.72 MiB per round
while remaining row groups still used Parquet `RowFilter`. RustDB steady p50
was 98.690 ms versus DuckDB 1.5.4 at 5.244 ms; load time was 8,505.498 ms
versus 1,473.883 ms. This rejects unconditional sidecar use as a performance
path.

One subsequent contract-valid SF1 Native Q6 comparison tested direct
full-projection with four threads, a 2 GiB limit, concurrency one, zero warmup,
and ten alternating rounds. Checksums matched. RustDB/DuckDB steady p50 was
46.241733/5.100391 ms, query peak RSS was 60,604,416/161,484,800 bytes, and
final storage was 346,305,523/261,894,548 bytes from 226,756,631 source bytes.
The first RustDB query read 19,415,612 sidecar bytes, evaluated 3,101,247 rows,
selected 59,313, and directly projected 25 row groups/59,313 rows; the remaining
Parquet work built eight readers, range-read 22,837,761 bytes, and spent
37.36 ms decoding. Direct projection improved the earlier unconditional
sidecar diagnostic, but regressed the retained no-sidecar RustDB p50 of
25.810 ms, so it is not a performance win or release-gate result.

The rejected comparison forced one row group per morsel and admitted direct
projection at a 2:1 threshold. A follow-up removed that task amplification and
tested bounded multi-row-group projection with the normal 1:1 policy in a
RustDB-only SF1 Q6 diagnostic. It still measured a 45.250696 ms steady p50 and
346,311,971 bytes of final storage. The retained no-sidecar baseline was
25.810 ms and about 268.37 MB, so batching row groups did not recover the
latency or storage regression.

Production Native imports therefore no longer create predicate sidecars. The
decoder remains only for compatibility with snapshots that already declare
one. On that compatibility path, full projection plans a bounded chunk of up
to four row groups and requires sidecar bytes to be no more than half the
matching compressed Parquet bytes, i.e. an estimated 2x byte benefit, before
range I/O. Predicate-only and zero-column legacy reads retain their separate
half-byte and 2x gates described above.

One RustDB-only default-off verification used 4 threads, a 2 GiB limit,
concurrency one, batch 8,192, zero warmups, and ten SF1 Q6 rounds. Checksum
prefix `a973df...` matched. Steady/overall p50 was 23.371626/23.4836615 ms,
first query was 30.992136 ms, load was 4,360.106355 ms, and query peak RSS was
58,449,920 bytes with a 28,930,048-byte delta. Final/peak storage was
268,366,819/268,368,524 bytes from 226,756,631 source bytes (1.1835x); sidecar
counters, terminal reservation, and Spill were all zero, and every round used
four lanes. Relative to the rejected bounded-sidecar diagnostic, steady query
time, load time, and final storage fell 48.35%, 48.29%, and 22.51%. The
ephemeral report is
`/tmp/rustdb-v07-native-sf1-default-no-sidecar.json`. Its steady p50 is about
9.45% below the retained 25.810 ms no-sidecar formal result, but the protocols
differ, so this is implementation verification rather than a DuckDB comparison
or release-gate claim.

A Native LZ4 segment experiment was rejected. In its one-sample diagnostic,
LZ4 occupied 356,070,711 bytes (1.57x source) versus ZSTD's 268,350,462 bytes
(1.18x), loaded in 4,659.67 ms versus 4,354.19 ms, and had a steady query time
of 33.468 ms versus 30.944 ms. The checksums and resource invariants matched,
but neither storage nor latency improved, so Native retains ZSTD. This is a
non-gate, one-sample development diagnostic.

Current gate reports must also record host `system`, OS `release`, `machine`,
`cpu_model`, logical CPU count and total physical memory, plus an explicit
`storage_medium` of `local-nvme` or `minio`. Each engine must provide a 64-digit
worker `build_id` in its hello and repeat the same id in every run. A report
that omits these provenance fields, changes a worker id within the report, or
labels another storage medium cannot be used as current gate evidence.

The retained internal limits are evidence-driven rather than public tuning
contracts. A 131K grouped decode experiment was rejected because its small
latency improvement increased RustDB peak reservation and RSS beyond the
accepted 64K envelope. A unique-dense Join specialization was likewise removed
after passing focused correctness tests but missing its latency threshold.
A slot-id-only grouped-Aggregate prototype was also withdrawn because physical
dictionary slot ids can change across batches; the retained dense batch path
resolves one representative logical key for each used slot before persistent
state updates.
The private Join-input hint reduces the SF1 diagnostic from 917 to 123 input
batches and reaches near latency parity at concurrency one. At concurrency two,
the configured 32K path supersedes the earlier 64K observation and, in one
development sample, is about 9.4% faster in throughput than DuckDB with lower
absolute RSS. The required 25% throughput-per-RSS advantage and the complete
release gate are still open.

The single-column Parquet UTF-8 Join path has a separate Arrow-v2 diagnostic.
With the same 300,000 scanned rows, 9,746,760 scanned bytes, 150,000 candidate
pairs, one result row, zero materialized Join output bytes and zero Spill,
RustDB query time changed from 87.608052 to 34.766580 ms and Join time from
84.872444 to 31.238821 ms. Peak reservation changed from 66,701,402 to
43,231,386 bytes and sampled RSS delta from 66,772,992 to 36,814,848 bytes.
The result checksum remained
`ac71a984d60398da739aa1c3984d6bc840b1199f03b19bc85e5d1367df64f13a`.
The before/after RustDB binary build ids are
`d8b773aa9dcc72ed545194ecd1adc372910bff1d932f9a614de745fe54557368`
and `4ffaae8f4b47de30bb404e5c0f32dc9d3b77b4125fda730e1adacdfa660d6d6d`;
the corresponding DuckDB worker image build ids are
`722049de76578a6500b37c9ea131accf246ff42415654a6ca37f87d26bbb65ae`
and `93486ec210efe9447c63a51e82d171d441ae9f70b4799543a94c73c73abb03be`.
In the current after report DuckDB measured 11.636041 ms, leaving RustDB about
3.0x slower. Because this is one warmup and one measured sample, and the DuckDB
worker image changed between reports, it is diagnostic implementation evidence
only: cross-round DuckDB timing is not an A/B and no release gate is claimed.
These two saved reports also predate the mandatory host and `storage_medium`
fields, so their build ids preserve binary provenance but do not make them
current-contract gate evidence.
The current-contract composite-key diagnostic joins the same SF1 customer
Parquet snapshot on `(c_name, c_mktsegment)`, uses four threads, 2 GiB and
concurrency one, and returns one aggregate row. With the same checksum,
300,000 scanned rows, 14,367,792 scanned bytes, 150,000 candidate pairs, zero
materialized Join output bytes and zero Spill, RustDB query/Join/TTFB changed
from 125.654571/119.157952/120.876749 ms to
35.982813/33.404117/35.234263 ms. Peak reservation changed from 74,150,780 to
50,504,130 bytes and sampled RSS delta from 61,210,624 to 38,731,776 bytes.
The RustDB build ids are
`ca475b92196603dc77bac01fc4803653ce65acbba7f1dc593cf4ca2d721b9b1e`
and `07ce433faee13a7e480708e086ed1486750207a4d8d1af62c7de2f877ca6d4ed`.
The same DuckDB 1.5.4 worker build
`93486ec210efe9447c63a51e82d171d441ae9f70b4799543a94c73c73abb03be`
measured 13.660192 ms in the after round, leaving RustDB about 2.63x slower.
This is one warmup plus one measured development sample, not p50 or release
gate evidence.

Replacing the Composite table's per-distinct-key `Vec<u8>` allocation with the
checked contiguous arena produced RustDB build
`5aac40deac7773ce187c12bd5e5b31d0719ced81dbd0a332f596e57edc59bb15`.
Under the same current-contract fixture, RustDB query/Join/TTFB measured
23.656896/21.379134/23.104320 ms. Peak reservation was 50,410,112 bytes and
absolute peak RSS was 98,238,464 bytes, both slightly below the preceding
50,504,130 and 98,631,680 bytes. The worker began from a lower RSS baseline,
so its 57,745,408-byte RSS delta is not compared as an improvement. Checksum,
scanned rows/bytes, candidate pairs, zero Join output bytes, zero Spill and
zero terminal reservation remained identical. The same DuckDB 1.5.4 worker
build measured 22.127124 ms in this round, about 1.07x faster than RustDB; its
large one-sample variance versus the preceding round reinforces that this is
development evidence, not p50 or release-gate evidence.

A later current-contract experiment extended batch-bound probing into normal
`ProbeCursor` execution and UTF-8 keys. With RustDB build
`0c699a00cb9447cb8982024a27eaa3b6ef75c54bd7b75b8428e61d071372ade7`,
query/Join time measured 41.538430/38.147317 ms, peak reservation was
43,231,386 bytes and RSS delta was 43,773,952 bytes. DuckDB worker build
`93486ec210efe9447c63a51e82d171d441ae9f70b4799543a94c73c73abb03be`
measured 11.896331 ms. Checksum, one-row result, 300,000 scanned rows,
9,746,760 scanned bytes, 150,000 candidate pairs, zero Join output bytes and
zero Spill were unchanged. Relative to the preceding accepted RustDB
34.766580/31.238821 ms diagnostic, query time regressed about 19.5% and Join
time about 22.1%. The experiment is rejected and its production expansion has
been withdrawn; it does not alter any gate. The earlier `Int64`/`UInt64`
`FixedProbe` used only by the direct Join-Aggregate sink remains and was not
part of the withdrawal. The retained temporal direct-read slice is independent
and this UTF-8 query does not exercise it.

The current fully accounted dense grouped-Aggregate single sample measured
RustDB group/query/TTFB at 27.761265/27.747892/26.954660 ms and 36.021413
queries/s, with 53,981,184-byte RSS from a 36,610,048-byte baseline and
39,033,799-byte peak reservation. It used 98 batches, four lanes and no Spill.
DuckDB group/query time was 24.272083/24.064193 ms at 41.199595 queries/s, with
71,041,024-byte RSS from a 68,567,040-byte baseline. RustDB is 14.4% slower,
uses 24.0% less absolute RSS and has about 15.1% higher throughput/RSS. The 25%
efficiency and overall gates remain open. Decimal prefix tests pass 5/5, Dense
workspace/fallback tests pass 3/3 and stream integration passes 1/1. The older
19.648516 ms sample is rejected because it predates correct Decimal
intermediate-overflow semantics; the subsequent 28.132285 ms full-`i256` and
26.303008 ms incompletely-accounted lazy-narrow steps are superseded.

The exact local range-coalescing implementation passes its four focused tests.
Its Q6 single sample measured RustDB at 35.581684 ms, with 46,252,032-byte
sampled RSS, 4,103,297-byte peak reservation and 126.661064 ms cumulative Scan;
DuckDB measured 23.608336 ms in the same round. The checksum remained
`f03b918823c1a0a48240522d16a3703104a98be48d179823a31535ba864120e9`.

The fully reserved predicate-cache slice passed its original 5 cache-planner
and reservation tests, 11 RowFilter tests and 3 real-reader tests. Its Q6 single
sample measured RustDB at 46.080768 ms, with 65,486,848-byte sampled RSS,
21,913,180-byte peak reservation and 161.809458 ms cumulative Scan. DuckDB
measured 41.378470 ms with 91,873,280-byte sampled RSS. The checksum remained
`f03b918823c1a0a48240522d16a3703104a98be48d179823a31535ba864120e9`
and no Spill occurred. Both engines showed pronounced timing variation in this
round, so these figures are retained only as same-round relative and resource
evidence: they neither establish an absolute RustDB regression nor pass the
latency gate. RustDB's throughput per sampled RSS was about 26.8% higher in
this one round, but the complete efficiency and release gates remain open. This
sample is now superseded by the Exact v1 result below; it remains only noisy
historical resource evidence. The cache planner now passes 6 focused tests,
including the rule that disables caching for a single overlap column.

The current retained Exact v1 Q6 single sample measured RustDB
group/query/TTFB at 30.237324/30.225241/28.883863 ms and 33.071710 queries/s.
RustDB sampled RSS was 42,278,912 bytes from a 40,914,944-byte baseline, peak
reservation was 3,438,372 bytes, and cumulative Scan time was 101.815122 ms.
Scan emitted 114,114 rows in 49 batches, reported 3,661,056 logical Arrow
bytes, used four lanes and did not Spill. DuckDB measured
20.275967/20.031091/19.989591 ms at 49.319473 queries/s, with 93,515,776-byte
RSS from an 82,305,024-byte baseline. The checksum remained
`f03b918823c1a0a48240522d16a3703104a98be48d179823a31535ba864120e9`.
Relative to the retained range-coalescing RustDB sample, query time improved
about 15.0%, cumulative Scan time improved about 19.6%, and reported logical
scan bytes were nearly halved. RustDB is still about 1.51x slower than DuckDB
in this single round, so neither the latency nor complete release gate is
claimed.

The later bounded row-group reader diagnostic reduced reader builds from 49 to
20. Its first query changed from 65.066 to 37.415 ms and cumulative steady Scan
time from 90.587 to 83.197 ms, while steady wall time was effectively unchanged
at 26.459 versus 26.518 ms. Adding the query-scoped shared local descriptor then
measured 31.477 versus 36.500 ms for the first query and 26.479 versus
27.130 ms steady, with 15 local opens; cumulative Scan was slightly worse within
the noise of this sample. Both steps preserved the checksum, row and byte
counts, four-lane execution, zero Spill, and zero terminal reservation. These
are RustDB-only, non-gate, one-sample diagnostics, not a DuckDB comparison or a
release performance claim.

One subsequent current contract-v3 comparison used the same eight-file SF1
source (226,756,631 bytes), four threads, 2 GiB, concurrency one, zero warmups
and ten alternating RustDB/DuckDB rounds. RustDB versus DuckDB 1.5.4 measured
4,432.728/1,643.654 ms load, 47.125/19.642 ms first post-reopen,
25.239/5.437 ms steady p50 and 472.768/171.445 ms amortized. RustDB setup/query
peak RSS was 362,070,016/59,305,984 bytes versus
636,338,176/159,887,360 bytes, and final storage was 268,324,750 versus
262,943,124 bytes. The checksum matched; RustDB used four lanes, no Spill and
zero terminal reservation. The implementation is retained for its lower setup
work and resource use, but steady latency remains 4.64x DuckDB and the v0.7
performance gate remains open.

Parquet query metrics now distinguish coalesced range I/O, decoder active polls,
RowFilter work, and schema alignment. These are additive cumulative lane values:
RowFilter compute is contained within decoder active time, and neither should be
read as wall-clock latency. Contract v3 accepts the fields when present so older
retained reports remain readable.

A later 65,536-row private decode experiment for Native Exact Q6 Aggregate was
rejected and removed. Against an otherwise identical instrumented binary it
reduced RowFilter evaluations from 733 to 95, but the two-sample RustDB-only
median regressed from 33.457 to 41.171 ms and peak reservation increased from
about 3.7 to 19.9 MiB. Checksums, 114,160 scan rows, 3,662,528 logical bytes,
49 output batches, zero Spill, and zero terminal reservation matched. No DuckDB
rerun was made because the RustDB candidate lost its own A/B.

Native segment staging now computes its SHA-256 incrementally over successfully
written bytes. Commit/load retains the mandatory complete-file SHA, Parquet
footer, and row-count verification before Catalog visibility. One sequential
RustDB-only SF1 import A/B changed load from 4,214.063 to 4,129.342 ms, about
2.0%, with the same reopened Q6 checksum, zero Spill, and zero terminal
reservation. This structural duplicate-read removal is retained, but the small
one-sample gain did not justify another DuckDB comparison.

A two-stage sparse-Decimal RowFilter experiment evaluated the first SQL
predicate as a single-column stage before fusing the remaining predicates. Its
focused correctness checks passed, but one matching Q6 sample measured RustDB
group/query/TTFB at 37.183236/37.171321/36.409373 ms and 26.893840 queries/s,
with 47,427,584-byte RSS from a 45,264,896-byte baseline, 3,355,582-byte peak
reservation and 132.837240 ms cumulative Scan. DuckDB group/query time was
20.611583/20.348643 ms. The checksum matched, four lanes were active and no
Spill occurred. Compared with the retained Exact v1 sample, RustDB query time
regressed about 23.0% and Scan time about 30.5%; the experiment was removed and
these numbers do not describe current behavior.

The retained adaptive CSV morsel slice passed its four boundary tests and a
single direct-CSV comparison using the same 64 MiB file, four threads, 2 GiB
memory, concurrency one and batch size 8192. Relative to the immediately prior
instrumented RustDB sample, morsels increased from 8 to 16, peak parser lanes
from 2 to 3 and peak active lanes from 3 to 4. Query time changed from
41.106117 to 32.607150 ms, cumulative Scan from 124.829488 to 97.684621 ms and
sampled peak RSS from 126,865,408 to 118,009,856 bytes; peak reservation rose
from 26,815,456 to 29,105,600 bytes. The checksum remained
`9bc997b47d1bd0f651e9946f021eb2157ea32331bd76cc35f2b805f1fef510d5`.
DuckDB 1.5.4 measured 26.075454 ms in the retained round, so RustDB remains
about 25.0% slower on this query. These are one-sample development diagnostics,
not a release performance claim.

The newer Arrow-v2 attribution path separates source I/O, framing, decoder
compute and queue/compute/barrier waits. It also replaces the local CSV
`ReaderStream` adapter with a directly metered bounded file reader; S3 and
compressed input keep their ordered streaming path. In one isolated A/B,
RustDB changed from 41.355082 to 36.555396 ms, framing from 13.587086 to
5.003133 ms and sampled peak RSS from 85,983,232 to 56,930,304 bytes, with the
same result checksum. DuckDB showed large same-round variance, so this is not a
cross-engine performance claim.

A direct CSV framer-append experiment passed focused correctness and cleanup
checks but was removed after the matching sample regressed RustDB query time by
21.5% and cumulative Scan by 18.9%, while increasing peak reservation. Its
39.617107 ms RustDB result versus DuckDB's 26.046691 ms is rejected evidence
and does not describe current behavior.

`FilteredGlobalNumeric32K` was rejected and fully removed, including its
dedicated tests. Its RustDB group/query/TTFB sample was
40.738919/40.726586/40.029810 ms versus DuckDB 20.504539 ms, with
47,820,800-byte RSS from a 41,922,560-byte baseline, 12,999,990-byte peak
reservation, 143.048457 ms cumulative Scan and 11,918,816 scanned bytes. These
numbers do not describe current behavior.
