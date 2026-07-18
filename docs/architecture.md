# Architecture

The query path is deliberately direct:

```text
SQL parser -> binder/catalog -> logical plan -> rule optimizer
           -> physical pipelines -> bounded RecordBatch stream -> caller
```

Arrow `RecordBatch` is the only public exchange batch. Internally each batch is
a `BatchEnvelope` carrying its query-memory lease. Fused Scan/Filter/Projection
pipelines claim independent tasks across
`min(compute_threads, runnable Scan tasks)` lanes;
Aggregate, Join build, Sort, and scalar-subquery materialization remain
pipeline breakers; Window adds a shared Sort plus partition spool, while
`UNION ALL` Append remains streaming. Bounded envelope queues apply consumer
backpressure.
Object-store work is asynchronous and source fan-out is capped by
`io_concurrency`. `compute_threads` is an upper bound, not a promise that every
plan has that many simultaneously runnable lanes. The query scheduler also
reserves at least 32 MiB of query budget per configured lane, so a 64 MiB
query uses at most two lanes and a 128 MiB query at most four. This query-wide
cap prevents nested Scan/Join queues from consuming every memory credit before
a blocking operator can switch to Spill; a 1 GiB query can still use all 18
requested lanes. Without `ORDER BY`, batches from multiple lanes have no
stable output order.

Literal SQL `LIKE` patterns are compiled once per evaluated batch instead of
being expanded to a repeated Arrow string column and tokenized for every row.
Exact, prefix, suffix, contains, and ordered multi-`%` forms use direct string
matching. Patterns containing `_` retain Unicode-scalar semantics through one
reused two-row dynamic-programming workspace. Arbitrary single-character
`ESCAPE`, NULL propagation, `NOT LIKE`, and lazy trailing-escape errors are
preserved; dynamic pattern columns keep the generic per-row path.

Expression projection always passes the input row count to Arrow, including an
empty terminal projection. Metadata-only `COUNT(*)` therefore keeps a true
zero-column batch after a residual Filter without losing its number of rows.

Every asynchronous worker belongs to the query's `TaskGroup`, including the
public-stream producer, Scan lanes, Aggregate partial lanes, Hash/Grace Join
workers, Sort run generators, and Window partition workers. The first worker
error or panic records one terminal failure and cancels its siblings. Normal
completion and error paths
wait for every registered task to unwind before removing query Spill files;
consumer abandonment starts the same convergence through a background reaper,
so `QueryResult::cancel()` remains a non-blocking signal.

Window input-key evaluation, partition-state updates, replay evaluation, chunk
planning, and output materialization acquire short-lived engine-wide compute
permits. A permit is released before cancellation-aware memory waits, Spill reads or writes,
stream yields, and channel sends. Window queries therefore share the same
four/eight-thread CPU budget as Scan, Aggregate, Join, and Sort without holding
compute capacity while waiting on I/O or downstream backpressure.

Scan lanes reserve a projected-schema decode credit before polling a decoder.
Filter/Projection and blocking-operator outputs reserve conservative workspace
before Arrow kernels run, then transfer that reservation directly into the
output `BatchEnvelope`. Per-lane input queues remain single-slot, while shared
fan-in queues are bounded in proportion to their active lane count. The public
handoff remains single-slot. Every queued envelope retains its memory lease;
when downstream is slow, cancellation-aware memory waiters resume on lease
release instead of treating temporary queue pressure as out-of-memory.

Dynamic registered Parquet scans and all scans with LIMIT keep one row group
per reader. Fixed Parquet providers, including Native snapshots, may instead
place two through four row groups in one reader when that reduces setup work
without reducing the planned lane count. A query-local immutable decode plan
is shared per file: it owns metadata, projection, the filter description, and
the reader template. Each reader still constructs its own mutable Arrow
`RowFilter`, builder, and stream. In the exact-schema, no-Hive,
no-dictionary case, schema alignment validates nullability and returns the
existing arrays without copying or remapping them.

The Parquet I/O semaphore covers only the poll that obtains the next batch.
It is released before the batch is yielded to a downstream queue or public
consumer, so slow output backpressure cannot pin an object-I/O slot needed by
another scan lane.

For one CSV object, one ordered producer performs a conditional GET, detects raw,
gzip, or zstd input from magic bytes, and decompresses concatenated gzip
members or zstd frames. A quote/escape-aware framer emits morsels only at full
record boundaries, including across source chunks and quoted newlines, then
places them on one bounded shared parser queue. The header is removed once
before distribution. Local files use a directly metered bounded async reader;
S3 keeps its ordered object stream and never issues unsafe parallel ranges.
Multiple matched CSV files instead remain independent file tasks.

A Native query pins one immutable manifest snapshot, verifies its segment
identity, and presents those segments through a fixed strict-schema Parquet
provider. It resolves and registers the exact object identities before checksum
verification, then reuses those same sources for lazy footer and range reads;
a same-size path replacement therefore cannot become a newly accepted query
snapshot between verification and provider construction. Native advertises
Exact filter ownership only for predicates that are complete over the snapshot
schema. At execution it rechecks the prepared provider schema and both
capability declarations before delegating; any drift fails the query instead
of running without the removed SQL residual.

Production Native writes do not create `.rdbpred` companions. The format and
reader remain available for snapshots that already declare one. It is a
query-neutral row-group/column encoding, not a saved SQL predicate or result
cache: it records nullable fixed-width values once and can serve different
exact comparisons later. The current compatibility format indexes `Int8`,
`Int16`, `Int32`, `Int64`, `Date32`, and `Decimal128` with precision at most
18. A declared legacy companion remains part of the immutable snapshot and is
validated together with its Parquet segment; opening or querying a table never
synthesizes one.

The first query slice owns only complete, safely typed AND trees of direct
comparisons and `IS [NOT] NULL` over `Int64`, `Date32`, and
`Decimal128(precision <= 18)`. Narrow integer blocks are format-compatible but
currently fall back to Parquet evaluation. Sidecar I/O runs in the admitted
scan lane, not during morsel planning. Multiple conditions on one column are
evaluated in one borrowed-block pass without materializing decoded values. The
exact bitmap is intersected in row coordinates with any page-index selection;
covered row groups bypass Arrow's Parquet `RowFilter`, while uncovered or
unsupported predicates use the normal Parquet exact path.

Legacy-sidecar reads have separate conservative cost gates. When Parquet still
decodes the projected payload, required predicate blocks must be no more than
half the compressed Parquet predicate-only bytes they avoid; a zero-column
scan may admit up to twice those bytes because it avoids predicate decoding
entirely. A direct full-projection bypass additionally requires complete
sidecar blocks for the union of predicate and output columns and at least a 2x
byte advantage: required sidecar bytes must be no more than half the matching
compressed Parquet bytes. The reader may validate and range-read a bounded
chunk of up to four row groups atomically, then evaluates each row-group bitmap
and decodes only selected values into reservation-backed projected batches.
If any row group lacks coverage or the whole chunk fails the cost gate, it
falls back before sidecar range I/O and keeps the ordinary Parquet chunk.
Successful direct output increments
`native_predicate_sidecar_full_projection_bypasses` and
`native_predicate_sidecar_full_projection_rows`. Full-projection candidates
that safely decline report their covered row-group count through
`native_predicate_sidecar_full_projection_fallback_row_groups`; errors do not
increment that counter.

A rejected estimate is a normal fallback. Index, block, selection, and direct
output buffers retain query-memory leases until the public handoff releases the
engine-owned lease. Resource admission failure is also an optimization
fallback, but a legacy sidecar declared by the manifest that is missing,
malformed, checksum-invalid, bound to the wrong segment/schema/row-group
layout, or changes during the query is a query error.

Snapshot load and commit already perform full segment SHA-256 and Parquet
row-count verification. Their final process-local file fingerprints seed the
query verification cache, avoiding a duplicate full hash on first use. The
seed is scoped to the immutable snapshot and never persisted. Each query still
checks owner markers and the current file fingerprint, and an identity change
still enters the full-verification singleflight before any output is returned.
Segment staging computes SHA-256 incrementally over bytes accepted by its
quota-accounted writer. Commit/load still rereads and verifies the completed
file plus its Parquet footer and row count before Catalog visibility; the
incremental digest removes only the earlier writer-side duplicate reread.

`NativeStorageConfig` optionally applies hard engine and table quotas. A
prepared snapshot is checked before its staging directory is published, and a
transaction is checked again with all of its table writes aggregated before
the durable WAL/Catalog boundary. The engine limit measures the complete
Native root and reserves the next Catalog generation, WAL commit record, and
`CURRENT` update. Table accounting deduplicates inherited directories and
includes current, retired, staged, and published-but-uncommitted snapshots.
These hard limits are independent of, and do not replace, the existing steady
2x and write-peak 3x source-size governance.

For a fixed multi-segment provider without LIMIT, scan planning concurrently
preloads only the bounded prefix needed by the available I/O and compute lanes.
The prefix is at most `min(file_count, io_concurrency, target_tasks)`. Footer
metadata is always eligible; page indexes are included only when the predicate
supports them and Bloom pruning does not need to run first. Preload and lazy
planning share one query pruning budget, every decoded object keeps its normal
memory lease, and each preloaded entry is transferred to file planning once.
Admission failure drops partial state and returns to the lazy path;
cancellation, corruption, and object-identity changes remain query errors.

`CsvScanConfig` enables single-file parallel parsing by default and targets
8 MiB of decompressed data per morsel. The producer's source buffer, retained
framing buffer, queued morsels, and decoder outputs are charged to query memory;
a record larger than the target stays intact and must fit the available budget.
If it cannot fit, the resource error carries the object URI and the buffered
record's decompressed offset while preserving the underlying budget details.
LIMIT or cancellation stops the producer and parser lanes through the same
query `TaskGroup`. Projection, row-group statistics, Hive partitions, and
limits are applied before decoding where safe. Residual SQL filters always
remain in the plan, so a source hint can never silently change query results.

Blocking operators reserve retained state through a hierarchical engine/query
memory pool. When the multi-lane Aggregate path is eligible, every supported
aggregate (`COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`) produces lane-local partial
states and a final merge. On pressure Aggregate evicts only its largest victim
partition; survivors remain resident. Aggregate and Join choose a power-of-two
fanout from 2 through 256 using measured bytes, recursively repartition with a
new seed, and reuse one IPC stream per lane/partition/generation up to a 256 MiB
rotation target. Join uses an immutable shared build for parallel probe when it
fits, processes the largest spilled build partition first, admits lanes by
estimated footprint, and switches to external sort-merge after repeated seeds
do not shrink a partition. Duplicate-key groups are replayed in bounded chunks.
Sort creates lane-local memory blocks or LZ4 Arrow IPC runs and performs a
bounded k-way merge.

Primary in-memory Hash Join selects a key layout before building. Single
`Int64`/`UInt64`, UTF-8, and Binary keys use dedicated tables. Eligible tuples
of two or more exact-type flat Boolean, integer, UTF-8/Binary, Decimal128, Date,
Time, Timestamp, or Duration values use one Arrow row encoding per build chunk
and per probe batch. Distinct encoded build tuples are copied into one
geometrically grown byte arena and addressed by checked offsets from compact
hash entries; full encoded bytes are still compared after the hash match.
Duplicate row ids use a charged sidecar. The arena, hash-table allocation and
rehash replacement peak are all charged to the query reservation. Float,
Dictionary, nested,
existence-summary, Grace, and Spill paths retain the generic typed-key
implementation. A failed Composite reservation abandons the primary build and
enters the existing Spill path rather than allocating an uncharged fallback.
The binder and physical-plan verifier require both sides of every hash key to
have exactly the same post-coercion Arrow type, preventing incompatible wide
Decimal precision/scale pairs from being compared as raw unscaled integers.

`EngineConfig::execution` keeps the adaptive partition target, maximum
repartition depth, optional Spill write-amplification limit, and runtime-filter
memory budget together. `EngineConfig::csv_scan` separately controls
single-file CSV parallelism and the decompressed morsel target. Both public
configuration structs are non-exhaustive and have builder methods.

`UNION ALL` compiles to a streaming Append over type-aligned inputs. DISTINCT
set operations reuse the existing reservation-accounted Aggregate and Join
operators: `UNION DISTINCT` groups the appended rows, while `INTERSECT` and
`EXCEPT` de-duplicate both sides and use NULL-equal Semi or Anti joins.
`INTERSECT ALL`/`EXCEPT ALL` count NULL-equal whole rows on both sides and use a
bounded streaming `Repeat` operator for the resulting multiplicity; they never
collect the result. Query-level sorting and limiting run after the complete
recursive set tree.

Window execution sorts once for each shared `(PARTITION BY, ORDER BY, frame)`
specification. It detects partition boundaries from evaluated keys, writes a
bounded query-scoped partition spool, and evaluates independent partitions on
the query TaskGroup. Ranking functions and prefix/whole-partition aggregate
frames stream their results back as Arrow batches; peer-aware `RANGE` frames
retain only their required sidecar state. `QUALIFY` is a post-window Filter,
and hidden window columns are removed by the final Projection.

Parser-visible `RIGHT` and `FULL` joins use the same hash, Grace, and external
sort-merge machinery as existing equi joins. Match tracking is charged to the
query and preserves unmatched build rows across parallel and Spill paths.
Residual predicates are evaluated before a pair is marked as matched. `USING`
adds one visible key column followed by non-key columns from each side; the
visible key of a full join is a typed `COALESCE` of both inputs.

Aggregate DISTINCT uses a tagged `(group, aggregate-id, value)` key, so
multiple DISTINCT aggregates share one de-duplication stage without
conflating their inputs. In memory the key retains typed cell values; its
private Spill codec writes the group, aggregate tag, and value as bounded
binary fields. High-cardinality keys use recursive partitioning and the same
Spill governance as ordinary aggregate state.

The binder represents a one-level correlated reference explicitly as an
`OuterRef`. Planning temporarily introduces a `DependentJoin`; decorrelation
pulls equality keys and residual predicates into set-at-a-time join plans.
Scalar, membership, and existence subqueries become internal single-row,
Mark, null-aware anti, Semi, or Anti joins as appropriate. Direct top-level
`IN`/`NOT IN`/`EXISTS` filters are staged independently, including when they
appear later in an `AND` chain, so they lower directly to membership, Semi, or
Anti joins without retaining marker columns between filters. The physical-plan
verifier rejects every remaining `OuterRef` or `DependentJoin`, so execution
never falls back to evaluating a subquery once per outer row.

A scalar aggregate with only equality correlation keys is grouped directly on
the inner keys and left-joined once, avoiding an outer-domain distinct scan;
COUNT restores the empty-group value zero while the other aggregates remain
NULL. The Q21 `EXISTS value <> outer` plus late-filtered `NOT EXISTS` pair has a
narrow identity-gated rewrite: identical table-function specs share one
provider, one grouped scan computes all-row and late-row min/max summaries, and
one left join evaluates both existence conditions with SQL NULL semantics.

In the current alpha, parallel Aggregate and Sort, and the shared-build hash
Join probe, require more than one configured lane and at least a 64 MiB query
memory budget. The shared Join build must also fit within one quarter of that
budget. Smaller budgets use the serial or partitioned/Spill path; Grace Join
partitions may still run concurrently when multiple partition tasks exist.
Spill improves bounded-memory completion but does not make an individual row,
key group, decoder allocation, or minimum writer workspace arbitrarily small.

Ordinary grouped Aggregate keeps encoded and `CellValue` group indices in one
representation-specific persistent map. Arrow row-encoded inputs query the
encoded map with a borrowed byte slice; only a previously unseen SQL group
copies its row bytes. Float groups retain normalized `CellValue` equality, and
victim Spill moves and remaps the same owned index rather than creating a
second lookup table.

Spill files are query-scoped, mode `0600`, and removed after success, failure,
cancellation, or consumer abandonment.
Active Spill paths, long-lived IPC writers, partition indices, decoded Spill
batches, and merge state all retain reservations; file rotation or recursive
partitioning therefore cannot grow an uncharged in-memory path list. Spill I/O
runs on a fixed dedicated pool. Engine/query byte quotas and the configured
10%/1 GiB free-space reserve are checked during writes.

Each query directory contains a versioned `.rustdb-spill` marker and a private
`.rustdb-active` file whose exclusive advisory lock is held for the lifetime
of its `SpillManager`. Startup scavenging only considers UUID-named query
directories with a valid marker older than the configured TTL, then acquires
the activity lock without waiting and holds it through deletion. A held lock
preserves an active directory even if its marker is older than the TTL; process
exit releases the lock so a later Engine can reclaim the orphan. Unknown,
unmarked, or invalidly marked directories are never treated as RustDB orphans.
Valid legacy directories without `.rustdb-active` remain eligible for cleanup.

Remote backup snapshots and restore downloads use separate strictly named
`rustdb-remote-{backup|restore}-<uuid>` directories under the Spill root. Each
directory is mode `0700` and has a mode-`0600`, versioned sibling owner marker
whose content binds the operation kind and UUID; the live operation holds an
exclusive lock on that marker. Engine startup, and remote-restore preflight,
only reclaim an expired candidate after validating its name, directory type
and permissions, marker type/permissions/content, and acquiring the lock.
Unknown, forged, symlinked, fresh, or active paths are preserved.

The configured limit is an engine reservation budget, not a process-RSS hard
limit. Decoder credits and kernel workspace are conservative because exact
Arrow buffer sizes are not always known before execution; RustDB reconciles
the reservation as soon as a batch returns and rejects a single batch that
cannot fit. Filter/Projection kernels pre-reserve expression workspace.
Aggregate holds
leases for evaluated expressions and row-encoded keys, and Join key arrays keep
their lease for as long as the evaluated keys are retained. CSV sampling is
leased while inference runs, and the inferred query schema keeps a lease for
the prepared provider lifetime. Prepared Parquet providers retain leases for
every per-file schema, the merged physical/public schemas, and Hive partition
metadata (including partition strings) for their full lifetime. Workspaces use
conservative pre-reservations,
but allocator bookkeeping and temporary decoder buffers can still differ from
the reservation estimate.
Once a result batch is yielded, memory retained by the embedding caller is
outside the engine's ownership and budget; benchmark reports record RSS
separately.

Every production query uses a child of one Engine root memory pool, so
concurrent query reservations are constrained by one configured limit rather
than one limit per query. `Engine::memory_snapshot()` exposes the root's current
bytes, lifetime peak, and limit. The comparison runner samples it only after
all queries in a concurrent group have completed and released `QueryResult`;
worker-visible CPU affinity, cgroup CPU quota/cpuset, memory maximum, and
visible memory are recorded separately to prove both engines ran under the
same outer resource envelope.

Local contents rely on the operating-system page cache. The engine cache holds
Parquet schema/footer, page-index, and Bloom metadata under one shared byte
budget. Footer/page-index entries and Bloom entries maintain separate LRU
orders; Bloom entries are evicted first so broadly reused file metadata stays
resident. Keys contain URI, size, ETag, version, and—on supported local
platforms—the opened-file device/inode/mtime/ctime identity; Bloom keys also
contain row-group/column/range identity. Concurrent misses are singleflighted with
cancellation-aware waiters. A leader's query-local cancellation or memory
failure is retried by a healthy waiter rather than shared. No decoded data page
is retained.
File discovery has a separate Engine-memory-derived metadata cap and avoids a
second de-duplication set. Query snapshot entries retain memory reservations.
Parquet reads its fixed trailer and reserves a conservative footer expansion
before decoding; the resulting lease stays live through every row-group
morsel. Row-group pruning iterates indices directly instead of materializing a
file-sized selected list. Decoder and public output batches remain
structurally bounded by `io_concurrency`, channel capacity, and `batch_size`.

When enabled in `Auto` mode and useful for a supported predicate, Parquet deep
pruning loads page indexes or split-block Bloom filters through the same
conditional object reader. Page min/max and null counts produce an Arrow
`RowSelection`; a negative Bloom lookup removes a whole row group. Missing or
unsupported metadata is only a missed optimization, every residual SQL filter
remains executable, and invalid field combinations or malformed encoded
metadata are input errors. A legal legacy Bloom offset without its optional
length remains a bounded conservative skip. The
pruning pool is bounded per query and per file, and metadata reads, rejected
budgets, and eliminated pages/rows/groups are visible in query metrics.

Before any physical input stream is polled, RustDB walks every Scan in the
query (including dynamic views), captures each object's fresh identity, and
seals the query-wide snapshot map. Parquet footer/data ranges and the CSV
object GET are conditional on those snapshots. On Linux and macOS, local
Parquet readers for one query/file share one lazily opened descriptor and use
positional reads. Every range validates both that descriptor's
device/inode/size/mtime/ctime and the current canonical path before and after
I/O. In-place truncation, deletion, and atomic path replacement therefore fail
as object changes. Local CSV instead validates its opened descriptor before
reading and before EOF; an atomic replacement may finish through the consistent
old CSV descriptor. Neither path mixes file versions within one read.
The prepared providers' row/byte/file statistics are frozen into that query's
Scan nodes before join ordering and EXPLAIN; they never overwrite the shared
Catalog entry or another concurrent query's snapshot.
Cancellation races outstanding object requests, and S3 metrics include logical
resolution, metadata, snapshot, and data requests plus transferred body bytes.
CSV metrics separately report compressed/source bytes, decompressed bytes,
record-aligned morsels, peak parser lanes, source I/O, framing and decode
compute. Typed execution waits distinguish compute permits, full bounded queues
and barriers; their cumulative multi-lane totals may exceed query wall time.
Preparation metrics distinguish query-admission wait, SQL parse, table-function
preparation, binding, provider preparation, optimization, and nested Native
verification. Admission and top-level `Session::execute` parsing precede the
query-context elapsed clock, so the benchmark runner retains end-to-end wall
time as the latency authority. Parsing retained `CREATE TEMP VIEW` source runs
inside the command context, while prepared-AST execution has no SQL parse
phase.
Metadata metrics expose cache hits, misses, and time waiting on a shared
in-flight metadata load. `parquet_reader_builds` counts constructed Parquet
readers, while `parquet_local_file_opens` counts lazy local descriptor opens.
For strict fused sparse scans, eligible top-level Decimal128 predicate columns
with precision at most 18 are decoded through a query-local Decimal64 Arrow
schema hint and widened back to the public Decimal128 schema during alignment.
Unsupported physical layouts fall back to the normal Decimal128 reader. The
number of active file-column hints is exposed as
`parquet_narrow_decimal_columns`.
Execution metrics also expose adaptive Spill/repartitioning and runtime-filter
activity.

Registered CSV tables keep a stable logical schema between explicit refreshes
and require every newly discovered file to match the current physical CSV
schema. `REFRESH TABLE` re-infers the matched files and atomically installs a
new provider: surviving logical columns keep their previous order, removed
columns disappear, and genuinely new columns are appended in name order. Scan
projection and predicates are mapped by name when the refreshed file header
uses a different physical order.

The stable public surface is `Engine`, `Session`, registration options,
`QueryResult`, cancellation, and metrics. Catalog, logical/physical plan, data
source, scheduler, and operator types are crate-private so implementations can
evolve without coupling embedded callers to internal nodes.
