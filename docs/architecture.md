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

Every asynchronous worker belongs to the query's `TaskGroup`, including the
public-stream producer, Scan lanes, Aggregate partial lanes, Hash/Grace Join
workers, Sort run generators, and Window partition workers. The first worker
error or panic records one terminal failure and cancels its siblings. Normal
completion and error paths
wait for every registered task to unwind before removing query Spill files;
consumer abandonment starts the same convergence through a background reaper,
so `QueryResult::cancel()` remains a non-blocking signal.

Scan lanes reserve a projected-schema decode credit before polling a decoder.
Filter/Projection and blocking-operator outputs reserve conservative workspace
before Arrow kernels run, then transfer that reservation directly into the
output `BatchEnvelope`. Per-lane input queues remain single-slot, while shared
fan-in queues are bounded in proportion to their active lane count. The public
handoff remains single-slot. Every queued envelope retains its memory lease;
when downstream is slow, cancellation-aware memory waiters resume on lease
release instead of treating temporary queue pressure as out-of-memory.

Parquet morsels are file plus row group and may execute concurrently. CSV
morsels are files; one CSV remains sequential so quoted records cannot be split
incorrectly. Projection, row-group statistics, Hive partitions, and limits are
applied before decoding where safe. Residual SQL filters always remain in the
plan, so a source hint can never silently change query results.

Blocking operators reserve retained state through a hierarchical engine/query
memory pool. When the multi-lane Aggregate path is eligible, every supported
aggregate (`COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`) produces lane-local partial
states and a final merge; its hash partitions can Spill and recursively
repartition with a new seed. Join uses an immutable shared build for parallel
probe when it fits, Grace partitions on pressure, and switches to external
sort-merge after two seeds do not shrink a partition. Duplicate-key groups are
replayed in bounded chunks. Sort creates lane-local memory blocks or LZ4 Arrow
IPC runs and performs a bounded k-way merge.

`UNION ALL` compiles to a streaming Append over type-aligned inputs. DISTINCT
set operations reuse the existing reservation-accounted Aggregate and Join
operators: `UNION DISTINCT` groups the appended rows, while `INTERSECT` and
`EXCEPT` de-duplicate both sides and use NULL-equal Semi or Anti joins.
Query-level sorting and limiting run after the complete recursive set tree.

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

In the current alpha, parallel Aggregate and Sort, and the shared-build hash
Join probe, require more than one configured lane and at least a 64 MiB query
memory budget. The shared Join build must also fit within one quarter of that
budget. Smaller budgets use the serial or partitioned/Spill path; Grace Join
partitions may still run concurrently when multiple partition tasks exist.
Spill improves bounded-memory completion but does not make an individual row,
key group, decoder allocation, or minimum writer workspace arbitrarily small.

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

Local contents rely on the operating-system page cache. The engine cache holds
Parquet schema/footer metadata and separately keyed footer-plus-page-index
metadata under an approximate byte-bounded LRU. Keys contain URI, size, ETag,
and version; no decoded data page is retained.
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
object GET are conditional on those snapshots. A file can change between
queries, but a mid-query change returns an error instead of mixing versions.
The prepared providers' row/byte/file statistics are frozen into that query's
Scan nodes before join ordering and EXPLAIN; they never overwrite the shared
Catalog entry or another concurrent query's snapshot.
Cancellation races outstanding object requests, and S3 metrics include logical
resolution, metadata, snapshot, and data requests plus transferred body bytes.

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
