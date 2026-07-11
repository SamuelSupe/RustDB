# Architecture

The query path is deliberately direct:

```text
SQL parser -> binder/catalog -> logical plan -> rule optimizer
           -> physical pipelines -> bounded RecordBatch stream -> caller
```

Arrow `RecordBatch` is the only exchange batch. Scan, Filter, and Projection
stream immediately. Aggregate, Join build, Sort, and scalar-subquery
materialization are pipeline breakers. Engine owns a fixed-size Tokio compute
runtime configured by `compute_threads`; a two-batch channel carries output to
the embedding runtime and applies consumer backpressure. Object-store work is
asynchronous, while per-source fan-out is bounded by `io_concurrency`.

Parquet morsels are file plus row group and may execute concurrently. CSV
morsels are files; one CSV remains sequential so quoted records cannot be split
incorrectly. Projection, row-group statistics, Hive partitions, and limits are
applied before decoding where safe. Residual SQL filters always remain in the
plan, so a source hint can never silently change query results.

Blocking operators reserve retained state through a hierarchical engine/query
memory pool. Sort writes LZ4 Arrow IPC runs and performs bounded-fan-in merge.
Aggregate spills hash partitions and recursively repartitions skewed partitions
with a new seed. Join uses in-memory hash build, Grace partitions on pressure,
then bounded recursive repartitioning and a skew fallback when hashing cannot
shrink a duplicate-key partition. Spill files are query-scoped, mode `0600`,
and removed after success, failure, cancellation, or consumer abandonment.
Active Spill paths, long-lived IPC writers, partition indices, decoded Spill
batches, and merge state all retain reservations; file rotation or recursive
partitioning therefore cannot grow an uncharged in-memory path list.

The configured limit is an engine reservation budget, not a process-RSS hard
limit. Arrow readers and kernels allocate inside `next()`/kernel calls before
the resulting buffer size is known; RustDB reserves the complete batch as soon
as ownership returns and rejects it before processing if it cannot fit.
Workspaces use conservative pre-reservations, but allocator bookkeeping and
temporary decoder buffers can still differ from the reservation estimate.
Once a result batch is yielded, memory retained by the embedding caller is
outside the engine's ownership and budget; benchmark reports record RSS
separately.

Local contents rely on the operating-system page cache. The only engine cache
holds Parquet schema/footer metadata under an approximate byte-bounded LRU.
Keys contain URI, size, ETag, and version; no decoded data page is retained.
File discovery has a separate Engine-memory-derived metadata cap and avoids a
second de-duplication set. Query snapshot entries retain memory reservations.
Parquet reads its fixed trailer and reserves a conservative footer expansion
before decoding; the resulting lease stays live through every row-group
morsel. Row-group pruning iterates indices directly instead of materializing a
file-sized selected list. Decoder and public output batches remain
structurally bounded by `io_concurrency`, channel capacity, and `batch_size`.

Before any physical input stream is polled, RustDB walks every Scan in the
query (including dynamic views), captures each object's fresh identity, and
seals the query-wide snapshot map. Parquet footer/data ranges and the CSV
object GET are conditional on those snapshots. A file can change between
queries, but a mid-query change returns an error instead of mixing versions.
Cancellation races outstanding object requests, and S3 metrics include logical
resolution, metadata, snapshot, and data requests plus transferred body bytes.

The stable public surface is `Engine`, `Session`, registration options,
`QueryResult`, cancellation, and metrics. Catalog, logical/physical plan, data
source, scheduler, and operator types are crate-private so implementations can
evolve without coupling embedded callers to internal nodes.
