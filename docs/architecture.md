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

Local contents rely on the operating-system page cache. The only engine cache
holds Parquet schema/footer metadata under an approximate byte-bounded LRU.
Keys contain URI, size, ETag, and version; no decoded data page is retained.

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
