# Migrating from v0.1 to v0.2

`v0.2.0-alpha.1` keeps `Engine`, `Session`, `QueryResult`, and the public Arrow
stream shape. The configuration surface is intentionally tightened while the
project is still alpha.

## Engine configuration

`EngineConfig` is now `#[non_exhaustive]`. External crates should start from
`EngineConfig::default()` and mutate fields, or use the builder:

```rust,no_run
use rustdb::{Engine, EngineConfig, SpillConfig};

# fn main() -> rustdb::Result<()> {
let spill = SpillConfig {
    directory: "/var/tmp/rustdb".into(),
    query_limit_bytes: Some(200 << 30),
    ..SpillConfig::default()
};
let config = EngineConfig::builder()
    .memory_limit(1 << 30)
    .compute_threads(4)
    .spill(spill)
    .build();
let engine = Engine::new(config)?;
# let _ = engine;
# Ok(())
# }
```

The v0.1 `EngineConfig::temp_dir` field is replaced by
`EngineConfig::spill.directory`. The CLI spells this `--spill-directory` and
accepts `--temp-dir` as a compatibility alias during the v0.2 alpha cycle.

## Registered external tables

Registration now retains the location patterns and a stable schema. Every
query resolves the patterns again, so files added or removed between queries
are discovered automatically. The resolved objects are fixed for the lifetime
of that query. New columns are not exposed until an explicit refresh:

```rust,no_run
# async fn refresh(session: &rustdb::Session) -> rustdb::Result<()> {
let schema = session.refresh_table("events").await?;
println!("refreshed fields: {}", schema.fields().len());
# Ok(())
# }
```

The SQL equivalent is `REFRESH TABLE events`. Refresh atomically replaces the
Catalog entry; already planned queries continue with their old `Arc` snapshot.

CSV remains strict between refreshes. A newly discovered file must match the
registered physical schema or the query fails with URI and column context.
Explicit CSV refresh re-infers the currently matched files. Columns surviving
from the previous Catalog schema keep their old public order, removed columns
disappear, and genuinely new columns are appended in lexical name order. File
header order is independent of that public order; RustDB remaps Scan projection
and predicates by column name.

## Parquet schema modes

`ParquetOptions::schema_mode` accepts `Strict`, `UnionByName`, or
`SafeWidening`. Existing `union_by_name = true` remains supported and maps to
`UnionByName` when `schema_mode` remains at its `Strict` default. In the Rust
API, combining the legacy flag with an explicitly non-`Strict` mode is an
error. In SQL, naming both `union_by_name` and `schema_mode` is always an error.
`SafeWidening` performs only documented lossless top-level conversions and
reports the file URI and column when a merge or checked cast fails. Only
`UnionByName` fills a missing column with `NULL`; `SafeWidening` requires every
registered column to be present and accepts extra columns without exposing
them until `REFRESH TABLE`.

## Result memory

Engine-owned batches are charged while they are inside scans, kernels,
operator state, and internal queues. v0.2 also charges Filter/Projection
expression workspace, Aggregate expression and row-key workspace, evaluated
Join keys, and CSV inference plus the prepared CSV schema. The lease is
released immediately before a batch crosses `QueryResult`'s public stream
boundary, so batches retained by the embedding application remain outside the
engine budget as in v0.1. The configured limit governs stable engine-owned
state; it is not a strict process-RSS cap for allocator and decoder transient
allocations.

Temporary pressure now backpressures Scan and kernel lanes through
cancellation-aware reservations. A permanently impossible single-batch or
single-operation request still returns `ResourceExhausted`; callers no longer
need to compensate for lane-count-dependent queue spikes by increasing the
limit.

## Parallel execution and Spill

`compute_threads` now caps Scan/Filter/Projection lanes, partial Aggregate,
parallel Join work, and Sort-run generation. Actual concurrency can be lower
when there are fewer Scan or partition tasks, an operator is not eligible, or
the memory budget is below its parallel threshold. Every supported aggregate
function participates in partial/final aggregation when that path is selected.
Queries without `ORDER BY` must no longer rely on file or lane output order.

Spill configuration moves under `EngineConfig::spill` and includes the I/O
pool, engine/query quotas, free-space reserve, and orphan TTL. Every live query
directory holds an exclusive lock on `.rustdb-active`; startup cleanup skips a
locked directory even after its marker reaches the TTL. A crashed process
releases the kernel lock, allowing a future Engine to clean the valid,
version-marked orphan. Applications must not delete or replace RustDB marker or
activity files while a query is active.
