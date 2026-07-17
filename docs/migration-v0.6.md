# Migrating from v0.5 to v0.6 alpha

`v0.6.0-alpha.1` adds an optional persistent Native database while preserving
the ephemeral `Engine::new` path, external CSV/Parquet registration, prepared
statements, cancellation, and streaming Arrow results.

## Open a persistent database

Use `Engine::open` when Native tables must survive process restart:

```rust
use rustdb::{Engine, EngineConfig, Result};

fn open_database() -> Result<Engine> {
    Engine::open("./warehouse", EngineConfig::default())
}
```

The database directory is private to RustDB and includes a format marker,
versioned Catalog generations, immutable table snapshots, and staging state.
Do not edit, copy individual files from, or use it as a temporary directory.
Opening a database takes its process lock; a second writer process is rejected.
The lock coordinates RustDB processes, not arbitrary filesystem writers;
external mutation of the database directory while an engine is open is outside
the storage contract and may fail the current or next query.

`Engine::new` remains ephemeral. Native write commands require an engine opened
with a database path.

## Import, append, and replace

The initial alpha intentionally exposes only bulk, query-driven writes:

```sql
CREATE TABLE events AS
SELECT * FROM read_csv('events.csv', header = true);

INSERT INTO events
SELECT * FROM read_parquet('next/*.parquet');

CREATE OR REPLACE TABLE events AS
SELECT * FROM read_parquet('full/*.parquet');
```

Consume the returned `QueryResult` stream to completion before treating a write
as successful. Commit publication is atomic: existing queries keep their old
snapshot and later queries see the new generation. An error explicitly reports
whether publication is known to have completed or its outcome is unknown; do
not blindly retry either case.

Column lists, row `VALUES`, row-level update/delete, schema alteration, and
general transactions remain unsupported. Append requires the source columns to
match the target positionally and without a lossy cast. Native table names are
limited to 255 UTF-8 bytes so Catalog generations remain within their reserved
disk budget.

## Backup and restore

This alpha supports consistent local-directory backup and restore:

```rust
engine.backup_to("./backup-2026-07-15")?;

let restored = Engine::restore_from(
    "./backup-2026-07-15",
    "./restored-warehouse",
    EngineConfig::default(),
)?;
```

Both destination directories must not already exist. Backup copies one pinned
Catalog generation and exactly its reachable snapshots, then validates the
copy before publishing it. Publication never replaces a path created by another
process. If the destination rename succeeds but directory durability cannot be
confirmed, `backup_to` returns the structured `CommitOutcomeUnknown` error and
the caller must inspect the destination instead of blindly retrying. A later
backup in the same parent removes only recognizable, unlocked RustDB backup
temporaries, including interrupted partial copies; unknown or active directories
are preserved. S3/MinIO
backup destinations are planned for a later v0.6 alpha; S3/MinIO remain
supported as import sources in alpha.1.

## Aggregate type change

`SUM` no longer narrows an integer accumulator back to `Int64` or `UInt64`.
Signed and unsigned integer inputs now return `Decimal128(38, 0)`; Decimal128
inputs return `Decimal128(38, input_scale)`. Embedders that downcast Arrow
result arrays by position must update the expected type. `COUNT` remains
`Int64`, floating-point `SUM` remains `Float64`, and `AVG` remains `Float64`.

## Current alpha boundaries

Native storage is a bulk, immutable-segment format and may be the only retained
copy after a successful import. Writes enforce the 2x final and 3x peak source
ratios plus a bounded 64 KiB per-table format-metadata allowance, and never
treat unknown files as recoverable RustDB state. Remote backup, row-level
DML/MVCC, replication, and the full multi-table CBO are not part of alpha.1.
