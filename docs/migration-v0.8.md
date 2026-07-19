# Migrating from v0.7 to v0.8

RustDB v0.8 changes the writable Native database contract. Opening an existing
v0.7 database remains supported for reads, but writes fail until the operator
runs an explicit migration:

```sh
rustdb migrate /path/to/warehouse
```

The migration fully opens and validates the source, creates or validates the
sibling `/path/to/warehouse.v0.7-backup`, initializes an empty WAL, and then
atomically replaces the database marker. Re-running the command on a migrated
database is a no-op. An existing backup is reused only when its database ID and
complete catalog snapshot still match the live v0.7 source; a stale or foreign
backup stops the migration before the source marker changes.

## New additive Rust API

```rust,no_run
use rustdb::{Engine, EngineConfig, TransactionOptions};

# fn example() -> rustdb::Result<()> {
let engine = Engine::open("./warehouse", EngineConfig::default())?;
let session = engine.session();
let mut transaction = session.begin_transaction(TransactionOptions::read_only())?;
let _id = transaction.id();
transaction.commit()?;
# Ok(())
# }
```

`Session::begin_transaction`, `TransactionOptions`, `Transaction`,
`TransactionPreparedStatement`, `CommitInfo`, `Engine::migrate`, and
`MigrationInfo` are additive. Query results remain streaming. A transaction
cannot commit while one of its result streams is still alive. After a mutation
has been staged, its result must be consumed through end-of-stream. Cancelling,
abandoning, or receiving an execution error from that result rolls back the
whole transaction because v0.8 has no statement savepoints. Dropping a result
signals cancellation without blocking; wait for background query cleanup before
issuing transaction control on the same session.

Commit errors are terminal and classified by durability. A
`NativeCommitPostCommitFailure` means the Catalog generation is committed and
`Transaction::commit_info()` remains available. A `CommitOutcomeUnknown` means
the WAL/Catalog publication could not be reconciled; the handle is
indeterminate and rejects further execute/commit/rollback calls. Reopen the
database and inspect the recovered Catalog before retrying any logical write.
SQL `COMMIT` removes the active transaction for both terminal error classes.

SQL `BEGIN`/`START TRANSACTION`, `COMMIT`, and `ROLLBACK` use snapshot isolation.
`READ ONLY` and `READ WRITE` are accepted; other isolation levels, savepoints,
and chained transaction control are rejected.

Native tables now support transactional `INSERT`, `UPDATE FROM`, `DELETE USING`,
`TRUNCATE`, and `RETURNING`. Persistent table/view DDL participates in the same
transaction snapshot. Concurrent disjoint row changes rebase, while the first
committer wins for the same stable row or Catalog object.

Persistent objects now live in SQL schemas. Unqualified names resolve to
`main`; `CREATE SCHEMA`, `DROP SCHEMA`, `SHOW SCHEMAS`, and
`information_schema.schemata` are supported. Qualified `schema.object` names
work consistently in queries, DML, DDL, COPY, maintenance, temporary views,
and external-table registration. Existing catalog manifest v1 entries are
loaded in `main`; the next catalog commit writes manifest v2 with an explicit
schema set.

## Operational additions

- `COPY FROM` and `COPY TO` move CSV/Parquet data between Native tables and
  local or S3-compatible locations. A strictly named local staging file or a
  remote child object left by a process crash blocks reuse of that destination
  until the operator inspects and removes it.
- `Engine::backup_to_location` and `Engine::restore_from_location` publish and
  validate local or S3 backups; the CLI exposes `backup` and `restore`.
- Abandoning an embedded remote-backup future does not abandon its objects: an
  Engine-owned task finishes manifest publication or completes multipart and
  object cleanup. Process crashes still require an object-store incomplete-
  multipart lifecycle policy. A completed object from a pre-manifest crash is
  not an incomplete multipart upload; RustDB rejects the non-empty destination
  until the operator inspects and removes that dedicated prefix.
- `COMPACT`, `VACUUM`, `ANALYZE`, and `CHECKPOINT` provide explicit maintenance.
- `information_schema` and `rustdb_system` expose read-only metadata and
  operational state.
- Optional Native engine/default-table/named-table hard quotas are configured
  on `EngineConfig` for each open; they are not persisted in the database. The
  engine limit covers the complete Native root, while named overrides accept
  `schema.table`.
- `Error::CopyPostCommitFailure` means COPY output is already durable but
  result delivery or later cleanup failed. Do not retry that destination.

The query API remains source-compatible. `ParameterValue` adds TIME,
arbitrary-precision timestamp/TIMESTAMPTZ, UUID, and Arrow interval variants.
SQL adds bounded window frames, navigation/value windows, aggregate `FILTER`,
multi-expression aggregate distinct, grouping sets, complete compound
intervals, and IANA timezone conversion.

## Storage compatibility

- Database marker v1 (v0.7) is read-only and explicitly migrates to marker v2.
- Table manifests v1 and v2 remain readable without rewriting data.
- New table snapshots use manifest v3, which adds stable physical row identity,
  visible/physical row counts, and versioned checksummed delete vectors.
- Native segments encode Arrow interval columns as a private fixed-width
  physical Parquet representation and restore the manifest's logical type on
  scan. Applications continue to observe Arrow interval arrays.
- WAL commit intent is durable before Catalog `CURRENT` publication. Recovery
  completes a durable unpublished commit or removes uncommitted staging.
- Catalog manifest v1 remains readable and is interpreted as the `main`
  schema; new catalog generations use manifest v2.

All v0.8 delivery slices are present in `v0.8.0-alpha.1`; see the compatibility
matrix for deliberate exclusions and combination limits.
