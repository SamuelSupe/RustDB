# RustDB v0.8 roadmap

RustDB v0.8 evolves the persistent Native store from atomic bulk snapshots
into a durable, multi-writer embedded OLAP database. It remains single-node,
Rust-embedded, and CLI-accessible. Performance comparisons and throughput
gates are intentionally outside this release; correctness, recovery, and
format compatibility are release gates.

## Transaction contract

- Native v3 keeps immutable base and delta segments. Deletes are versioned
  delete vectors; updates create a new row version and delete the old one.
- Transactions use optimistic multi-writer snapshot isolation. A transaction
  pins one catalog/data snapshot. Concurrent writes to the same logical row
  use first-committer-wins; catalog objects use object-version conflicts.
- Write skew is permitted and documented. Serializable isolation, predicate
  locks, nested transactions, and savepoints are not part of v0.8.
- A successful commit is fully durable. WAL, data files, and required directory
  metadata are synced before success is returned. Recovery replays committed
  work and removes uncommitted staging.
- A post-publication error is reported as committed, while a WAL/Catalog
  publication that cannot be reconciled is terminal and indeterminate. The
  original transaction cannot retry or roll back; reopening the database is
  the reconciliation boundary.
- A transaction result that has already staged a mutation must be consumed to
  end-of-stream. Cancellation, an execution error, or consumer abandonment
  after staging makes the whole transaction roll back; v0.8 does not pretend
  to provide a statement savepoint.
- Historical versions exist only while required by active snapshots and
  recovery. v0.8 does not expose time-travel queries.

## Storage governance

Engine and table disk quotas are hard limits checked before publication and
again for an aggregate transaction commit. The engine quota covers the whole
Native root plus commit headroom; a table quota includes current, retired,
staged, and published-but-uncommitted snapshots. Named limits accept qualified
`schema.table` names. Steady
physical storage targets at most twice the current logical data estimate;
active snapshots and transaction staging may temporarily use at most three
times that estimate. Compaction and vacuum reclaim versions older than the
oldest active snapshot.

## SQL and API surface

The public transaction API is available alongside SQL `BEGIN`, `COMMIT`, and
`ROLLBACK`. v0.8 adds transactional `INSERT VALUES`, `INSERT SELECT`,
`UPDATE FROM`, `DELETE USING`, `TRUNCATE`, and DML `RETURNING`. Persistent
schemas and views plus safe transactional table/column DDL complete the
catalog lifecycle. `main` is the default persistent schema; `CREATE SCHEMA`,
`DROP SCHEMA`, `SHOW SCHEMAS`, `information_schema.schemata`, and qualified names are
available across query, DML, DDL, COPY, and maintenance paths. `MERGE`, upsert,
indexes, and relational constraints remain
deferred.

`COPY FROM` and `COPY TO` form a bidirectional CSV/Parquet path for local,
S3, and MinIO locations. Local output uses staging plus rename; object-store
output publishes its manifest last. Remote Native backup and restore share the
same consistency contract. A late result-delivery failure after COPY
publication returns `CopyPostCommitFailure`, explicitly telling the caller not
to retry an output that is already durable.

The query-language slice adds value/navigation windows, bounded
`ROWS`/`RANGE`/`GROUPS` frames, aggregate `FILTER`, ordering clauses for the
current order-insensitive aggregates, multi-expression aggregate distinct,
and `GROUPING SETS`/`ROLLUP`/`CUBE`. Order-sensitive list, string, and
percentile aggregates remain deferred.
The scalar type slice adds `TIME`, complete intervals, timestamp precision,
`TIMESTAMPTZ`, `AT TIME ZONE`, IANA timezone behavior, and UUID.

## Delivery stages

All four delivery slices below are implemented in `v0.8.0-alpha.1`; the split
records format and review boundaries rather than partially shipped behavior.

1. **alpha.1 foundation — implemented**: Native v3, checksummed WAL, snapshot-isolation
   transaction manager, public transaction API, crash recovery, and explicit
   v0.7 migration.
2. **alpha.2 relational writes — implemented**: DML, `RETURNING`, and transactional catalog/DDL.
3. **alpha.3 operations — implemented**: CSV/Parquet COPY, remote backup, compaction, vacuum, analyze,
   checkpoint, `information_schema`, and system views.
4. **alpha.4 SQL/types — implemented**: advanced OLAP SQL plus complete time and timezone types.

Every alpha must open, recover, and migrate independently. The final v0.8
acceptance run uses deterministic MVCC schedules, one restart at every durable
write boundary, golden SQL/COPY results, migration failure recovery, quota
checks, and resource-leak checks. Repeated soak and performance gates are not
required.

## Explicit exclusions

Serializable isolation, public time travel, savepoints, `MERGE`, upsert,
constraints, indexes, LIST/STRUCT/MAP/JSON execution, LATERAL/UNNEST, recursive
CTEs, Python/JDBC bindings, service protocols, replication, and distributed
execution remain outside v0.8.
