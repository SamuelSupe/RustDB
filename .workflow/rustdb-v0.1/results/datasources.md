# Data source result

Accepted:

- Unified local, `file://`, AWS S3, and S3-compatible object access.
- Strict streaming CSV with bounded inference, quoted-newline support, and
  bounded cross-file concurrency.
- Async Parquet range reads, row-group morsels, projection/limit/statistics
  pruning, Hive partitions, strict schema validation, and `union_by_name`.
- Query-global object snapshots are registered before schema/footer discovery,
  then revalidated and sealed for every Scan and dynamic View before any data
  is consumed; all metadata and data reads are conditional on that identity.
- Metadata LRU keys include URI, size, ETag, and version; planning-time S3
  resolution, schema/footer reads, snapshots, and data reads feed query metrics.

Verification:

- Local CSV/Parquet/Hive integration tests pass.
- Required live MinIO tests pass for signed and anonymous path-style access,
  disabled metadata cache behavior, cancellation, mid-query replacement, and
  CSV/Parquet result equality.
- An 8-row-group, greater-than-4-MiB Parquet fixture prunes at least 7 groups
  and transfers less than one quarter of the object.
- CSV tests cover explicit schema/delimiter, quoted newlines, malformed rows,
  invalid UTF-8, and incompatible multi-file schemas with source URIs.

Risk:

- Third-party footer/parser transient allocations are not all reserved through
  the engine memory pool.
- Concurrent cold metadata misses do not yet use singleflight.
