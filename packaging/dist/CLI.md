# RustDB CLI help

[简体中文](CLI.zh-CN.md)

Run `rustdb --help` for the authoritative option list or `rustdb --help-zh`
for built-in Simplified Chinese help.

## Query input and output

Without `-c` or `-f`, RustDB starts its interactive shell. SQL statements in
the shell and SQL files must end with a semicolon.

```sh
# One statement
rustdb -c "SELECT count(*) FROM read_csv('/data/events/*.csv')"

# A SQL file, rendered as JSON Lines
rustdb -f report.sql --format jsonl

# Machine-readable CSV with an explicit NULL token
rustdb -f report.sql --format csv --csv-null '\N'
```

Formats are `table`, `csv`, and `jsonl`. `--metrics` writes execution metrics
to stderr after the streamed result has been consumed.

## Read-only HTTPS Shell

Start one TLS server for one persistent Native database. Loopback is the safe
default; a non-loopback listener also requires its public HTTPS origin so the
generated certificate has the correct identity:

```sh
rustdb serve --database /srv/rustdb/analytics

rustdb serve \
  --database /srv/rustdb/analytics \
  --listen 0.0.0.0:7400 \
  --advertise-url https://analytics.example.com:7400
```

The first start creates a local CA, a renewable server certificate, and a
an Admin principal and random profile Token. Stop the server, export its connection bundle, transfer
the bundle through a trusted channel, and import it on the client:

```sh
rustdb profile export \
  --database /srv/rustdb/analytics \
  --output analytics.rustdb-profile
rustdb profile import analytics.rustdb-profile --name analytics
```

With the server stopped, list credentials without exposing secret material and
export a Profile for a specific Query/Admin principal. The token-specific
export reuses the managed bundle's URL and CA; add `--server-url` to override
the HTTPS origin.

```sh
rustdb token list --database /srv/rustdb/analytics --principal analyst
rustdb token rotate --database /srv/rustdb/analytics --principal analyst
rustdb profile export --database /srv/rustdb/analytics \
  --token-id <UUID> --output analyst.rustdb-profile
rustdb token revoke --database /srv/rustdb/analytics --token-id <UUID>
```

`token list` emits only UUID, principal, active/revoked/inactive state, and
valid-until; it never emits the token secret, digest, or token-file path.

Use the named Profile with the remote CLI. Interactive mode waits for each
background Query; `-c` and `-f` are script-friendly and preserve the local
renderers. Ctrl-C sends a best-effort cancellation request.

```sh
rustdb shell --profile analytics -c \
  "SELECT region, count(*) FROM sales GROUP BY region" --format table
rustdb shell --profile analytics -f report.sql --format csv >report.csv
```

The remote boundary is intentionally read-only. It accepts query, metadata,
and explain statements over Native/system tables and server-local registered
sources. It rejects DDL/DML, maintenance, uploads, direct `read_csv` or
`read_parquet`, and remote source administration. See `docs/HTTP-SHELL.md` and
`docs/openapi-v1.yaml` in the distribution for the lifecycle and public `/v1`
contract.

`serve` accepts result-retention controls independently of the engine Spill
limits: `--result-directory`, `--result-ttl-secs`, `--result-global-limit`, and
`--result-query-limit`. By default, completed results live for one hour; their
total hard limit is 10 GiB and one Query can use 2 GiB. Layered admission is
configured with `--query-*-limit`, `--principal-*-limit`, principal running and
queue limits, and `--principal-weight`; Admin has no implicit priority. The
engine enforces the corresponding Spill hard limits through
`--spill-engine-limit` and `--spill-query-limit`. The same command accepts `--s3-region`,
`--s3-endpoint`, `--s3-path-style`, `--s3-allow-http`, and `--s3-anonymous`
for server-local registered sources. Use HTTP only for trusted development
endpoints and anonymous mode only for public objects.

Persistent CSV/Parquet registrations are changed only on the server host while
`rustdb serve` is stopped:

```sh
rustdb datasource add-parquet \
  --database /srv/rustdb/analytics \
  --name sales \
  --location 's3://lake/sales/*.parquet'
rustdb datasource list --database /srv/rustdb/analytics
rustdb datasource refresh --database /srv/rustdb/analytics --name sales
rustdb datasource remove --database /srv/rustdb/analytics --name sales
```

Registrations never store object-store credentials. Supply credentials to the
server through its service account's default credential chain.

## Persistent Native database

Use `--database` to open a local persistent database instead of an ephemeral
engine:

```sh
rustdb --database ./warehouse -c \
  "CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/*.parquet')"

rustdb --database ./warehouse -c \
  "BEGIN READ ONLY; SELECT count(*) FROM events; COMMIT"
```

RustDB v0.8 Native databases use a checksummed WAL and snapshot-isolation
transactions. A transaction pins one catalog/data snapshot. Consume or drop
every streaming result before `COMMIT`. Mutation results must be consumed to
end-of-stream; cancellation or abandonment after staging rolls back the whole
transaction because v0.8 has no statement savepoints.

If `COMMIT` reports an unknown outcome, do not repeat it: reopen the database
and inspect the recovered Catalog. A post-commit failure explicitly means the
generation is already durable and must not be retried.

Persistent objects default to the `main` schema. Use `CREATE SCHEMA analytics`
and qualified names such as `analytics.events`; the same name works in queries,
DML, DDL, COPY, and maintenance commands. `SHOW SCHEMAS` and
`information_schema.schemata` expose the namespace. If COPY reports
`CopyPostCommitFailure`, its destination is already durable and must not be
retried. If COPY instead reports an existing incomplete staging file or remote
child object, inspect and remove that incomplete destination before retrying;
RustDB does not delete crash leftovers automatically.

Opening a v0.7 database is read-only until it is migrated explicitly:

```sh
rustdb migrate ./warehouse
```

Migration validates the source first and keeps a sibling
`warehouse.v0.7-backup`. An existing backup must match the current source
catalog snapshot exactly. It never silently upgrades a database during open.

Create or restore a verified backup with the standalone database commands:

```sh
rustdb backup ./warehouse ./warehouse-backup
rustdb restore ./warehouse-backup ./warehouse-restored

rustdb --s3-region us-east-1 \
  backup ./warehouse s3://analytics/rustdb/warehouse-2026-07-18
rustdb --s3-region us-east-1 \
  restore s3://analytics/rustdb/warehouse-2026-07-18 ./warehouse-restored
```

Remote backup uploads immutable objects first and publishes its manifest last.
Restore validates object size and hash and requires a new local destination.
S3 endpoint/path-style/HTTP/anonymous flags are the same as query scans; secret
keys are still resolved only by the default credential chain.

Abandoning the embedded backup future keeps an Engine-owned worker alive until
it publishes the manifest or removes unfinished objects. Configure an S3
incomplete-multipart lifecycle policy for process or host crashes. If a crash
leaves a completed object before the manifest, RustDB rejects that non-empty
destination; inspect and remove its dedicated prefix before retrying.

## Data sources

```sql
SELECT *
FROM read_parquet('/data/events/*.parquet')
WHERE event_date >= '2026-01-01'
LIMIT 10;

SELECT count(*)
FROM read_csv('/data/events/*.csv.gz', compression = 'auto');
```

Local paths, globs, `file://`, `s3://`, raw CSV, gzip CSV, zstd CSV, and Parquet
are supported. The SQL compatibility boundary is documented in the source
repository; unsupported SQL returns an explicit error.

For AWS S3, credentials are resolved through the default credential chain:

```sh
AWS_PROFILE=analytics rustdb --s3-region us-east-1 -c \
  "SELECT count(*) FROM read_parquet('s3://bucket/events/*.parquet')"
```

For MinIO or another compatible service:

```sh
rustdb --s3-endpoint http://127.0.0.1:9000 \
  --s3-region us-east-1 --s3-path-style --s3-allow-http \
  -c "SELECT * FROM read_parquet('s3://bucket/events/*.parquet') LIMIT 10"
```

Use `--s3-anonymous` only for public objects. RustDB intentionally has no CLI
flags for plaintext access or secret keys.

## Resource controls

The most commonly adjusted options are:

- `--memory-limit 2GiB`: query-engine memory budget;
- `--database PATH`: persistent Native database directory;
- `--native-engine-limit 20GiB`: hard quota for the complete Native database directory;
- `--native-default-table-limit 5GiB`: default hard quota for one Native table;
- `--native-table-limit events=10GiB`: named table override; repeat for more
  tables and use `schema.table` for a non-default schema;
- `--threads 4`: compute worker count;
- `--batch-size 8192`: target Arrow batch rows;
- `--io-concurrency 16`: concurrent scan tasks;
- `--metadata-cache 256MiB`: Parquet metadata cache;
- `--max-concurrent-queries 1`: admitted query count;
- `--spill-directory PATH`: query Spill root;
- `--spill-engine-limit` and `--spill-query-limit`: hard Spill quotas;
- `--runtime-filter-bytes 8MiB`: Join runtime-filter budget.

For `serve`, result TTL/global/per-Query limits are separate from these local
execution controls; see the HTTPS Shell section above.

Sizes accept `B`, `KB`, `MB`, `GB`, `KiB`, `MiB`, and `GiB`.
The same limits are available through `NativeStorageConfig` in the Rust API.
Limits are supplied on each open and are not written into the database format.

## Interactive commands

- `.tables`: list registered external tables;
- `.help` or `.help en`: show English shell help;
- `.help zh`: show Simplified Chinese shell help;
- `.quit` or `.exit`: exit.

Result order is unspecified without an outer `ORDER BY`. Press Ctrl-C to cancel
the active query.
