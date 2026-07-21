# RustDB read-only HTTP Shell

RustDB Beta can expose one Native database to the existing command-line query
experience over HTTPS. The server executes only read-only SQL. It is not a
browser shell, a write API, or a remote administration service.

For the exact wire contract, see [OpenAPI v2](openapi-v2.yaml). For deployment,
monitoring, and recovery, see the [Beta operator guide](operator-guide.md). A Simplified
Chinese version of this guide is available in
[http-shell.zh-CN.md](http-shell.zh-CN.md).

## Start a local server

The safest default listens only on loopback:

```sh
rustdb serve --database /srv/rustdb/analytics
```

First startup creates a local CA, a short-lived server certificate, an `admin`
principal, a random profile token, and connection metadata in the
operating-system user state directory. `principals.json` stores only SHA-256
token digests; clear-text tokens exist only in permission-restricted profile
token files and are never logged. The legacy server-side `bearer.token` file is
not accepted. Secret material is not written into the Native database directory.
The process stays in the foreground; use systemd, launchd, Docker, or another
process manager when daemon supervision is required.

To accept remote connections, both the listener and the client-visible HTTPS
URL must be explicit:

```sh
rustdb serve \
  --database /srv/rustdb/analytics \
  --listen 0.0.0.0:7400 \
  --advertise-url https://analytics.example.com:7400
```

The advertised hostname or IP is placed in the certificate SAN. RustDB rejects
`0.0.0.0` as an advertised client address. All TCP listeners use TLS; there is
no plaintext-HTTP mode.

## Transfer and import a profile

Export a permission-restricted connection bundle on the server host while the
server is stopped:

```sh
rustdb profile export \
  --database /srv/rustdb/analytics \
  --output analytics.rustdb-profile
```

Transfer the bundle through a trusted out-of-band channel such as SSH or
removable media. On the client:

```sh
rustdb profile import analytics.rustdb-profile --name analytics
```

The named profile records the advertised URL and paths to local CA and Token
files. The Token file is permission-restricted and the Token is never accepted
as a plaintext command-line flag.

Manage principals locally while the server is stopped:

```sh
rustdb principal list --database /srv/rustdb/analytics
rustdb principal create --database /srv/rustdb/analytics --id analyst --role query
rustdb token list --database /srv/rustdb/analytics --principal analyst
rustdb token rotate --database /srv/rustdb/analytics --principal analyst
rustdb token revoke --database /srv/rustdb/analytics --token-id <UUID>
rustdb profile export --database /srv/rustdb/analytics \
  --token-id <UUID> --output analyst.rustdb-profile
```

Rotation adds an overlapping credential so clients can move without an
interruption; revoke the old token explicitly afterwards. `token list` exposes
only UUID, principal, lifecycle state, and validity. `profile export --token-id`
reuses the URL and CA in the managed server bundle; add `--server-url` only to
set an explicit HTTPS origin. Query-role principals can access only Queries
they own. Admin principals can access every Query, and Idempotency Keys are
scoped to the submitting principal. `--no-auth` is an explicit development-only
mode; it uses one fixed anonymous Query owner and never grants Admin permission.

## Query from the CLI

Open the interactive shell:

```sh
rustdb shell --profile analytics
```

Although the protocol creates a background Query ID, the official shell waits,
polls sequenced Arrow IPC batches with backpressure, and renders them like a
local query. `Ctrl-C` requests remote cancellation. There are no job-management
backslash commands.

Script mode also waits and maps the terminal Query state to the process exit
code:

```sh
rustdb shell --profile analytics \
  -c "SELECT region, sum(amount) FROM sales GROUP BY region" \
  --format table

rustdb shell --profile analytics -f report.sql --format csv > report.csv
rustdb shell --profile analytics -f report.sql --format jsonl > report.jsonl
```

`table`, `csv`, and `jsonl` are the supported local renderers. A fully rendered
result is explicitly deleted from the server. If output fails or the client
disconnects, the result remains available until its one-hour TTL.

## Read-only SQL boundary

The remote server accepts one of these statements per request:

- `SELECT`, including non-recursive `WITH` queries;
- `VALUES`;
- `SHOW` and `DESCRIBE`;
- `EXPLAIN` and `EXPLAIN ANALYZE`.

Remote queries may read Native tables, system tables, and CSV/Parquet sources
that were registered locally on the server. Direct file functions such as
`read_csv('/path')` and `read_parquet('s3://bucket/key')` are rejected. So are
DDL, DML, transactions, maintenance, uploads, and source administration. The
server validates the parsed statement and repeats the policy while expanding
persistent Views; it does not rely on SQL keyword filtering.

Change persistent source registrations locally while `rustdb serve` is
stopped, then restart the server. Keep S3 credentials in the server's standard
credential provider; RustDB does not save them in source metadata or profiles.

```sh
rustdb datasource add-csv \
  --database /srv/rustdb/analytics \
  --name web_events \
  --location 's3://lake/events/*.csv.gz'

rustdb datasource add-parquet \
  --database /srv/rustdb/analytics \
  --name sales \
  --location 's3://lake/sales/*.parquet'

rustdb datasource list --database /srv/rustdb/analytics
rustdb datasource refresh --database /srv/rustdb/analytics --name sales
rustdb datasource remove --database /srv/rustdb/analytics --name web_events
```

The definitions live in the optional Native Catalog sidecar
`catalog/external-sources.json`, are updated atomically, and are included in
Native backup/restore. They contain locations and non-secret format options,
never credentials. The database lock naturally rejects these local changes
while `rustdb serve` owns the database.

## Typed parameters

The public protocol accepts `?` or contiguous `$1` parameters, but not both in
one statement. Each value has an explicit type:

```sh
curl --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: report-2026-07-19-001' \
  --data '{
    "sql": "SELECT * FROM sales WHERE day >= $1 AND region = $2",
    "parameters": [
      {"type": "date32", "value": 20653},
      {"type": "utf8", "value": "APAC"}
    ],
    "timeout_ms": 60000
  }' \
  https://analytics.example.com:7400/v2/queries
```

Parameters can replace expression values only. They cannot be table names,
column names, source patterns, S3 configuration, or table-function options.
The canonical wire types are `boolean`, `int64`, `uint64`, `float64`,
`decimal128`, `utf8`, `binary`, `date32`, and `timestamp_microsecond`. A JSON
`null` value creates a typed NULL of the declared `type`; Decimal also requires
`precision` and `scale`. See `TypedParameter` in the OpenAPI document for the
exact shape.

## Raw HTTP lifecycle

Every submission needs an Idempotency Key:

```sh
QUERY_ID=$(
  curl --silent --cacert ca.pem \
    -H "Authorization: Bearer $(<token)" \
    -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: example-query-0001' \
    --data '{"sql":"SELECT count(*) AS rows FROM sales"}' \
    https://analytics.example.com:7400/v2/queries |
  jq -r .query_id
)
```

The server compares the decoded request envelope, not the original JSON bytes.
Whitespace and object-member order are insignificant, and an omitted empty
`parameters` array is equivalent to `"parameters": []`. Reusing a Key with a
different decoded envelope returns `409 idempotency.key_conflict`.

Poll status until it is terminal:

```sh
curl --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}"
```

Status timestamps are Unix-epoch milliseconds in `created_at_ms`, optional
`started_at_ms`, and optional `finished_at_ms`. Every status includes
`result_available`; retained results also expose `result_expires_at_ms`,
`result_rows`, `result_bytes`, and `result_batches`. Successful Queries include
the bounded `metrics` object; failed, cancelled, or interrupted Queries include
`error`. Absent optional fields are omitted rather than encoded as JSON `null`.

List visible Queries in stable newest-first order with optional state and
creation-time filters. Query principals see their own work; Admin sees all
principals. The default page is 50 and the maximum is 200; pass the opaque
`next_cursor` unchanged to fetch the following page:

```sh
curl --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v2/queries?state=interrupted&created_after_ms=1784500000000&limit=50"
```

Fetch JSON by offset:

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/json' \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}/results?offset=0&limit=1000"
```

Or fetch NDJSON with an opaque Cursor returned by the previous page:

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/x-ndjson' \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}/results?cursor=${CURSOR}&limit=1000"
```

JSON/NDJSON require successful completion. Cursor and offset are mutually
exclusive; pages are immutable and repeatable, and the server does not advance
shared fetch state.

For incremental typed output while execution is running, request exactly one
Arrow batch sequence at a time:

```sh
curl --dump-header batch.headers --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/vnd.apache.arrow.file' \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}/results?batch_seq=0" \
  --output batch-000.arrow
```

An Arrow `200` is an independent IPC file containing exactly one RecordBatch.
A schema-only IPC file with `X-RustDB-Result-Complete: true` marks completion.
If shutdown interrupts execution, committed batches remain readable and the
last response reports `X-RustDB-Result-State: interrupted` with completion
true. That stream is an explicit result prefix, never a successful full result;
JSON/NDJSON remain unavailable for it.
If the requested sequence is not committed yet, `204` has no body and includes
`Retry-After: 1`. Advance only to the exact `X-RustDB-Next-Batch-Seq`; do not mix
`batch_seq` with `cursor`, `offset`, or `limit`.

Cancel queued or running work, then delete terminal state:

```sh
curl -X POST --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}/cancel"

curl -X DELETE --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v2/queries/${QUERY_ID}"
```

## Result encoding

JSON pages have the shape `{schema, rows, page}`. NDJSON starts with one
`schema` record, emits a `row` array for every row, and ends with one `page`
record. Rows are positional arrays so duplicate column names remain valid.
Every persisted result batch carries a SHA-256 that is verified both when read
and during restart recovery; a mismatch fails and invalidates that result.

```json
{
  "schema": [
    {"name": "region", "data_type": "Utf8", "nullable": false},
    {"name": "total", "data_type": "Decimal128(38, 2)", "nullable": true}
  ],
  "rows": [["APAC", 1234.50]],
  "page": {
    "offset": 0,
    "row_count": 1,
    "complete": true
  }
}
```

The equivalent NDJSON framing is:

```jsonl
{"type":"schema","columns":[{"name":"region","data_type":"Utf8","nullable":false},{"name":"total","data_type":"Decimal128(38, 2)","nullable":true}]}
{"type":"row","values":["APAC",1234.50]}
{"type":"page","page":{"offset":0,"row_count":1,"complete":true}}
```

Integer, floating-point, and Decimal values are JSON numbers. The official CLI
keeps the original JSON number token, so Int64, UInt64, and Decimal do not pass
through a floating-point conversion; third-party clients that decode all
numbers as floating point may lose precision. Non-finite floats use the strings `"NaN"`, `"Infinity"`, and
`"-Infinity"`. Date is ISO `YYYY-MM-DD`, timezone-free Timestamp is ISO text
without `Z`, timezone-aware Timestamp is RFC 3339, and Binary is Base64.

The default page contains 1,000 rows. A page never exceeds 10,000 rows or
16 MiB of encoded row arrays; the schema and JSON/NDJSON framing add a small
amount of HTTP-body overhead. Non-terminal pages include `next_cursor`; final
pages omit it. The JSON representation is the default, and the exact
`application/x-ndjson` media type selects NDJSON. `Accept-Encoding: gzip` is
honored for responses worth compressing.

## Configuration and operations

Server options may be loaded from TOML, `RUSTDB_*` environment variables, and
CLI flags. Precedence is:

```text
CLI > environment > TOML > defaults
```

Important defaults are one running Query, a weighted-fair queue of 64, a
30-minute Query timeout, one-hour result TTL, a 10 GiB result-store hard limit,
and a 2 GiB per-Query result hard limit. Every principal has the same default
weight; the Admin role receives no scheduling priority.

Use these `serve` options when the defaults do not fit the deployment:

| Concern | CLI | TOML (`[server]` / `[engine]`) | Environment |
| --- | --- | --- | --- |
| Result location and retention | `--result-directory`, `--result-ttl-secs` | `result_directory`, `result_ttl_secs` | `RUSTDB_RESULT_DIRECTORY`, `RUSTDB_RESULT_TTL_SECS` |
| Result total/per-Query quota | `--result-global-limit`, `--result-query-limit` | `result_global_limit`, `result_query_limit` | `RUSTDB_RESULT_GLOBAL_LIMIT`, `RUSTDB_RESULT_QUERY_LIMIT` |
| Query memory/Spill/result reservation | `--query-memory-limit`, `--query-spill-limit`, `--query-result-limit` | `query_memory_limit`, `query_spill_limit`, `query_result_limit` | `RUSTDB_HTTP_QUERY_MEMORY_LIMIT`, `RUSTDB_HTTP_QUERY_SPILL_LIMIT`, `RUSTDB_HTTP_QUERY_RESULT_LIMIT` |
| Principal running/queue limits | `--principal-max-running`, `--principal-max-queued` | `principal_max_running`, `principal_max_queued` | `RUSTDB_HTTP_PRINCIPAL_MAX_RUNNING`, `RUSTDB_HTTP_PRINCIPAL_MAX_QUEUED` |
| Principal resource reservation | `--principal-memory-limit`, `--principal-spill-limit`, `--principal-result-limit` | `principal_memory_limit`, `principal_spill_limit`, `principal_result_limit` | `RUSTDB_HTTP_PRINCIPAL_MEMORY_LIMIT`, `RUSTDB_HTTP_PRINCIPAL_SPILL_LIMIT`, `RUSTDB_HTTP_PRINCIPAL_RESULT_LIMIT` |
| Principal fair-share weight | `--principal-weight` | `principal_weight` | `RUSTDB_HTTP_PRINCIPAL_WEIGHT` |
| RSS pressure guard | TOML only | `rss_warning_ratio`, `rss_high_ratio`, `rss_critical_ratio`, `rss_sample_interval_ms` | — |
| Blocking service I/O pool | `--service-io-threads` | `service_io_threads` | `RUSTDB_SERVICE_IO_THREADS` |
| Local Admin socket | `--admin-socket` | `admin_socket` | `RUSTDB_ADMIN_SOCKET` |
| TLS renewal check | `--tls-renew-interval-secs` | `tls_renew_interval_secs` | `RUSTDB_TLS_RENEW_INTERVAL_SECS` |
| Spill hard limits | `--spill-engine-limit`, `--spill-query-limit` | `spill_engine_limit`, `spill_query_limit` | `RUSTDB_SPILL_ENGINE_LIMIT`, `RUSTDB_SPILL_QUERY_LIMIT` |
| Authentication escape hatch | `--no-auth` | `no_auth` | `RUSTDB_NO_AUTH` |
| AWS region and endpoint | `--s3-region`, `--s3-endpoint` | `s3_region`, `s3_endpoint` | `RUSTDB_S3_REGION`, `RUSTDB_S3_ENDPOINT` |
| S3 request mode | `--s3-path-style`, `--s3-allow-http`, `--s3-anonymous` | `s3_path_style`, `s3_allow_http`, `s3_anonymous` | `RUSTDB_S3_PATH_STYLE`, `RUSTDB_S3_ALLOW_HTTP`, `RUSTDB_S3_ANONYMOUS` |

The S3 settings apply to server-local registered sources. `--s3-allow-http` is
only for trusted development endpoints, and `--s3-anonymous` is only for public
objects; credentials otherwise come from the server process's default provider
chain.

`/healthz` and `/readyz` are unauthenticated but disclose only `ok`, `ready`, or
`not_ready`. Unless explicit no-auth development mode is enabled, every `/v2`
request is authenticated before its body or query parameters are parsed. The
Prometheus `/metrics` endpoint also requires authentication and Admin
permission. CORS is disabled. Access logs do not include request bodies or SQL
text; private rotating JSONL audit records contain identity and SQL fingerprints.

The RSS guard samples the process against the smaller of physical memory and
the active Linux cgroup limit. Its default 70/80/90% watermarks throttle new
submissions, reject new submissions, and cancel the largest live Query. HTTP
rejections include `Retry-After`; idempotent replays are resolved before this
admission check. Blocking result and service-state work uses a separate bounded
pool rather than compute lanes.

Offline principal and role changes still require the server to be stopped.
Running servers accept only a narrow local Admin-socket protocol for status,
token reload/rotation/revocation, and bounded shutdown:

```sh
rustdb service status --database /srv/rustdb/analytics
rustdb service rotate-token --database /srv/rustdb/analytics --principal analyst
rustdb service revoke-token --database /srv/rustdb/analytics --token-id <UUID>
rustdb service shutdown --database /srv/rustdb/analytics
```

The socket is a mode-`0600` Unix socket inside the private per-database state
directory unless explicitly overridden. Rotation adds an overlapping Token;
export or distribute it, verify clients, then revoke the old Token. The
long-lived CA remains unchanged. RustDB periodically renews a near-expiry leaf
certificate and hot-reloads it without stopping the listener.

## Common failures

| Symptom | Meaning and action |
| --- | --- |
| TLS hostname error | Connect through the configured advertise URL. If that URL changed, stop the server, remove only the generated bundle path printed by `rustdb serve`, restart, then export/import the new bundle. |
| `401` | Import the current profile or redistribute it after Token rotation. |
| `409 idempotency.key_conflict` | Generate a new key, or retry with the same decoded request envelope. |
| `409 query.not_complete` | Poll status before requesting JSON/NDJSON; use sequenced Arrow IPC for committed running output. |
| `429 admission.queue_full` | The admission queue is full; wait for `Retry-After`. |
| `429 admission.rss_throttled` | Process RSS crossed the warning watermark; wait for `Retry-After` and reduce concurrency. |
| `503 admission.rss_rejected` | Process RSS crossed the high/critical watermark; stop submitting work and investigate memory pressure. |
| `422 sql.unsupported` | Use an allowed read-only statement and only registered relations. |
| `query.resource_exhausted` | Reduce the result, consume/delete retained results, or move `--result-directory` to a suitable filesystem. |
| Query was active during restart | Queued/running work becomes terminal `interrupted` with `query.interrupted`. Any committed Arrow batches remain readable as an explicitly incomplete prefix; submit a new request for a full result. |
| Completed Query missing after restart | It expired, was deleted, failed validation, or is owned by another principal. Review service logs before resubmitting. |

Errors use a stable string `error` code, human-readable `message`, mandatory
`retry` classification, and optional `request_id`, `query_id`, and structured
`details`. Clients must not infer retry safety from the message. Include IDs—not
credentials or profile bundles—when collecting diagnostics.

```json
{
  "error": "sql.unsupported",
  "message": "HTTP queries allow SELECT, WITH, VALUES, SHOW, DESCRIBE, EXPLAIN, and EXPLAIN ANALYZE only",
  "retry": "never",
  "request_id": "9ae24e09597d4b28aec5a455263a70b2"
}
```
