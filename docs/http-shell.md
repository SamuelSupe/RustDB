# RustDB read-only HTTP Shell

RustDB v0.9 can expose one Native database to the existing command-line query
experience over HTTPS. The server executes only read-only SQL. It is not a
browser shell, a write API, or a remote administration service.

For the exact wire contract, see [OpenAPI v1](openapi-v1.yaml). For the design
and deliberate limits, see [the v0.9 roadmap](roadmap-v0.9.md). A Simplified
Chinese version of this guide is available in
[http-shell.zh-CN.md](http-shell.zh-CN.md).

## Start a local server

The safest default listens only on loopback:

```sh
rustdb serve --database /srv/rustdb/analytics
```

First startup creates a local CA, a short-lived server certificate, a random
Bearer Token, and connection metadata in the operating-system user state
directory. Secret material is not written into the Native database directory.
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

## Query from the CLI

Open the interactive shell:

```sh
rustdb shell --profile analytics
```

Although the protocol creates a background Query ID, the official shell waits,
polls, fetches immutable pages, and renders them like a local query. `Ctrl-C`
requests remote cancellation. There are no job-management backslash commands.

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
  https://analytics.example.com:7400/v1/queries
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
    https://analytics.example.com:7400/v1/queries |
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
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}"
```

Status timestamps are Unix-epoch milliseconds in `created_at_ms`, optional
`started_at_ms`, and optional `finished_at_ms`. Successful Queries include the
bounded `metrics` object; failed or cancelled Queries instead include `error`.
Absent optional fields are omitted rather than encoded as JSON `null`.

Fetch JSON by offset:

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/json' \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/results?offset=0&limit=1000"
```

Or fetch NDJSON with an opaque Cursor returned by the previous page:

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/x-ndjson' \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/results?cursor=${CURSOR}&limit=1000"
```

Cursor and offset are mutually exclusive. Pages are immutable and repeatable;
the server does not advance shared fetch state. Results become readable only
after successful completion.

Cancel queued or running work, then delete terminal state:

```sh
curl -X POST --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/cancel"

curl -X DELETE --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}"
```

## Result encoding

JSON pages have the shape `{schema, rows, page}`. NDJSON starts with one
`schema` record, emits a `row` array for every row, and ends with one `page`
record. Rows are positional arrays so duplicate column names remain valid.

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

Important defaults are one running Query, a FIFO queue of 64, a 30-minute Query
timeout, one-hour result TTL, and result storage limited to the smaller of
10 GiB and 10% of its filesystem. One Query may use at most 25% of that global
result limit.

Use these `serve` options when the defaults do not fit the deployment:

| Concern | CLI | TOML (`[server]` / `[engine]`) | Environment |
| --- | --- | --- | --- |
| Result location and retention | `--result-directory`, `--result-ttl-secs` | `result_directory`, `result_ttl_secs` | `RUSTDB_RESULT_DIRECTORY`, `RUSTDB_RESULT_TTL_SECS` |
| Result total/per-Query quota | `--result-global-limit`, `--result-query-limit` | `result_global_limit`, `result_query_limit` | `RUSTDB_RESULT_GLOBAL_LIMIT`, `RUSTDB_RESULT_QUERY_LIMIT` |
| AWS region and endpoint | `--s3-region`, `--s3-endpoint` | `s3_region`, `s3_endpoint` | `RUSTDB_S3_REGION`, `RUSTDB_S3_ENDPOINT` |
| S3 request mode | `--s3-path-style`, `--s3-allow-http`, `--s3-anonymous` | `s3_path_style`, `s3_allow_http`, `s3_anonymous` | `RUSTDB_S3_PATH_STYLE`, `RUSTDB_S3_ALLOW_HTTP`, `RUSTDB_S3_ANONYMOUS` |

The S3 settings apply to server-local registered sources. `--s3-allow-http` is
only for trusted development endpoints, and `--s3-anonymous` is only for public
objects; credentials otherwise come from the server process's default provider
chain.

`/healthz` and `/readyz` are unauthenticated but disclose only `ok`, `ready`, or
`not_ready`. Every `/v1` request is authenticated before its body or query
parameters are parsed. CORS is disabled. Access logs do not include request
bodies or SQL text.

Rotate the Token only while the server is stopped. The old Token becomes
invalid immediately, so export and redistribute a new profile bundle. The
long-lived CA remains unchanged; the server renews its short-lived leaf
certificate at startup.

## Common failures

| Symptom | Meaning and action |
| --- | --- |
| TLS hostname error | Connect through the configured advertise URL. If that URL changed, stop the server, remove only the generated bundle path printed by `rustdb serve`, restart, then export/import the new bundle. |
| `401` | Import the current profile or redistribute it after Token rotation. |
| `409 idempotency.key_conflict` | Generate a new key, or retry with the same decoded request envelope. |
| `409 query.not_complete` | Poll status; results are not exposed while execution is active. |
| `429` | The 64-entry queue is full; wait for `Retry-After`. |
| `422 sql.unsupported` | Use an allowed read-only statement and only registered relations. |
| `query.resource_exhausted` | Reduce the result, consume/delete retained results, or move `--result-directory` to a suitable filesystem. |
| Query missing after restart | Query jobs and retained results are intentionally process-local. Resubmit with a new Idempotency Key. |

Errors use a stable string `error` code, human-readable `message`, and optional
`request_id`, `query_id`, and structured `details`. Include IDs—not credentials
or profile bundles—when collecting diagnostics.

```json
{
  "error": "sql.unsupported",
  "message": "HTTP queries allow SELECT, WITH, VALUES, SHOW, DESCRIBE, EXPLAIN, and EXPLAIN ANALYZE only",
  "request_id": "9ae24e09597d4b28aec5a455263a70b2"
}
```
