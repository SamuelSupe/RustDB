# RustDB v0.9.0-alpha.1 roadmap: read-only HTTP Shell

This document is the v0.9 implementation contract. It replaces the earlier
full HTTP database API proposal. v0.9 keeps the existing CLI experience and
adds a small, secure, read-only HTTP protocol behind it; it is not a graphical
UI or a general remote database administration API.

## Release objective

One long-running process opens one Native database:

```text
rustdb shell --profile analytics
             |
             | HTTPS + Bearer Token
             v
rustdb serve --database /srv/analytics
             |
             +-- bounded background Query jobs
             +-- read-only SQL policy
             +-- existing RustDB Engine
             +-- temporary indexed Arrow IPC results
```

The interactive and script CLI waits internally for each background Query,
downloads immutable result pages, renders the existing `table`, `csv`, or
`jsonl` format, and deletes a fully consumed result. Users do not manage jobs
and v0.9 provides no `\jobs`, `\fetch`, or other job commands. `Ctrl-C` sends a
best-effort remote cancellation request before returning control to the CLI.

There is no performance gate for v0.9. The release gate is one complete
correctness and resource-lifecycle pass.

## Public HTTP surface

The public protocol is described by `docs/openapi-v1.yaml` and is versioned
under `/v1`:

```text
GET    /healthz
GET    /readyz
GET    /v1/info
POST   /v1/queries
GET    /v1/queries/{query_id}
GET    /v1/queries/{query_id}/results
POST   /v1/queries/{query_id}/cancel
DELETE /v1/queries/{query_id}
```

`/healthz` and `/readyz` are unauthenticated and disclose only a minimal
health state. Every `/v1` route requires TLS and the single server Bearer
Token. `/v1/info` lets the official CLI reject an incompatible protocol before
submitting SQL.

Every `POST /v1/queries` requires an `Idempotency-Key`. Reusing a key with the
same decoded request envelope returns the original Query ID; using it with a
different envelope returns `409 idempotency.key_conflict`. The key record lives
with the ephemeral Query and disappears when the Query is deleted or expires.

The request contains exactly one SQL statement, optional typed positional
parameters, and an optional timeout no longer than the server's 30-minute
default. Request bodies are limited to 1 MiB. Parameters use `?` or `$n` with
the existing Rust parameter semantics; one statement cannot mix the two
styles. Parameters replace expression values only, never identifiers, paths,
source patterns, or options.

Accepted statements are limited to:

- `SELECT`, including `WITH` queries;
- `VALUES`;
- `SHOW` and `DESCRIBE`;
- `EXPLAIN` and `EXPLAIN ANALYZE`.

The policy is checked on the parsed and bound statement and again before
physical execution. Text matching is not a security boundary. DDL, DML,
transaction commands, maintenance commands, side-effecting functions,
`read_csv(...)`, `read_parquet(...)`, and unregistered relations are rejected
even when nested in a CTE, subquery, or View.

## Query lifecycle

A valid submission returns an unguessable Query ID and enters a bounded FIFO
queue. The default server runs one Query and queues at most 64; a full queue
returns `429` with `Retry-After`. There is no query-list endpoint.

```text
queued -> running -> succeeded
   |         |
   +---------+-> cancelled
   +---------+-> failed
```

The status resource returns state, timestamps, a stable terminal error, and a
bounded metric summary: scanned/output rows and bytes, peak memory, and Spill
read/write/peak bytes. It never exposes the complete operator profile.

Results are unreadable until the Query has succeeded. Cancellation and
deletion are separate idempotent operations: `POST .../cancel` cancels queued
or running work; `DELETE` removes only a terminal Query and its result, and
returns `409` for running work. Query state and results are process-local and
are not recovered after a server restart.

`QueryResult::cancel`, disconnects, timeout, errors, and shutdown must all
converge through the Engine TaskGroup. Temporary results are removed only after
workers are quiescent.

## Result storage and pagination

The HTTP server fully materializes a successful Query into private, indexed
Arrow IPC fragments. It does not store a JSON or NDJSON copy. These files are
separate from execution Spill and use a private query directory (`0700`) and
files (`0600`). Static encryption is deferred; deployments rely on host and
filesystem security.

Defaults:

- result TTL: one hour;
- global result quota: the smaller of 10 GiB and 10% of filesystem capacity;
- per-Query quota: 25% of the global result quota;
- default page: 1,000 rows;
- maximum page: 10,000 rows and 16 MiB after HTTP encoding.

Quota or free-space exhaustion fails the Query with a structured resource
error. Startup scavenging removes only valid RustDB-owned stale result
directories. A successful official CLI fetch explicitly deletes the terminal
Query. Interrupted output leaves the immutable result available until TTL so
the same page can be retried.

The result endpoint accepts either an opaque Cursor or `offset + limit`.
Cursor and offset are mutually exclusive. Reads do not advance server-side
state, so multiple clients may independently and repeatedly fetch a page.

Content negotiation supports `application/json` and
`application/x-ndjson`. JSON uses `{schema, rows, page}`. NDJSON emits one
`schema` record, one `row` record per row, and one terminal `page` record.
Rows are arrays so column order and duplicate names are preserved.

Values use natural JSON guided by the separate Arrow schema:

- integers, floats, and Decimal are JSON numbers;
- `NaN`, positive infinity, and negative infinity are the strings `"NaN"`,
  `"Infinity"`, and `"-Infinity"`;
- Date uses `YYYY-MM-DD`;
- a timezone-free Timestamp uses ISO microsecond text without `Z`;
- a timezone-aware Timestamp uses RFC 3339;
- Binary uses Base64.

The official CLI parses number tokens with the schema and preserves Int64,
UInt64, and Decimal exactly. Third-party clients that decode every number as a
binary float accept possible precision loss. `Accept-Encoding: gzip` enables
response compression; small responses remain uncompressed.

## TLS, Token, and profiles

`rustdb serve` defaults to `127.0.0.1`. A non-loopback listener requires an
explicit `--advertise-url https://HOST:PORT`; `0.0.0.0` is never written to a
client profile. First startup creates a long-lived local CA, renewable
short-lived server certificate, random Token, and connection metadata in an
OS user state directory keyed by Database ID. None of these files are stored
inside the Native database or included in a Native backup.

The CA remains stable while the leaf certificate is renewed at startup. Token
rotation is an offline operation: stop the server, rotate locally, redistribute
a profile bundle, and restart. There is no grace period for the old Token.

Profiles contain the advertised URL plus paths to CA and Token files. An admin
exports a permission-restricted bundle and transfers it out of band; a client
imports it under a name and connects with `rustdb shell --profile NAME`.
v0.9 does not integrate with OS keychains. CORS is always disabled.

Server configuration may come from TOML, `RUSTDB_*` environment variables, or
CLI flags, with this precedence:

```text
CLI > environment > TOML > defaults
```

Token bytes are accepted only from a permission-restricted file, never a
plaintext CLI option. Access logs record Request ID, Query ID, SQL fingerprint,
state, duration, and resource metrics, but not SQL text unless an explicit
debug option is enabled.

All `/v1` requests authenticate before their body or query parameters are
parsed. Result retention can be configured with `--result-directory`,
`--result-ttl-secs`, `--result-global-limit`, and `--result-query-limit` (or
the equivalent TOML/environment settings). Server-local registered S3 sources
can likewise receive region, endpoint, path-style, explicit HTTP, and anonymous
settings through `serve`; explicit HTTP is for trusted development endpoints
only and anonymous mode is for public objects only.

## Registered sources

Remote SQL can access Native tables, system tables, and persistent server-local
CSV/Parquet source registrations. It cannot submit a local path or S3 URI.
Source definitions are changed only by a local CLI while `rustdb serve` is
stopped; restarting the server captures the new Catalog. Credentials remain in
the server's default credential provider and are never persisted in source
metadata.

## Implementation boundaries

Keep HTTP code out of the Engine and split it into focused modules for config,
TLS, authentication, routing/models, admission, lifecycle, result storage, and
encoding. The HTTP layer must reuse `Session::prepare`, `QueryResult::stream`,
TaskGroup cancellation, memory accounting, and Native Catalog snapshots. No
project-owned `unsafe` and no DataFusion are introduced.

## Acceptance

One OrbStack acceptance run must cover:

1. first-start CA/certificate/Token generation and leaf renewal;
2. offline profile export/import and protocol negotiation;
3. TLS and Token rejection, loopback default, and non-loopback advertise URL;
4. read-only AST/bound-plan enforcement, including nested bypass attempts;
5. typed `?` and `$n` parameters and idempotent submission conflict handling;
6. queue, execution, success, failure, timeout, cancellation, and shutdown;
7. JSON/NDJSON cursor and offset pagination, gzip, repeat reads, and all CLI
   output formats;
8. result TTL, quota rejection, automatic deletion, interruption retention,
   restart scavenging, and zero residual task/reservation/result files;
9. OpenAPI validation and bilingual CLI/document examples.

There is no soak test, throughput target, browser validation, or DuckDB
comparison in this release gate.

## Explicitly deferred

- browser UI and CORS;
- write APIs, remote DDL/DML, transactions, and Session state;
- remote source administration and client file upload;
- multi-user identity, RBAC, OAuth/OIDC, and audit identities;
- generated language SDKs;
- WebSocket, SSE, Arrow Flight, and PostgreSQL Wire Protocol;
- durable Query recovery and server-side result streaming before completion;
- multi-database serving, replication, and distributed execution.
