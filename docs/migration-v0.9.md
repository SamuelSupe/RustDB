# Migrating from v0.8 to v0.9

RustDB v0.9 adds an optional read-only HTTP Shell around one Native database.
It does not expose the v0.8 write, transaction, maintenance, or source-management
surface remotely.

## Native storage compatibility

v0.9 does not rewrite Native table segments, delete vectors, the database
marker, or the WAL solely to enable HTTP serving. An existing v0.8 Native
database can be opened directly by `rustdb serve`; there is no mandatory
`rustdb migrate` step.

Persistent CSV/Parquet source registrations use the optional, atomically
replaced `catalog/external-sources.json` Catalog sidecar. Existing databases
without that file start with no remote-visible external sources. The sidecar
does not change a Native format version, is included in Native backups, and
never persists object-store credentials.

As with every alpha upgrade, take and validate a Native backup before the first
v0.9 metadata write. Never modify Catalog files by hand.

## Existing Rust and local CLI users

The embedded `Engine`, `Session`, `PreparedStatement`, and streaming
`QueryResult` APIs remain available. Local SQL continues to support the v0.8
write and maintenance features. The new HTTP process is an optional deployment
mode, not a replacement for embedding RustDB.

The remote boundary is intentionally narrower:

- one read-only statement per Query;
- no HTTP Session or transaction state;
- no remote DDL, DML, maintenance, upload, or source administration;
- no direct `read_csv(...)` or `read_parquet(...)` path/URI access;
- no browser UI, CORS, or generated language SDK.

Applications built against an earlier, broader v0.9 HTTP proposal must not use
that unpublished design. The supported protocol is the background-Query API in
[openapi-v1.yaml](openapi-v1.yaml).

## Upgrade procedure

1. Stop processes that have the Native database open and complete or roll back
   local transactions.
2. Create and verify a v0.8 Native backup.
3. Install the v0.9 binary and run the normal local query smoke check.
4. While the server is stopped, add only the CSV/Parquet sources that remote
   users need. Keep S3 credentials in the service account's default provider.
5. Start `rustdb serve --database PATH` on loopback first. Confirm `/healthz`,
   `/readyz`, and authenticated `/v1/info`.
6. For remote listening, configure both `--listen` and an HTTPS
   `--advertise-url` whose host is reachable by clients.
7. Export the generated connection bundle through a trusted channel, import it
   as a named Profile, and run one read-only query through `rustdb shell`.

First startup creates server connection state in the operating-system user
state directory keyed by Native Database ID. The CA, leaf key, Token, Profile
metadata, and HTTP result files are outside the Native database and Native
backups.

## Configuration migration

HTTP server settings support TOML, `RUSTDB_*` environment variables, and CLI
arguments. Their precedence is:

```text
CLI > environment > TOML > defaults
```

Move Token bytes into a permission-restricted Token file. v0.9 intentionally
has no plaintext Token flag. The listener defaults to `127.0.0.1`; changing it
to a non-loopback address without a valid `--advertise-url` is rejected.

The default service policy is one active Query, a 64-entry FIFO queue, a
30-minute timeout, and one-hour ephemeral result retention. Result storage is
separate from execution Spill and must have enough private disk space for the
smaller of 10 GiB and 10% of its filesystem, subject to the 25% per-Query
limit. Tune these values before production evaluation rather than assuming the
embedded Engine's Spill settings govern HTTP results.

## Client migration

Import a named Profile, then replace a local invocation such as:

```sh
rustdb --database /srv/rustdb/analytics -f report.sql --format csv
```

with:

```sh
rustdb shell --profile analytics -f report.sql --format csv
```

Interactive and script output remains `table`, `csv`, or `jsonl`. Internally,
the remote CLI now submits a Query with an Idempotency Key, polls status,
fetches pages, and deletes a successfully consumed result. `Ctrl-C` performs a
best-effort remote cancellation.

Raw HTTP clients must:

- trust the exported CA and send `Authorization: Bearer`;
- send a unique `Idempotency-Key` with every submission;
- tolerate new response fields within `/v1`;
- poll the Query until terminal before fetching results;
- choose JSON or NDJSON and paginate with either Cursor or offset;
- explicitly cancel running work and delete terminal results when no longer
  needed;
- parse `error`, `message`, and optional Request/Query IDs from failures.

## Token and certificate operations

The CA is long-lived. The server renews a short-lived leaf certificate at
startup without requiring clients to trust another CA. Rotate the Bearer Token
only while the server is stopped; the old Token stops working immediately and
all clients need a newly exported Profile bundle.

v0.9 stores Profile Token files with user-only permissions but does not use
macOS Keychain or Linux Secret Service. Protect the Profile bundle like a
password and never commit it to a repository.

## Rollback

1. Stop `rustdb serve` and wait for it to quiesce.
2. Preserve logs needed for diagnosis, but do not copy Token or private-key
   material into support bundles.
3. Remove no Native files by hand. A v0.8 binary ignores the additive external
   source sidecar but cannot use those registrations.
4. If any other newer-alpha Catalog change was committed, restore the validated
   v0.8 backup before using an older alpha.

HTTP Query IDs and temporary results are process-local and intentionally do not
survive either upgrade or rollback. Resubmit required queries with new
Idempotency Keys.
