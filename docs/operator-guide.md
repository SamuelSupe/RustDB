# RustDB Beta operator guide

This guide covers `v1.0.0-beta.1`. RustDB Beta is pre-production software: it
has a defined compatibility and operations contract, but no production SLA.
Run it as a single-node service with recoverable source data and tested backups.

## Support matrix

| Area | Beta status |
| --- | --- |
| Linux x86_64 and AArch64 | Supported binary and non-root OCI targets |
| macOS Apple Silicon | Supported for embedded use, CLI, and development |
| Windows and macOS Intel | Unsupported |
| Local storage | A local filesystem with atomic rename, `fsync`, and advisory file locks |
| AWS S3 | Supported through the AWS default credential chain |
| MinIO | Tested with `RELEASE.2025-04-22T22-12-26Z` and path-style access |
| Other S3-compatible stores | Best effort; validate consistency, multipart, and ETag behavior first |
| Data formats | CSV, gzip CSV, zstd CSV, Parquet, and local Native storage |

Keep a Native database on local block storage. Network filesystems whose lock,
rename, or durability behavior is unknown are not supported. S3 is used for
CSV/Parquet sources and verified backup/restore, not as the live Native volume.

## Filesystem layout

Use a dedicated operating-system account and separate capacity budgets:

```text
/srv/rustdb/database/   Native database
/srv/rustdb/state/      TLS, principals, profile tokens, audit log
/srv/rustdb/results/    retained HTTP query results
/srv/rustdb/spill/      temporary operator Spill
/etc/rustdb/            read-only service configuration
```

Directories containing database, security state, results, or Spill should be
mode `0700`; sensitive files are mode `0600`. Do not edit files inside a Native
database. Native backup does not include HTTP security state, audit records, or
retained query results.

Plan capacity for the retained database plus publication headroom, results,
Spill, and a backup. Native and Spill admission each retain at least 10% and
1 GiB by default. Result quotas are separate. Monitor all filesystems rather
than assuming the engine memory limit is a process RSS or disk limit.

## Binary and OCI deployment

Validate the packaged archive with its SHA-256 file and `scripts/dist/check.sh`
before installation. Run the OCI image as UID/GID `10001:10001`; the image does
not require root and `/usr/local/bin/rustdb` is read-only to that user.

Example hardened container invocation:

```sh
docker run --rm --init --read-only \
  --user 10001:10001 \
  --stop-timeout 35 \
  --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -p 7400:7400 \
  -v /srv/rustdb:/var/lib/rustdb \
  -v /etc/rustdb:/etc/rustdb:ro \
  ghcr.io/samuelsupe/rustdb:v1.0.0-beta.1 \
  --spill-directory /var/lib/rustdb/spill \
  serve \
  --database /var/lib/rustdb/database \
  --config /etc/rustdb/rustdb.toml
```

Create and chown the host directories before starting the container. Keep the
root filesystem read-only and mount only the four writable data directories.
Send `SIGTERM` and allow at least the fixed 30-second shutdown grace; do not use
`SIGKILL` during normal operation.

## Configuration

Start from [`packaging/config/rustdb.example.toml`](../packaging/config/rustdb.example.toml).
The top-level `schema_version = 1` is mandatory and unknown fields are rejected.

```sh
rustdb config validate /etc/rustdb/rustdb.toml
rustdb --log-format json serve \
  --database /srv/rustdb/database \
  --config /etc/rustdb/rustdb.toml
```

Precedence is `CLI > environment > TOML > defaults`. Prefer TOML for stable,
non-secret policy, environment variables for deployment-specific values, and
CLI flags for an intentional one-run override. S3 credentials are never placed
in TOML, SQL, endpoint URLs, or CLI arguments; use the AWS default credential
chain. Plain HTTP S3 endpoints require the explicit trusted-development option.

For a 4-core/16-GiB host, begin with four compute threads and a 2–4 GiB engine
memory limit. Set explicit result and Native quotas. Increase concurrency only
after observing queueing, result-disk growth, and peak memory under the actual
query mix.

## Authentication and authorization

Authentication is enabled by default. First start creates an `admin` principal
and one random profile token. `principals.json` stores SHA-256 digests only;
clear-text profile tokens are separate private files and are never logged.

Stop the server before local identity administration:

```sh
rustdb principal list --database /srv/rustdb/database --state-root /srv/rustdb/state
rustdb principal create --database /srv/rustdb/database \
  --state-root /srv/rustdb/state --id reporting --role query
rustdb token list --database /srv/rustdb/database \
  --state-root /srv/rustdb/state --principal reporting
rustdb token rotate --database /srv/rustdb/database \
  --state-root /srv/rustdb/state --principal reporting
rustdb profile export --database /srv/rustdb/database \
  --state-root /srv/rustdb/state --token-id TOKEN_ID \
  --output reporting.rustdb-profile
rustdb token revoke --database /srv/rustdb/database \
  --state-root /srv/rustdb/state --token-id TOKEN_ID
```

Rotation adds an overlapping token; distribute the new profile, verify it, and
then revoke the old token. Token listing is safe for inventory: it emits UUID,
principal, lifecycle state, and validity only. Profile export by token UUID
reuses the managed connection bundle's URL and CA; pass `--server-url` when an
explicit HTTPS origin is required. Query principals can access only their own
query IDs. Admin principals can access every query and `/metrics`. The final
enabled Admin cannot be disabled or demoted.

`--no-auth` is a development-only escape hatch. It grants query access but no
Admin identity, so `/metrics` remains forbidden. Never use it on a non-loopback
listener or an untrusted host.

## Health, metrics, logs, and audit

- `GET /healthz` reports process liveness and `GET /readyz` reports readiness;
  both are unauthenticated and intentionally disclose only minimal state.
- `GET /metrics` uses Prometheus text format and requires an Admin bearer token.
- `--log-format json` writes structured events to stderr. Set `RUST_LOG` for
  filtering; logs use SQL fingerprints instead of SQL text.
- `<state-root>/<database-id>/audit.jsonl` records authentication, principal,
  token, query, and server lifecycle events. It is private, synced per record,
  rotates at 64 MiB, and retains one rotated file.

Example Admin scrape:

```sh
curl --fail --cacert /secure/ca.pem \
  -H "Authorization: Bearer $(< /secure/admin.token)" \
  https://analytics.example.com:7400/metrics
```

Alert on readiness failure, query rejections/failures, sustained running-query
growth, result and database filesystem pressure, and audit-write errors in the
service log. Metrics are low-cardinality process counters and gauges; they do
not contain principal or query-ID labels. The Beta audit log is operational
evidence, not a compliance-grade immutable ledger.

## Check, diagnostics, repair, and backup

Run integrity checks with the server stopped:

```sh
rustdb native check /srv/rustdb/database
rustdb native check /srv/rustdb/database --json
rustdb diagnostics --database /srv/rustdb/database \
  --output /secure/rustdb-diagnostics.json
```

`native check` is read-only. Diagnostics produces a versioned, redacted JSON
summary; it hashes the database path and omits credentials, endpoint values,
table names, and sensitive paths. Review it before sharing through a trusted
support channel.

Repair is plan-first and deliberately narrow:

```sh
rustdb native repair /srv/rustdb/database --json
rustdb native repair /srv/rustdb/database --apply --json
rustdb native check /srv/rustdb/database
```

Always take a full verified backup before `--apply`. The repair command creates
only a metadata backup, revalidates its plan under the database lock, and never
reconstructs missing user data. See [Native check and repair](native-repair.md).

Backup and restore into a new, empty destination:

```sh
rustdb backup /srv/rustdb/database /backup/rustdb-2026-07-19
rustdb --s3-region us-east-1 backup \
  /srv/rustdb/database s3://backup-bucket/rustdb/2026-07-19
rustdb restore /backup/rustdb-2026-07-19 /srv/rustdb/restore-check
rustdb native check /srv/rustdb/restore-check
```

The manifest is published only after backup validation; restore refuses to
replace an existing destination. Periodically perform a full restore and query
checksum check. Configure an S3 incomplete-multipart lifecycle policy and use a
dedicated empty prefix per backup.

## Incident sequence

1. Stop new traffic and preserve the first stable error code, request ID, and
   query ID; never collect credentials or raw profile bundles.
2. Send `SIGTERM` and wait for shutdown. Copy logs and the private audit files.
3. Run `native check --json` and `diagnostics` without opening the database for
   writes.
4. Restore the latest backup to a new path. Do not overwrite the source.
5. Use repair only when its read-only plan is understood and a full backup
   exists. Escalate corruption that repair refuses.

## Beta release gate and SLA

Run the single auditable gate from a clean, committed worktree on a host with at
least four logical CPUs and 16 GiB RAM. Inputs are deliberately explicit: the
script never downloads, synthesizes, or silently skips a release fixture.

```sh
export RUSTDB_BETA_ACCEPTANCE_OUTPUT=/srv/rustdb-evidence/beta-2026-07-19
export RUSTDB_BETA_LOCAL_FIXTURE=/srv/rustdb-fixtures/lake-local
export RUSTDB_BETA_LOCAL_FORMAT=parquet       # csv or parquet
export RUSTDB_BETA_MINIO_MANIFEST=/srv/rustdb-fixtures/minio-manifest.json
export RUSTDB_BETA_MINIO_FORMAT=parquet       # csv or parquet
export RUSTDB_BETA_CLICKBENCH_DATA_DIR=/srv/rustdb-fixtures/clickbench
export RUSTDB_BETA_CLICKBENCH_PROFILE=functional
scripts/ci/beta_acceptance.sh
```

The output must be an absolute, previously nonexistent directory outside the
Git worktree. Both lake fixtures must contain at least 100 GiB and 10,000
flat, regular files/objects, all in the selected format. The gate generates its
own `count(*)` query over the complete read-only `/beta-data` mount and MinIO
prefix. The MinIO manifest has this credential-free, ETag-bound form:

```json
{
  "schema": "rustdb-beta-object-manifest-v1",
  "root_uri": "s3://bucket/prefix",
  "objects": [
    {"uri": "s3://bucket/prefix/part-000.parquet", "size": 123, "etag": "..."}
  ]
}
```

Use equivalent local and MinIO data: the gate requires one checksum across all
four 2/4-GiB, eight-client runs. It revalidates the local size/mtime inventory;
before and after the MinIO runs, it lists MinIO and requires every URI, size,
and ETag to match the manifest. Every query must also discover the manifest's
exact file count. ClickBench requires the preloaded SHA-256-pinned functional
fixture and canonical `queries.sql`; the repository supplies the versioned
deterministic derivative used for execution. The gate binds both query
identities and runs ClickBench with four CPUs, a 12-GiB container limit, a
4-GiB engine budget, batch 8192, and I/O concurrency 16. It runs
`scripts/ci/orbstack.sh all` once, those four external runs, and one ClickBench
pass. It records the commit, host/profile, normalized fixture inventories,
commands, logs, runner build ID, resource summaries, and outcome in
`<output>/evidence.json`. A failed or interrupted run also writes failed
evidence and must not be reused. Dedicated low-memory Spill stress is not
repeated.

The 100-GiB `count(*)` path is the discovery, snapshot, metadata, concurrency,
and memory-accounting part of the gate. The functional ClickBench pass checks
real Parquet data-page execution against 43 pinned row-count and typed-checksum
oracle entries. The canonical official queries remain pinned for the source and
full profile; the deterministic derivative adds complete tie-breakers only
where bounded canonical results leave ties unspecified. Oracle generation and
review use exactly one rowset from a digest-pinned `clickhouse-local` image.
ClickHouse is not run by the release gate, and neither part is a cross-engine
performance comparison.

The annotated release tag must bind the accepted commit:

```text
RustDB-Acceptance-SHA: <40-hex commit>
RustDB-Acceptance-Status: passed
```

The distribution workflow refuses release without those exact trailers. This
is a one-time release gate, not a soak test, availability target, latency SLO,
or durability SLA. Beta users must retain source data, backups, and a rollback
path. Inspect `evidence.json`, confirm `status: passed` and its
`accepted_commit`, then copy that exact SHA into the annotated tag trailer.

See also the [HTTP Shell guide](http-shell.md), [compatibility matrix](compatibility.md),
[troubleshooting guide](troubleshooting.md), and [Beta migration guide](migration-v1-beta.md).
