# RustDB binary distribution

[简体中文](README.zh-CN.md)

This archive contains the standalone `rustdb` command for querying local and
S3-compatible CSV/Parquet data, managing a persistent local Native OLAP
database, and exposing that database through the Beta read-only HTTPS Shell.
The engine remains single-node and embedded; the optional server is one
`rustdb serve` process for one Native database.

## Quick start

Run from the extracted directory:

```sh
./bin/rustdb --version
./bin/rustdb -c "SELECT count(*) FROM read_parquet('/data/*.parquet')"
./bin/rustdb --database ./warehouse -c "CREATE TABLE events AS SELECT * FROM read_parquet('/data/events.parquet')"
./bin/rustdb serve --database ./warehouse
```

Or install it:

```sh
./install.sh
rustdb --help
```

Use `./install.sh --prefix "$HOME/.local"` for an unprivileged user install.
See [docs/INSTALL.md](docs/INSTALL.md) for checksum, installation, upgrade, and
uninstallation instructions. [docs/CLI.md](docs/CLI.md) documents query and
resource options. [docs/http-shell.md](docs/http-shell.md) covers TLS startup,
offline Profiles, server-local data sources, the remote CLI, and raw HTTP use.
[docs/operator-guide.md](docs/operator-guide.md) covers supported deployments,
authentication, metrics, audit, backup, repair, and incident response.

## Beta 2 operational boundary

Beta 2 is a fresh-start format release. It creates Native marker epoch `4` and
requires service configuration `schema_version = 2`; it does not convert older
database epochs or configuration schemas. Re-import CSV/Parquet into a fresh
database. The remote Shell contract is `/v2` only.

`rustdb backup` creates one verified Native plus safe HTTP-control-state bundle;
`backup-check` validates it without restore, and `restore` requires fresh
database and service-state targets. Query journals/results, audit, Spill,
temporary data, locks, and the Admin socket are excluded. Online local
administration uses the private Unix socket through `rustdb service`; offline
`service check`/`repair` validates marker-owned state. The server also includes
70/80/90% RSS pressure protection, periodic TLS renewal with hot reload, and a
bounded service I/O pool.

## Package contents

- `bin/rustdb`: release CLI executable;
- `install.sh` and `uninstall.sh`: bilingual POSIX installation helpers;
- `docs/CLI*.md`: English and Simplified Chinese CLI help;
- `docs/INSTALL*.md`: English and Simplified Chinese installation help;
- `docs/http-shell*.md`: English and Simplified Chinese HTTPS Shell help;
- `docs/operator-guide*.md`: English and Simplified Chinese operations guide;
- `docs/diagnostics*.md`, `docs/native-import.md`, `docs/native-repair.md`, and
  `docs/compatibility.md`: support, import, recovery, and compatibility references;
- `docs/troubleshooting.md`, migration, Parquet pruning, and S3 references;
- `packaging/config/rustdb.example.toml`: validated schema-version 2 example;
- `docs/openapi-v2.yaml`: the public OpenAPI 3.1 contract;
- `RELEASE-NOTES.md`: version-specific changes and known limits;
- `SHA256SUMS`: checksums for every installed payload;
- `VERSION` and `LICENSE`.

The Rust library API and source code are distributed through the source
repository rather than this binary archive. Windows binaries are not provided
in the current release line.

## Security

S3 credentials come from the AWS default credential chain. Do not place access
keys in SQL, shell history, or command-line options. Plain HTTP S3 endpoints
require the explicit `--s3-allow-http` flag and should be limited to trusted
development networks.

The remote Shell always uses TLS and digest-backed principal tokens. It is read-only:
it cannot run DDL/DML, upload files, manage data sources, or call direct file
table functions. Keep exported Profiles and service backup bundles private.
