# RustDB binary distribution

[简体中文](README.zh-CN.md)

This archive contains the standalone `rustdb` command for querying local and
S3-compatible CSV/Parquet data, managing a persistent local Native OLAP
database, and exposing that database through the v0.9 read-only HTTPS Shell.
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
resource options. [docs/HTTP-SHELL.md](docs/HTTP-SHELL.md) covers TLS startup,
offline Profiles, server-local data sources, the remote CLI, and raw HTTP use.

## Package contents

- `bin/rustdb`: release CLI executable;
- `install.sh` and `uninstall.sh`: bilingual POSIX installation helpers;
- `docs/CLI*.md`: English and Simplified Chinese CLI help;
- `docs/INSTALL*.md`: English and Simplified Chinese installation help;
- `docs/HTTP-SHELL*.md`: English and Simplified Chinese HTTPS Shell help;
- `docs/openapi-v1.yaml`: the public OpenAPI 3.1 contract;
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

The remote Shell always uses TLS and a generated Bearer Token. It is read-only:
it cannot run DDL/DML, upload files, manage data sources, or call direct file
table functions. Keep the exported Profile bundle private.
