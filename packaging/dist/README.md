# RustDB binary distribution

[简体中文](README.zh-CN.md)

This archive contains the standalone `rustdb` command for querying local and
S3-compatible CSV and Parquet data. The engine is single-node and embedded in
the command; no server process is required.

## Quick start

Run from the extracted directory:

```sh
./bin/rustdb --version
./bin/rustdb -c "SELECT count(*) FROM read_parquet('/data/*.parquet')"
```

Or install it:

```sh
./install.sh
rustdb --help
```

Use `./install.sh --prefix "$HOME/.local"` for an unprivileged user install.
See [docs/INSTALL.md](docs/INSTALL.md) for checksum, installation, upgrade, and
uninstallation instructions. [docs/CLI.md](docs/CLI.md) documents query and
resource options.

## Package contents

- `bin/rustdb`: release CLI executable;
- `install.sh` and `uninstall.sh`: bilingual POSIX installation helpers;
- `docs/CLI*.md`: English and Simplified Chinese CLI help;
- `docs/INSTALL*.md`: English and Simplified Chinese installation help;
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
