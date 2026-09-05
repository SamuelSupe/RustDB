# Install and remove RustDB

[简体中文](INSTALL.zh-CN.md)

## Verify the archive

The adjacent `.sha256` file authenticates accidental corruption when obtained
through a trusted channel:

```sh
sha256sum -c rustdb-v1.0.0-beta.3-linux-aarch64.tar.gz.sha256
# macOS:
shasum -a 256 -c rustdb-v1.0.0-beta.3-macos-aarch64.tar.gz.sha256
```

After extraction, `SHA256SUMS` covers every installable payload.

## Install

```sh
tar -xzf rustdb-v1.0.0-beta.3-linux-aarch64.tar.gz
cd rustdb-v1.0.0-beta.3-linux-aarch64
./install.sh
```

The default prefix is `/usr/local`. Use `sudo ./install.sh` only when that
prefix is not writable. For a user install:

```sh
./install.sh --prefix "$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

`RUSTDB_PREFIX` supplies the same setting. `DESTDIR` stages files for a package
manager without changing the paths embedded in the prefix:

```sh
DESTDIR=/tmp/package-root ./install.sh --prefix /usr
```

## Upgrade

Extract the new archive, verify it, and run its `install.sh` with the same
prefix. The known RustDB files are replaced; databases and query data are not
touched. The installer also removes the obsolete Beta 1 `openapi-v1.yaml`
contract from that prefix.

Beta 2 is not an in-place data/config migration. It opens only Native marker
epoch `4` and accepts only service configuration `schema_version = 2`. Before
replacing an older binary, retain the original source CSV/Parquet (or export it
with that binary), then create a fresh Beta 2 database and configuration. The
`migrate` command validates the current epoch; it does not convert old data.

Imported HTTP Shell Profiles and server TLS/Token/Admin-socket state live in the
operating-system user state directory and are not removed or replaced by
`install.sh`.

## Uninstall

From the extracted package:

```sh
./uninstall.sh --prefix /usr/local --dry-run
./uninstall.sh --prefix /usr/local
```

An installed copy is also available at
`PREFIX/share/rustdb/uninstall.sh`. Uninstallation removes only the known CLI
and documentation files. It never removes database, input, Spill, or user
configuration directories.

The installed tree under `PREFIX/share/doc/rustdb` preserves the archive's
relative links. It includes bilingual HTTP Shell, operator, and diagnostics
guides, Native import/repair and compatibility references, the validated
`packaging/config/rustdb.example.toml`, release notes, and the public
`docs/openapi-v2.yaml` contract.

## Platforms

Release archives use these target labels:

- `linux-x86_64`;
- `linux-aarch64`;
- `macos-aarch64`.

Build and validate on the target operating system. Linux packages can be built
for the current OrbStack architecture with `scripts/dist/build.sh`; use
`scripts/dist/build.sh --native` on a native Linux or macOS build host.
