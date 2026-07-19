# Install and remove RustDB

[简体中文](INSTALL.zh-CN.md)

## Verify the archive

The adjacent `.sha256` file authenticates accidental corruption when obtained
through a trusted channel:

```sh
sha256sum -c rustdb-v0.9.0-alpha.1-linux-aarch64.tar.gz.sha256
# macOS:
shasum -a 256 -c rustdb-v0.9.0-alpha.1-macos-aarch64.tar.gz.sha256
```

After extraction, `SHA256SUMS` covers every installable payload.

## Install

```sh
tar -xzf rustdb-v0.9.0-alpha.1-linux-aarch64.tar.gz
cd rustdb-v0.9.0-alpha.1-linux-aarch64
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
touched.

Imported HTTP Shell Profiles and server TLS/Token state live in the operating
system user state directory and are not removed or replaced by `install.sh`.

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

The installed documentation also includes `HTTP-SHELL.md`,
`HTTP-SHELL.zh-CN.md`, and the public `openapi-v1.yaml` contract.

## Platforms

Release archives use these target labels:

- `linux-x86_64`;
- `linux-aarch64`;
- `macos-aarch64`.

Build and validate on the target operating system. Linux packages can be built
for the current OrbStack architecture with `scripts/dist/build.sh`; use
`scripts/dist/build.sh --native` on a native Linux or macOS build host.
