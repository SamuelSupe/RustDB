# 安装和卸载 RustDB

[English](INSTALL.md)

## 校验发行包

从可信渠道获取文件后，可用相邻的 `.sha256` 检查意外损坏：

```sh
sha256sum -c rustdb-v1.0.0-beta.2-linux-aarch64.tar.gz.sha256
# macOS：
shasum -a 256 -c rustdb-v1.0.0-beta.2-macos-aarch64.tar.gz.sha256
```

解压后的 `SHA256SUMS` 覆盖所有可安装文件。

## 安装

```sh
tar -xzf rustdb-v1.0.0-beta.2-linux-aarch64.tar.gz
cd rustdb-v1.0.0-beta.2-linux-aarch64
./install.sh
```

默认前缀是 `/usr/local`。只有该目录不可写时才需要 `sudo ./install.sh`。用户级安装：

```sh
./install.sh --prefix "$HOME/.local"
export PATH="$HOME/.local/bin:$PATH"
```

也可以使用 `RUSTDB_PREFIX`。`DESTDIR` 用于系统打包暂存，不改变 prefix 中的安装
路径：

```sh
DESTDIR=/tmp/package-root ./install.sh --prefix /usr
```

## 升级

解压并校验新版本，然后使用相同 prefix 运行新包中的 `install.sh`。脚本只替换已知
RustDB 文件，不会修改数据库或查询数据；同时会清理该 prefix 中 Beta 1 遗留的
`openapi-v1.yaml` 契约。

Beta 2 不提供原地数据或配置迁移：它只打开 Native marker epoch `4`，并且只接受
`schema_version = 2` 的服务配置。替换旧二进制前，请保留原始 CSV/Parquet（或使用
旧二进制导出），再新建 Beta 2 数据库和配置。`migrate` 只验证当前 epoch，不转换
旧数据。

HTTP Shell Profile 与服务端 TLS/Token/Admin-socket 状态位于操作系统用户级状态
目录，`install.sh` 不会删除或替换它们。

## 卸载

在解压目录执行：

```sh
./uninstall.sh --prefix /usr/local --dry-run
./uninstall.sh --prefix /usr/local
```

安装后也可运行 `PREFIX/share/rustdb/uninstall.sh`。卸载只删除已知 CLI 和文档文件，
不会删除数据库、输入数据、Spill 或用户配置目录。

`PREFIX/share/doc/rustdb` 中的安装树会保留发行包的相对链接，并包含中英文 HTTP
Shell、运维和诊断指南，Native 导入/修复与兼容参考、已校验的
`packaging/config/rustdb.example.toml`、发行说明，以及公开的
`docs/openapi-v2.yaml` 协议。

## 平台

发行包使用以下目标标签：

- `linux-x86_64`；
- `linux-aarch64`；
- `macos-aarch64`。

必须在目标操作系统上构建和校验。`scripts/dist/build.sh` 可通过 OrbStack 构建当前
架构的 Linux 包；在原生 Linux 或 macOS 构建机上使用
`scripts/dist/build.sh --native`。
