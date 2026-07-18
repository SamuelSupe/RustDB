# RustDB 二进制发行包

[English](README.md)

本发行包提供独立的 `rustdb` 命令，用于查询本地磁盘和 S3-compatible
对象存储上的 CSV、Parquet 数据，以及管理本地持久化 Native OLAP 数据库。查询引擎
直接嵌入命令行程序，不需要启动服务端。

## 快速开始

直接在解压目录运行：

```sh
./bin/rustdb --version
./bin/rustdb -c "SELECT count(*) FROM read_parquet('/data/*.parquet')"
./bin/rustdb --database ./warehouse -c "CREATE TABLE events AS SELECT * FROM read_parquet('/data/events.parquet')"
```

或者安装到系统：

```sh
./install.sh
rustdb --help-zh
```

没有管理员权限时可运行 `./install.sh --prefix "$HOME/.local"`。校验、安装、
升级和卸载方法见 [docs/INSTALL.zh-CN.md](docs/INSTALL.zh-CN.md)，查询和资源参数见
[docs/CLI.zh-CN.md](docs/CLI.zh-CN.md)。

## 包内容

- `bin/rustdb`：release CLI；
- `install.sh`、`uninstall.sh`：中英文 POSIX 安装与卸载脚本；
- `docs/CLI*.md`：中英文 CLI 帮助；
- `docs/INSTALL*.md`：中英文安装帮助；
- `SHA256SUMS`：所有安装文件的校验值；
- `VERSION`、`LICENSE`。

Rust Library API 和源码通过源码仓库提供，不包含在本二进制包中。当前版本线不提供
Windows 二进制。

## 安全

S3 凭证从 AWS 默认凭证链获取。不要把 Access Key 写入 SQL、Shell 历史或命令行
参数。明文 HTTP endpoint 必须显式添加 `--s3-allow-http`，并且只应在可信开发网络
使用。
