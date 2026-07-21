# RustDB 二进制发行包

[English](README.md)

本发行包提供独立的 `rustdb` 命令，用于查询本地磁盘和 S3-compatible
对象存储上的 CSV、Parquet 数据、管理本地持久化 Native OLAP 数据库，以及通过
Beta 只读 HTTPS Shell 暴露该数据库。引擎仍是单机嵌入式；可选服务模式使用一个
`rustdb serve` 进程服务一个 Native 数据库。

## 快速开始

直接在解压目录运行：

```sh
./bin/rustdb --version
./bin/rustdb -c "SELECT count(*) FROM read_parquet('/data/*.parquet')"
./bin/rustdb --database ./warehouse -c "CREATE TABLE events AS SELECT * FROM read_parquet('/data/events.parquet')"
./bin/rustdb serve --database ./warehouse
```

或者安装到系统：

```sh
./install.sh
rustdb --help-zh
```

没有管理员权限时可运行 `./install.sh --prefix "$HOME/.local"`。校验、安装、
升级和卸载方法见 [docs/INSTALL.zh-CN.md](docs/INSTALL.zh-CN.md)，查询和资源参数见
[docs/CLI.zh-CN.md](docs/CLI.zh-CN.md)。TLS 启动、离线 Profile、服务端本地数据源、
远程 CLI 与原始 HTTP 用法见 [docs/http-shell.zh-CN.md](docs/http-shell.zh-CN.md)。
支持部署、认证、指标、审计、备份、修复和故障处理见
[docs/operator-guide.zh-CN.md](docs/operator-guide.zh-CN.md)。

## Beta 2 运维边界

Beta 2 是 fresh-start 格式版本：新建 Native marker epoch `4`，服务配置必须使用
`schema_version = 2`，不转换旧数据库 epoch 或旧配置 schema。请将 CSV/Parquet
重新导入全新数据库。远程 Shell 只提供 `/v2` 契约。

`rustdb backup` 生成一个经过校验的 Native 与安全 HTTP 控制状态统一备份；
`backup-check` 可只校验不恢复，`restore` 要求全新的数据库和服务状态目标。备份排除
Query journal/结果、审计、Spill、临时数据、锁和 Admin socket。运行中的本机管理通过
私有 Unix socket 和 `rustdb service` 完成；停服后的 `service check`/`repair` 只校验或
修复 marker 证明归属的状态。服务端还提供 70/80/90% RSS 压力保护、TLS 周期续签与
热加载，以及有界专用服务 I/O 线程池。

## 包内容

- `bin/rustdb`：release CLI；
- `install.sh`、`uninstall.sh`：中英文 POSIX 安装与卸载脚本；
- `docs/CLI*.md`：中英文 CLI 帮助；
- `docs/INSTALL*.md`：中英文安装帮助；
- `docs/http-shell*.md`：中英文 HTTPS Shell 帮助；
- `docs/operator-guide*.md`：中英文运维指南；
- `docs/diagnostics*.md`、`docs/native-import.md`、`docs/native-repair.md`、
  `docs/compatibility.md`：支持、导入、恢复和兼容参考；
- `docs/troubleshooting.md`、迁移、Parquet 裁剪和 S3 参考；
- `packaging/config/rustdb.example.toml`：通过校验的 schema-version 2 示例；
- `docs/openapi-v2.yaml`：公开 OpenAPI 3.1 协议；
- `RELEASE-NOTES.md`：当前版本变化和已知边界；
- `SHA256SUMS`：所有安装文件的校验值；
- `VERSION`、`LICENSE`。

Rust Library API 和源码通过源码仓库提供，不包含在本二进制包中。当前版本线不提供
Windows 二进制。

## 安全

S3 凭证从 AWS 默认凭证链获取。不要把 Access Key 写入 SQL、Shell 历史或命令行
参数。明文 HTTP endpoint 必须显式添加 `--s3-allow-http`，并且只应在可信开发网络
使用。

远程 Shell 始终使用 TLS 和只保存 digest 的 principal token，并严格只读：不能执行
DDL/DML、上传文件、管理数据源或调用直接文件表函数。请像保护密码一样保护导出的
Profile 包和统一服务备份。
