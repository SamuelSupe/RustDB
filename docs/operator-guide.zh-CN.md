# RustDB Beta 运维指南

本文适用于 `v1.0.0-beta.1`。RustDB Beta 已定义兼容和运维契约，但仍属于预生产
软件，不提供生产 SLA。请把它作为单节点服务运行，并始终保留可恢复的源数据和已验证
备份。

## 支持矩阵

| 领域 | Beta 状态 |
| --- | --- |
| Linux x86_64、AArch64 | 支持二进制包与非 root OCI 镜像 |
| macOS Apple Silicon | 支持嵌入式、CLI 与开发 |
| Windows、macOS Intel | 不支持 |
| 本地存储 | 必须支持原子 rename、`fsync` 和 advisory file lock |
| AWS S3 | 通过 AWS 默认凭证链支持 |
| MinIO | 以 `RELEASE.2025-04-22T22-12-26Z` 和 path-style 方式验证 |
| 其他 S3-compatible 存储 | Best effort；需先验证一致性、multipart 和 ETag 行为 |
| 数据格式 | CSV、gzip CSV、zstd CSV、Parquet、本地 Native |

Native 数据库必须位于本地块存储。锁、rename 或持久化语义不明确的网络文件系统不在
支持范围内。S3 用于 CSV/Parquet 数据源和经过验证的备份恢复，不作为在线 Native
卷。

## 文件系统布局

使用专用操作系统账号，并分别规划容量：

```text
/srv/rustdb/database/   Native 数据库
/srv/rustdb/state/      TLS、principal、Profile Token、审计日志
/srv/rustdb/results/    HTTP Query 保留结果
/srv/rustdb/spill/      算子临时 Spill
/etc/rustdb/            只读服务配置
```

数据库、安全状态、结果和 Spill 目录应为 `0700`，敏感文件为 `0600`。不要手工
修改 Native 目录。Native backup 不包含 HTTP 安全状态、审计日志和保留结果。

磁盘预算需覆盖保留数据库、发布过程 headroom、结果、Spill 和一份备份。Native 与
Spill 默认各自保留至少 10% 和 1 GiB 空闲空间；结果配额独立计算。必须监控全部相关
文件系统，Engine 内存上限不是进程 RSS 或磁盘上限。

## 二进制与 OCI 部署

安装前使用发布包 SHA-256 和 `scripts/dist/check.sh` 验证归档。OCI 镜像以
UID/GID `10001:10001` 运行，不需要 root，且该用户不能修改
`/usr/local/bin/rustdb`。

加固后的容器示例：

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

启动前创建宿主机目录并设置 owner。根文件系统保持只读，只挂载四个可写数据目录。
正常停止时发送 `SIGTERM`，至少等待固定的 30 秒 graceful shutdown；不要使用
`SIGKILL`。

## 配置

从 [`packaging/config/rustdb.example.toml`](../packaging/config/rustdb.example.toml)
开始。顶层 `schema_version = 1` 必填，未知字段会被拒绝。

```sh
rustdb config validate /etc/rustdb/rustdb.toml
rustdb --log-format json serve \
  --database /srv/rustdb/database \
  --config /etc/rustdb/rustdb.toml
```

优先级为 `CLI > 环境变量 > TOML > 默认值`。稳定且非敏感的策略写入 TOML，部署
差异放入环境变量，CLI 仅用于明确的一次性覆盖。S3 凭证不得写入 TOML、SQL、endpoint
URL 或 CLI 参数，只能使用 AWS 默认凭证链。明文 S3 endpoint 仅允许显式启用的可信
开发环境。

4 核 16 GiB 主机可从 4 个计算线程和 2–4 GiB Engine 内存上限开始，并设置明确的
结果与 Native 配额。只有在观察真实负载下的排队、结果盘增长和峰值内存后再提高并发。

## 认证和授权

默认启用认证。首次启动创建 `admin` principal 和随机 Profile Token。
`principals.json` 只保存 SHA-256 digest；明文 Profile Token 位于独立私有文件且
不会写入日志。

本地管理身份前必须停止服务：

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

轮换会添加一枚可重叠 Token；分发并验证新 Profile 后再撤销旧 Token。Token 列表只
输出 UUID、principal、生命周期状态和有效期，可安全用于清点。按 Token UUID 导出时会
复用已管理连接包的 URL 与 CA；需要显式 HTTPS origin 时传入 `--server-url`。Query 角色
只能访问自己的 Query ID，Admin 可以访问全部 Query 和 `/metrics`。最后一个已启用
Admin 不能被禁用或降级。

`--no-auth` 只用于开发。它只授予 Query 权限，没有 Admin 身份，因此 `/metrics` 仍会
返回 forbidden。不要在非回环监听或不可信主机上使用。

## 健康、指标、日志和审计

- `GET /healthz` 报告进程存活，`GET /readyz` 报告就绪；两者无需认证且只泄露最小状态。
- `GET /metrics` 输出 Prometheus 文本，必须使用 Admin Bearer Token。
- `--log-format json` 向 stderr 输出结构化事件；用 `RUST_LOG` 过滤。日志只记录 SQL
  fingerprint，不记录 SQL 原文。
- `<state-root>/<database-id>/audit.jsonl` 记录认证、principal、Token、Query 和服务
  生命周期。文件私有、每条记录同步，64 MiB 轮转，并保留一个旧文件。

Admin 抓取示例：

```sh
curl --fail --cacert /secure/ca.pem \
  -H "Authorization: Bearer $(< /secure/admin.token)" \
  https://analytics.example.com:7400/metrics
```

建议对 ready 失败、Query 拒绝/失败、running Query 持续增长、结果盘/数据库盘压力以及
服务日志中的审计写入错误告警。指标使用低基数进程级 counter/gauge，不包含 principal
或 Query ID label。Beta 审计日志是运维证据，不是合规级不可变账本。

## Check、诊断、修复和备份

停止服务后执行完整性检查：

```sh
rustdb native check /srv/rustdb/database
rustdb native check /srv/rustdb/database --json
rustdb diagnostics --database /srv/rustdb/database \
  --output /secure/rustdb-diagnostics.json
```

`native check` 严格只读。diagnostics 生成版本化、脱敏 JSON：数据库路径会哈希，凭证、
endpoint 值、表名和敏感路径不会输出。只能在人工检查后通过可信支持渠道传输。

修复必须先计划，且范围刻意很窄：

```sh
rustdb native repair /srv/rustdb/database --json
rustdb native repair /srv/rustdb/database --apply --json
rustdb native check /srv/rustdb/database
```

执行 `--apply` 前必须创建完整、已验证的备份。repair 自身只创建 metadata backup，
会在数据库锁下重新验证计划，且绝不会猜测重建缺失用户数据。详见
[Native 检查与修复](native-repair.md)。

备份应恢复到新的空目录：

```sh
rustdb backup /srv/rustdb/database /backup/rustdb-2026-07-19
rustdb --s3-region us-east-1 backup \
  /srv/rustdb/database s3://backup-bucket/rustdb/2026-07-19
rustdb restore /backup/rustdb-2026-07-19 /srv/rustdb/restore-check
rustdb native check /srv/rustdb/restore-check
```

只有验证完成后才发布 backup manifest；restore 不会覆盖已有目标。应定期完成一次完整
恢复和查询 checksum 校验。S3 必须配置 incomplete-multipart 生命周期策略，每份备份
使用独立空 prefix。

## 故障处理顺序

1. 停止新流量，保留第一条稳定 error code、request ID、Query ID；不要收集凭证或原始
   Profile 包。
2. 发送 `SIGTERM` 并等待退出，复制日志和私有审计文件。
3. 在不以写模式打开数据库的情况下执行 `native check --json` 和 `diagnostics`。
4. 把最近备份恢复到新路径，绝不覆盖故障源目录。
5. 只有理解只读 repair plan 且已有完整备份时才 apply；repair 拒绝的损坏必须升级处理。

## Beta 发行门禁与 SLA

在至少 4 个逻辑 CPU、16 GiB 内存的主机上，从干净且已提交的工作区运行唯一一次可审计
门禁。所有输入都必须显式提供；脚本不会下载、合成或静默跳过发行样本。

```sh
export RUSTDB_BETA_ACCEPTANCE_OUTPUT=/srv/rustdb-evidence/beta-2026-07-19
export RUSTDB_BETA_LOCAL_FIXTURE=/srv/rustdb-fixtures/lake-local
export RUSTDB_BETA_LOCAL_FORMAT=parquet       # csv 或 parquet
export RUSTDB_BETA_MINIO_MANIFEST=/srv/rustdb-fixtures/minio-manifest.json
export RUSTDB_BETA_MINIO_FORMAT=parquet       # csv 或 parquet
export RUSTDB_BETA_CLICKBENCH_DATA_DIR=/srv/rustdb-fixtures/clickbench
export RUSTDB_BETA_CLICKBENCH_PROFILE=functional
scripts/ci/beta_acceptance.sh
```

输出必须是 Git 工作区之外、此前不存在的绝对路径。本地与 MinIO 样本都至少包含
100 GiB 和 10,000 个扁平普通文件/对象，并且只能包含所选格式。门禁会自行生成覆盖完整
`/beta-data` 只读挂载和 MinIO prefix 的 `count(*)` 查询。MinIO manifest 不得包含凭证，
并使用 ETag 固定对象身份：

```json
{
  "schema": "rustdb-beta-object-manifest-v1",
  "root_uri": "s3://bucket/prefix",
  "objects": [
    {"uri": "s3://bucket/prefix/part-000.parquet", "size": 123, "etag": "..."}
  ]
}
```

本地与 MinIO 必须是等价数据：2/4 GiB、8 客户端的四次运行必须得到同一 checksum。
门禁会重新核对本地文件的 size/mtime 清单，并在 MinIO 运行前后列出对象，要求每个
URI、size、ETag 都与 manifest 一致；每条查询还必须发现 manifest 中的精确文件数。
ClickBench 目录必须预先放好 `queries.sql` 及
SHA-256 固定的 functional 数据文件。门禁只运行一次 `scripts/ci/orbstack.sh all`、四次
外部数据路径和一次 ClickBench，并把 commit、主机/profile、规范化输入清单、命令、
日志、runner build ID、资源摘要和结果写入 `<输出>/evidence.json`。失败或中断也会生成
failed 证据，且该目录不可复用。不重复运行专用低内存 Spill 压力测试。

100 GiB 的 `count(*)` 路径负责发现、快照、元数据、并发和内存记账；functional
ClickBench 则通过固定的 43 组行数与 typed checksum oracle 验证真实 Parquet 数据页和
结果正确性。两部分都不构成跨引擎性能声明。

annotated release tag 必须绑定已验收 commit：

```text
RustDB-Acceptance-SHA: <40 位 commit>
RustDB-Acceptance-Status: passed
```

Distribution workflow 缺少上述精确 trailer 时会拒绝发布。这是一次性发行门禁，不是
soak test、可用性目标、延迟 SLO 或持久性 SLA。Beta 用户必须保留源数据、备份和回滚
路径。创建 tag 前先检查 `evidence.json` 的 `status: passed` 和 `accepted_commit`，再将
该 SHA 原样写入 annotated tag trailer。

另见 [HTTP Shell 指南](http-shell.zh-CN.md)、[兼容矩阵](compatibility.md)、
[故障排查](troubleshooting.md)和 [Beta 迁移指南](migration-v1-beta.md)。
