# RustDB CLI 帮助

[English](CLI.md)

运行 `rustdb --help` 查看权威英文参数列表，运行 `rustdb --help-zh` 查看内置简体
中文帮助。

## 查询输入与输出

不指定 `-c` 或 `-f` 时，RustDB 启动交互终端。交互输入和 SQL 文件中的语句都必须
以分号结束。

```sh
# 执行一条 SQL
rustdb -c "SELECT count(*) FROM read_csv('/data/events/*.csv')"

# 执行 SQL 文件并输出 JSON Lines
rustdb -f report.sql --format jsonl

# CSV 输出使用明确的 NULL 标记
rustdb -f report.sql --format csv --csv-null '\N'
```

输出格式支持 `table`、`csv`、`jsonl`。`--metrics` 会在流式结果消费完毕后，把执行
指标写到 stderr。

## 只读 HTTPS Shell

一个 TLS 服务进程只打开一个 Native 持久化数据库。默认只监听回环地址；监听非回环
地址时还必须提供客户端可达的 HTTPS origin，使自动生成的证书包含正确身份：

```sh
rustdb serve --database /srv/rustdb/analytics

rustdb serve \
  --database /srv/rustdb/analytics \
  --listen 0.0.0.0:7400 \
  --advertise-url https://analytics.example.com:7400
```

首次启动会生成本地 CA、可续签服务端证书、Admin principal 和随机 Profile Token。停止服务并导出
连接包，通过可信渠道传输后，再在客户端导入：

```sh
rustdb profile export \
  --database /srv/rustdb/analytics \
  --output analytics.rustdb-profile
rustdb profile import analytics.rustdb-profile --name analytics
```

服务停止时可清点 Token 且不暴露凭证材料，并为指定 Query/Admin principal 导出
Profile。按 Token 导出默认复用已管理连接包的 URL 与 CA；需要覆盖 HTTPS origin 时
增加 `--server-url`。

```sh
rustdb token list --database /srv/rustdb/analytics --principal analyst
rustdb token rotate --database /srv/rustdb/analytics --principal analyst
rustdb profile export --database /srv/rustdb/analytics \
  --token-id <UUID> --output analyst.rustdb-profile
rustdb token revoke --database /srv/rustdb/analytics --token-id <UUID>
```

`token list` 只输出 UUID、principal、active/revoked/inactive 状态和 valid-until，
绝不输出 Token secret、digest 或 Token 文件路径。

远程 CLI 使用命名 Profile。交互模式会等待每个后台 Query；`-c` 与 `-f` 适合脚本，
并保留本地输出格式。Ctrl-C 会尽力向服务端发送取消请求。

```sh
rustdb shell --profile analytics -c \
  "SELECT region, count(*) FROM sales GROUP BY region" --format table
rustdb shell --profile analytics -f report.sql --format csv >report.csv
```

远程边界严格只读，只允许针对 Native/系统表和服务端本地注册源执行查询、元数据与
Explain 语句；DDL/DML、维护、上传、直接 `read_csv`/`read_parquet` 和远程数据源管理
都会被拒绝。生命周期和公开 `/v2` 契约见本文件同目录的 `http-shell.zh-CN.md`
与 `openapi-v2.yaml`。

`serve` 的结果保留控制独立于引擎 Spill 配额：`--result-directory`、
`--result-ttl-secs`、`--result-global-limit`、`--result-query-limit`。默认已完成结果
保留一小时；总硬上限为 10 GiB，单 Query 上限为 2 GiB。可通过 `--query-*-limit`、
`--principal-*-limit`、principal 运行/排队上限和 `--principal-weight` 配置分层准入；
Admin 不会获得隐式优先级。引擎通过 `--spill-engine-limit` 与 `--spill-query-limit`
执行对应 Spill 硬上限。同一命令还支持为服务端本地注册源指定 `--s3-region`、`--s3-endpoint`、
`--s3-path-style`、`--s3-allow-http` 和 `--s3-anonymous`。HTTP 仅用于可信开发
endpoint，匿名模式仅用于公开对象。

Beta 2 服务配置必须使用 `schema_version = 2`；旧 schema 会被拒绝，不会原地升级。
默认 RSS 守护器以物理内存与 Linux cgroup 上限中的较小值为基准：70% 时限制新提交，
80% 时拒绝新提交，90% 时还会取消 live reservation 最大的运行中 Query。三个阈值与
采样周期通过 TOML 的 `[server]` 配置。

Query journal、保留结果、凭证与证书的阻塞 I/O 使用有界专用线程池
（`service_io_threads`，默认 `2`）。服务端按周期检查托管 TLS 叶证书
（`tls_renew_interval_secs`，默认六小时），临近过期时续签并热加载，不重启 listener。

CSV/Parquet 持久注册只能在服务端主机上、`rustdb serve` 停止时修改：

```sh
rustdb datasource add-parquet \
  --database /srv/rustdb/analytics \
  --name sales \
  --location 's3://lake/sales/*.parquet'
rustdb datasource list --database /srv/rustdb/analytics
rustdb datasource refresh --database /srv/rustdb/analytics --name sales
rustdb datasource remove --database /srv/rustdb/analytics --name sales
```

注册信息不会保存对象存储凭证；凭证由服务进程账号的默认凭证链提供。

## Native 持久化数据库

使用 `--database` 打开本地持久化数据库；不指定时仍使用临时 Engine：

```sh
rustdb --database ./warehouse -c \
  "CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/*.parquet')"

rustdb --database ./warehouse -c \
  "BEGIN READ ONLY; SELECT count(*) FROM events; COMMIT"
```

Beta 2 Native 数据库使用带校验和的 WAL 和快照隔离事务。一个事务固定一份
Catalog/数据快照；执行 `COMMIT` 前必须消费完或丢弃全部流式结果。mutation 结果
必须消费到 EOS。每条事务语句都有内部 savepoint；失败、取消或放弃只回滚该语句
staged 变化，事务仍可继续使用。SQL `SAVEPOINT` 仍不支持。

若 `COMMIT` 报告结果未知，不要重复提交；应重新打开数据库并检查恢复后的
Catalog。post-commit failure 则明确表示 generation 已经持久化，同样不能重试。

持久对象默认位于 `main` schema。可执行 `CREATE SCHEMA analytics` 并使用
`analytics.events`；查询、DML、DDL、COPY 和维护命令采用同一限定名。
`SHOW SCHEMAS` 与 `information_schema.schemata` 可查看命名空间。若 COPY 返回
`CopyPostCommitFailure`，目标已经持久化，不应重试。若提示已有未完成的 staging
文件或远端子对象，请检查并删除该未完成目标后再重试；RustDB 不会自动删除 crash
残留。

Beta 2 创建 Native marker epoch `4`。epoch `1`、`2`、`3` 会在修改权限、加锁、WAL
恢复或清理前被拒绝。`migrate` 只验证数据库已经使用当前 epoch，不会转换旧数据库：

```sh
rustdb migrate ./warehouse
```

请创建全新的 Beta 2 数据库，再从 CSV 或 Parquet 导入。普通 open 不会静默升级旧
格式。

使用独立数据库命令创建或恢复经过校验的备份：

```sh
rustdb backup ./warehouse ./warehouse-backup --state-root ./service-state
rustdb backup-check ./warehouse-backup
rustdb restore ./warehouse-backup ./warehouse-restored \
  --state-root ./restored-service-state

rustdb --s3-region us-east-1 \
  backup ./warehouse s3://analytics/rustdb/warehouse-2026-07-18 \
  --state-root ./service-state
rustdb --s3-region us-east-1 \
  backup-check s3://analytics/rustdb/warehouse-2026-07-18
rustdb --s3-region us-east-1 \
  restore s3://analytics/rustdb/warehouse-2026-07-18 ./warehouse-restored \
  --state-root ./restored-service-state
```

这是统一服务备份：存在 HTTP 状态时，除了 Native 数据，还会包含 principal、
Token/Profile、本地 CA 和托管 TLS identity 等安全控制状态，因此必须按敏感凭证保护。
备份明确排除 Query journal 与保留结果、审计日志、Spill 与临时数据、锁和 Admin
socket。`backup-check` 不执行恢复，只校验版本化 manifest、inventory、权限、校验和与
内嵌 Native 数据库。恢复会再次校验，并要求数据库路径和数据库对应的服务状态目标
都是全新的。

远端备份先上传不可变对象，最后发布 manifest。S3 endpoint、path-style、HTTP 与
匿名参数和查询 Scan 一致；Secret Key 仍只通过默认凭证链获取。

嵌入式调用方放弃 backup future 后，Engine-owned worker 仍会继续到 manifest 发布
成功或未完成对象清理结束。进程或主机崩溃应由 S3 未完成 multipart 生命周期策略
兜底。若 manifest 前崩溃留下已完成对象，RustDB 会拒绝这个非空目标；检查并删除
该专用前缀后再重试。

## 服务完整性与本机管理

停止 `serve` 后，可校验服务所有的 principal、Query journal、保留结果 manifest 和
chunk。`service repair` 默认只生成计划；只有 `--apply` 才写入，而且只处理能由
RustDB marker 证明归属的未完成或无效 artifact：

```sh
rustdb service check --database ./warehouse --state-root ./service-state
rustdb service repair --database ./warehouse --state-root ./service-state
rustdb service repair --database ./warehouse --state-root ./service-state --apply
```

`serve` 运行期间，通过私有本机 Unix Admin socket 查看状态、原子重载/轮换/撤销
凭证，以及请求优雅停服。socket 默认位于数据库对应的服务状态目录，权限为 `0600`；
`--admin-socket` 可指定其他路径。

```sh
rustdb service status --database ./warehouse --state-root ./service-state
rustdb service reload-tokens --database ./warehouse --state-root ./service-state
rustdb service rotate-token --database ./warehouse --state-root ./service-state \
  --principal analyst
rustdb service revoke-token --database ./warehouse --state-root ./service-state \
  --token-id <UUID>
rustdb service shutdown --database ./warehouse --state-root ./service-state
```

## 数据源

```sql
SELECT *
FROM read_parquet('/data/events/*.parquet')
WHERE event_date >= '2026-01-01'
LIMIT 10;

SELECT count(*)
FROM read_csv('/data/events/*.csv.gz', compression = 'auto');
```

支持普通本地路径、glob、`file://`、`s3://`、未压缩/gzip/zstd CSV 和 Parquet。
SQL 支持边界记录在源码仓库的兼容性文档中；不支持的 SQL 会返回明确错误。

AWS S3 凭证通过默认凭证链解析：

```sh
AWS_PROFILE=analytics rustdb --s3-region us-east-1 -c \
  "SELECT count(*) FROM read_parquet('s3://bucket/events/*.parquet')"
```

MinIO 或其他 S3-compatible 服务：

```sh
rustdb --s3-endpoint http://127.0.0.1:9000 \
  --s3-region us-east-1 --s3-path-style --s3-allow-http \
  -c "SELECT * FROM read_parquet('s3://bucket/events/*.parquet') LIMIT 10"
```

`--s3-anonymous` 仅用于公开对象。RustDB 不提供明文 Access Key/Secret Key 命令行
参数。

## 资源控制

常用选项：

- `--memory-limit 2GiB`：查询引擎内存预算；
- `--database PATH`：本地持久化 Native 数据库目录；
- `--native-engine-limit 20GiB`：完整 Native 数据库目录的硬配额；
- `--native-default-table-limit 5GiB`：单张 Native 表的默认硬配额；
- `--native-table-limit events=10GiB`：指定表覆盖值；可重复使用，非默认
  schema 使用 `schema.table`；
- `--threads 4`：计算线程数；
- `--batch-size 8192`：Arrow 批次目标行数；
- `--io-concurrency 16`：并发 Scan 任务数；
- `--metadata-cache 256MiB`：Parquet 元数据缓存；
- `--max-concurrent-queries 1`：最大并发查询数；
- `--spill-directory PATH`：查询 Spill 根目录；
- `--spill-engine-limit`、`--spill-query-limit`：Spill 硬配额；
- `--runtime-filter-bytes 8MiB`：Join Runtime Filter 内存预算。

`serve` 的结果 TTL、总/单 Query 配额与这些本地执行控制项彼此独立，详见上方 HTTPS
Shell 一节。

大小单位支持 `B`、`KB`、`MB`、`GB`、`KiB`、`MiB`、`GiB`。
Rust API 也可通过 `NativeStorageConfig` 设置这些配额。配额在每次打开时提供，
不会写入数据库格式。

## 交互终端命令

- `.tables`：列出已注册的外部表；
- `.help` 或 `.help en`：英文帮助；
- `.help zh`：中文帮助；
- `.quit` 或 `.exit`：退出。

没有外层 `ORDER BY` 时不保证结果顺序。按 Ctrl-C 可取消当前查询。
