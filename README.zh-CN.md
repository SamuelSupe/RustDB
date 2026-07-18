<div align="center">

# RustDB

**面向 CSV、Parquet、S3 与本地持久化分析的嵌入式单机 OLAP 引擎。**

[English](README.md) · [架构](docs/architecture.md) · [SQL 兼容范围](docs/compatibility.md) · [CLI 帮助](packaging/dist/CLI.zh-CN.md)

[![CI](https://github.com/SamuelSupe/RustDB/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/SamuelSupe/RustDB/actions/workflows/ci.yml)
[![Distribution](https://github.com/SamuelSupe/RustDB/actions/workflows/dist.yml/badge.svg)](https://github.com/SamuelSupe/RustDB/actions/workflows/dist.yml)
[![Version](https://img.shields.io/badge/version-0.8.0--alpha.1-orange)](Cargo.toml)
[![Rust](https://img.shields.io/badge/rust-1.97.0-dea584?logo=rust)](rust-toolchain.toml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

</div>

> [!WARNING]
> RustDB 目前仍是实验性的 alpha 软件，适合功能评估、开发与可复现的引擎研究；
> Native 存储格式和公开 API 尚未承诺生产级稳定性。

RustDB 可以直接查询本地磁盘或 S3-compatible 对象存储中的 CSV 和
Parquet，也可以把数据批量导入不可变分段的 Native 数据库，用于重复的本地分析。
Rust API 与 `rustdb` CLI 都以 Apache Arrow `RecordBatch` 流式返回结果。

SQL Binder、优化器、向量化算子、调度器、内存记账、Native 存储与 Spill
均由本项目实现；运行时不依赖 DataFusion，项目代码禁止使用 `unsafe`。

## 为什么是 RustDB？

- **直接查询数据源**：支持普通路径、glob、`file://`、AWS S3 和
  MinIO/S3-compatible endpoint。
- **高效列式路径**：投影/谓词下推、Parquet row-group/page-index/Bloom
  裁剪、运行时过滤器和元数据 singleflight。
- **高速 CSV**：支持原始、gzip、zstd 输入，并以 quote-aware framing
  安全地并行解析单个大文件。
- **Native 持久化分析**：支持事务 DML/DDL、稳定行版本、带校验和的 WAL
  恢复、快照隔离、维护，以及经过校验的本地/S3 备份与恢复。
- **资源受控执行**：多 lane pipeline、引擎/查询内存预算、有界队列、取消，
  以及受配额和磁盘余量治理的 Spill。
- **默认嵌入式**：提供精简、聚焦的外层 Rust API 与流式 CLI，无需常驻服务进程。

## 能力概览

| 领域 | v0.8 alpha 当前范围 |
| --- | --- |
| 数据格式 | CSV、gzip CSV、zstd CSV、Parquet |
| 存储 | 本地文件系统、S3/MinIO、本地持久化 Native 数据库 |
| 接口 | 嵌入式 Rust API、`rustdb` CLI/REPL |
| 输出 | 流式 Apache Arrow `RecordBatch` |
| 执行 | 向量化、多 lane、内存记账、支持 Spill |
| SQL | 面向 TPC-H 的分析 SQL、Join、聚合、窗口、集合运算、参数化查询 |
| 安全 | 项目代码使用 `#![forbid(unsafe_code)]` |

准确的 SQL、类型和格式边界请以[兼容矩阵](docs/compatibility.md)为准。

## 快速开始

### 使用 OrbStack 构建发行包

默认开发与打包路径使用 OrbStack 的 Docker 引擎：

```sh
git clone https://github.com/SamuelSupe/RustDB.git
cd RustDB
scripts/dist/build.sh
```

命令会在 `dist/` 生成带版本号的压缩包和 SHA-256 文件，并验证中英文帮助、
安装、运行和卸载。已安装固定 Rust 工具链的原生主机可以运行
`scripts/dist/build.sh --native`，为当前平台构建。

### 直接查询文件

```sh
cargo run --locked --release --bin rustdb -- \
  --threads 4 \
  --memory-limit 2GiB \
  -c "SELECT count(*) FROM read_parquet('/data/events/*.parquet')"
```

```sql
SELECT country, count(*) AS events
FROM read_csv('/data/events/*.csv.gz', compression = 'auto')
WHERE event_date >= DATE '2026-01-01'
GROUP BY country
ORDER BY events DESC
LIMIT 20;
```

不传 `-c` 和 `-f` 会进入 REPL。输出格式包括 `table`、`csv` 和 `jsonl`。
可以运行 `rustdb --help`、`rustdb --help-zh`，或在 REPL 中执行 `.help zh`
查看双语帮助。

### 查询 S3 或 MinIO

AWS 凭证只从默认凭证链或嵌入应用提供的 credential provider 获取；RustDB
不会提供明文 secret CLI 参数。

```sh
AWS_PROFILE=analytics rustdb --s3-region us-east-1 -c \
  "SELECT count(*) FROM read_parquet('s3://analytics/events/*.parquet')"
```

```sh
rustdb \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-region us-east-1 \
  --s3-path-style \
  --s3-allow-http \
  -c "SELECT * FROM read_parquet('s3://demo/events/*.parquet') LIMIT 10"
```

AWS、匿名访问和 MinIO 配置请参阅 [S3 配置](docs/s3.md)。

## 嵌入 RustDB

```rust,no_run
use futures::StreamExt;
use rustdb::{Engine, EngineConfig, ParquetOptions};

# async fn run() -> rustdb::Result<()> {
let engine = Engine::new(EngineConfig::default())?;
let session = engine.session();

session
    .register_parquet(
        "events",
        ["/data/events/*.parquet"],
        ParquetOptions::default(),
    )
    .await?;

let mut result = session
    .execute("SELECT country, count(*) FROM events GROUP BY country")
    .await?;

while let Some(batch) = result.stream().next().await {
    println!("{:?}", batch?);
}
# Ok(())
# }
```

公开结果始终保持流式，是否收集以及何时收集由调用者决定。查询被取消或结果被
丢弃后，TaskGroup 会先收敛，再清理该查询的 Spill。

## Native 持久化数据库

`Engine::new` 是临时会话；`Engine::open` 会打开本地持久化数据库，可以直接从
CSV 或 Parquet 导入不可变分段：

```rust,no_run
use futures::StreamExt;
use rustdb::{Engine, EngineConfig};

# async fn import() -> rustdb::Result<()> {
let engine = Engine::open("./warehouse", EngineConfig::default())?;
let session = engine.session();

let mut write = session
    .execute(
        "CREATE TABLE events AS \
         SELECT * FROM read_parquet('/data/events/*.parquet')",
    )
    .await?;

while let Some(batch) = write.stream().next().await {
    batch?;
}
# Ok(())
# }
```

`INSERT`、`UPDATE FROM`、`DELETE USING`、`TRUNCATE`、DML `RETURNING`，以及
安全的事务化表/View DDL 都使用同一发布协议。进行中的查询继续读取已固定快照，
后续查询看到新的 Catalog generation。带校验和的 WAL、稳定行 ID、delete vector、
乐观多写者快照隔离及类型化事务 API 均支持重启恢复。

Native 配额是可选的提交硬限制。引擎配额覆盖完整数据库目录及提交发布所需
余量；表配额覆盖当前、保留、暂存和事务快照。打开数据库时可以配置引擎
配额、默认表配额和指定表覆盖值：

```rust,no_run
use rustdb::{Engine, EngineConfig};

# fn open() -> rustdb::Result<()> {
let config = EngineConfig::builder()
    .native_engine_limit_bytes(Some(20 << 30))
    .native_default_table_limit_bytes(Some(5 << 30))
    .native_table_limit_bytes("events", 10 << 30)
    .build();
let engine = Engine::open("./warehouse", config)?;
# Ok(())
# }
```

配额需要在每次打开时提供，不会写入数据库。写入被拒绝时会返回结构化错误，
包含引擎或表、当前字节、新增字节、峰值字节及限制字节。

持久对象默认位于 `main` schema，也可使用 `schema.object`。RustDB 支持
`CREATE SCHEMA`、`DROP SCHEMA`、`SHOW SCHEMAS` 与
`information_schema.schemata`，限定名可贯穿 DML、DDL、COPY 和维护命令。
事务内 mutation 的结果必须消费到 EOS；若在 staged 后取消或放弃结果，整个
事务会回滚。`CopyPostCommitFailure` 表示 COPY 输出已经持久化，不应重试同一
目标。

提交错误具有明确的终态语义。`NativeCommitPostCommitFailure` 表示事务已经
提交，`Transaction::commit_info()` 仍可取得其 generation；
`CommitOutcomeUnknown` 表示事务结果不确定，不应在原句柄上再次 `commit` 或
`rollback`，而应重新打开数据库，核对可见 Catalog generation 后再写入。SQL
`COMMIT` 遵循同一规则，并会清除 Session 中的活动事务。

CLI 使用 `rustdb --database ./warehouse` 打开数据库。已有 v0.7 数据库在执行
`rustdb migrate ./warehouse` 前保持只读；迁移会校验源数据、创建或校验与
当前 catalog 快照完全一致的 `.v0.7-backup`，再原子启用 v0.8 WAL。

本地目录和 S3 都支持一致性备份与恢复：

```sh
rustdb backup ./warehouse ./warehouse-backup
rustdb --s3-region us-east-1 backup ./warehouse s3://bucket/rustdb/snapshot
rustdb restore ./warehouse-backup ./warehouse-restored
```

嵌入式调用方若放弃进行中的远程备份 future，RustDB 会由 Engine 后台继续收敛：
要么发布完整 manifest，要么中止 multipart 并删除未被 manifest 引用的对象。
进程或主机崩溃时无法执行这段清理，因此生产 bucket 仍应配置“未完成 multipart”
生命周期策略作为兜底。若崩溃发生在 multipart 完成后、manifest 发布前，可能留下
已经完成但不可达的对象；RustDB 会拒绝继续写入这个“非空但无 manifest”的目标，
请检查并删除该专用目标前缀后再重试。
远程备份/恢复使用的本地工作目录具有私有权限、版本化 owner marker 和活动锁。
Engine 启动时只回收超过 TTL 且可验证、未被锁定的崩溃残留；未知、伪造、符号链接、
未过期或仍活动的路径一律保留。

## 架构

```mermaid
flowchart LR
    SQL["SQL / 参数"] --> Binder["Binder + Session Catalog"]
    Binder --> Optimizer["规则优化器 + 统计信息"]
    Optimizer --> Pipelines["向量化物理 Pipeline"]
    Local["本地文件"] --> Scan["CSV / Parquet / Native Scan"]
    S3["S3 / MinIO"] --> Scan
    Native["Native 快照"] --> Scan
    Scan --> Pipelines
    Pipelines --> Memory["内存 Reservation + Spill 治理"]
    Memory --> Arrow["流式 Arrow RecordBatch"]
```

Arrow `RecordBatch` 是统一交换格式。Scan、Filter、Projection 在安全时融合；
Aggregate、Join、Sort、Window 是受控的 pipeline breaker。内部 batch 携带内存
lease，通过有界队列传递。所有查询 worker 属于同一个可取消 TaskGroup；阻塞
算子无法取得 reservation 时会切换到分区 Spill。

完整执行模型请参阅[架构文档](docs/architecture.md)。

## 功能状态

- v0.8 发布候选已完成一次 OrbStack 聚焦可靠性门禁，包括真实 MinIO 上的
  CSV/Parquet COPY 与 Native 备份恢复往返；详见[验收约定](docs/acceptance.md)
  和[发行说明](docs/releases/v0.8.0-alpha.1.md)。
- [`benchmarks/tpch`](benchmarks/tpch) 保留 TPC-H Q1-Q22 查询覆盖。
- v0.7 ClickBench 功能门禁在 4 CPU / 16 GiB 容器配置下运行 43 条官方查询一次。
- 已保留的 100 万行运行完成 **43/43 条查询**，证据见
  [`20260717-functional-1m-4c16g.json`](benchmarks/clickbench/evidence/20260717-functional-1m-4c16g.json)。

该 ClickBench 结果只用于验证功能完备性与可靠性，不是跨引擎性能声明。可选的
100M profile 和复现方法位于 [ClickBench 指南](benchmarks/clickbench/README.md)。

## 当前边界

可串行化隔离、savepoint、`MERGE`/upsert、约束、索引、公开 time travel、服务
协议、分布式执行以及 DuckDB SQL/数据库文件兼容不属于 v0.8。JSON/ORC/Iceberg
Scan 和嵌套 LIST/STRUCT/MAP 执行同样排除。没有最外层 `ORDER BY` 时，结果顺序
不作保证。

## 文档

| 文档 | 内容 |
| --- | --- |
| [架构](docs/architecture.md) | Pipeline、调度、内存、裁剪、Native 存储与 Spill |
| [兼容范围](docs/compatibility.md) | SQL、类型、格式和明确限制 |
| [CLI 中文帮助](packaging/dist/CLI.zh-CN.md) / [English](packaging/dist/CLI.md) | 命令、输出、资源和 S3 参数 |
| [安装中文说明](packaging/dist/INSTALL.zh-CN.md) / [English](packaging/dist/INSTALL.md) | 二进制包安装与卸载 |
| [S3 与 MinIO](docs/s3.md) | 凭证、endpoint 和对象存储行为 |
| [故障排查](docs/troubleshooting.md) | 资源、Spill、损坏和输入错误 |
| [验收说明](docs/acceptance.md) | 正确性与版本发布检查 |
| [v0.8 发行说明](docs/releases/v0.8.0-alpha.1.md) | 新增事务 Native 存储、SQL、COPY 与运维能力 |
| [v0.7 迁移](docs/migration-v0.7.md) | 历史执行内核和 benchmark 变化 |
| [v0.8 迁移](docs/migration-v0.8.md) | WAL 格式、显式数据库迁移与事务 API |
| [v0.8 路线图](docs/roadmap-v0.8.md) | Native v3、DML/DDL、COPY/维护与 SQL/时间阶段 |

## 开发

完整默认验证使用 OrbStack：

```sh
scripts/ci/orbstack.sh all
```

也可以执行聚焦检查：

```sh
docker compose run --rm dev cargo fmt --check
docker compose run --rm dev cargo clippy --all-targets -- -D warnings
docker compose run --rm dev cargo test --all-targets
```

请保持算子和数据源模块职责单一，保留流式结果语义，不要引入项目自身的
`unsafe` 或 DataFusion 运行时依赖。

## 许可证

RustDB 使用 [Apache License 2.0](LICENSE)。
