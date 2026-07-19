# Idempotent Native import / 幂等 Native 导入

RustDB Beta can persist CSV or Parquet data as a new Native table. Every import
requires a caller-selected `import_id`:

```bash
rustdb import \
  --database ./warehouse \
  --table events \
  --location ./events.csv.gz \
  --format csv \
  --import-id events-2026-07-19 \
  --header present \
  --compression auto
```

The table and its import receipt are published in the same Native Catalog
generation. Repeating the exact request returns the original receipt without
opening the source or writing another snapshot. Reusing an id with a different
table, normalized location, format, or CSV option fails with
`native.import_conflict`. Imports never replace an existing table, view, or
persistent external source. Import ids are permanent database-level keys; a
later `DROP TABLE` does not make an id reusable.

If a process stops before Catalog publication, reopen the database and submit
the same request again. If the client receives
`transaction.outcome_unknown` or `transaction.post_commit_failure`, it must
reopen before retrying; the durable receipt then makes the retry a replay. Do
not change the request while reconciling an uncertain outcome.

CSV imports expose `header`, one-byte ASCII `delimiter`, and
`compression=auto|none|gzip|zstd`. `auto` detects compression from magic bytes.
Parquet imports reject CSV-only options. Local relative paths are normalized to
absolute lexical paths before fingerprinting; S3 URIs are normalized without
storing credentials.

---

RustDB Beta 可以把 CSV 或 Parquet 持久化为新的 Native 表。每次导入都必须由调用方
提供 `import_id`。表引用和导入回执会在同一个 Native Catalog generation 中发布。
完全相同的请求再次提交时，会直接返回原回执，不会重新打开数据源，也不会重复写入
快照。若同一 ID 对应的表、规范化路径、格式或 CSV 选项不同，则返回
`native.import_conflict`。导入绝不会覆盖已有表、View 或持久外部数据源。
导入 ID 是数据库级永久键；之后执行 `DROP TABLE` 也不会让该 ID 变为可复用。

如果进程在 Catalog 发布前中断，重新打开数据库后提交完全相同的请求即可。如果客户
端收到 `transaction.outcome_unknown` 或 `transaction.post_commit_failure`，必须先
重新打开数据库再重试；持久回执会让这次请求安全地转为 replay。核对不确定结果时不
应修改请求内容。

CSV 导入支持 `header`、单字节 ASCII `delimiter` 和
`compression=auto|none|gzip|zstd`；`auto` 根据 magic bytes 检测压缩格式。
Parquet 导入拒绝 CSV 专用选项。本地相对路径会先规范化为绝对词法路径；S3 URI 会在
不保存凭证的前提下规范化。
