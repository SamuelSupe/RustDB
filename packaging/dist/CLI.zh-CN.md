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
- `--threads 4`：计算线程数；
- `--batch-size 8192`：Arrow 批次目标行数；
- `--io-concurrency 16`：并发 Scan 任务数；
- `--metadata-cache 256MiB`：Parquet 元数据缓存；
- `--max-concurrent-queries 1`：最大并发查询数；
- `--spill-directory PATH`：查询 Spill 根目录；
- `--spill-engine-limit`、`--spill-query-limit`：Spill 硬配额；
- `--runtime-filter-bytes 8MiB`：Join Runtime Filter 内存预算。

大小单位支持 `B`、`KB`、`MB`、`GB`、`KiB`、`MiB`、`GiB`。

## 交互终端命令

- `.tables`：列出已注册的外部表；
- `.help` 或 `.help en`：英文帮助；
- `.help zh`：中文帮助；
- `.quit` 或 `.exit`：退出。

没有外层 `ORDER BY` 时不保证结果顺序。按 Ctrl-C 可取消当前查询。
