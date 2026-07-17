# RustDB CLI help

[简体中文](CLI.zh-CN.md)

Run `rustdb --help` for the authoritative option list or `rustdb --help-zh`
for built-in Simplified Chinese help.

## Query input and output

Without `-c` or `-f`, RustDB starts its interactive shell. SQL statements in
the shell and SQL files must end with a semicolon.

```sh
# One statement
rustdb -c "SELECT count(*) FROM read_csv('/data/events/*.csv')"

# A SQL file, rendered as JSON Lines
rustdb -f report.sql --format jsonl

# Machine-readable CSV with an explicit NULL token
rustdb -f report.sql --format csv --csv-null '\N'
```

Formats are `table`, `csv`, and `jsonl`. `--metrics` writes execution metrics
to stderr after the streamed result has been consumed.

## Data sources

```sql
SELECT *
FROM read_parquet('/data/events/*.parquet')
WHERE event_date >= '2026-01-01'
LIMIT 10;

SELECT count(*)
FROM read_csv('/data/events/*.csv.gz', compression = 'auto');
```

Local paths, globs, `file://`, `s3://`, raw CSV, gzip CSV, zstd CSV, and Parquet
are supported. The SQL compatibility boundary is documented in the source
repository; unsupported SQL returns an explicit error.

For AWS S3, credentials are resolved through the default credential chain:

```sh
AWS_PROFILE=analytics rustdb --s3-region us-east-1 -c \
  "SELECT count(*) FROM read_parquet('s3://bucket/events/*.parquet')"
```

For MinIO or another compatible service:

```sh
rustdb --s3-endpoint http://127.0.0.1:9000 \
  --s3-region us-east-1 --s3-path-style --s3-allow-http \
  -c "SELECT * FROM read_parquet('s3://bucket/events/*.parquet') LIMIT 10"
```

Use `--s3-anonymous` only for public objects. RustDB intentionally has no CLI
flags for plaintext access or secret keys.

## Resource controls

The most commonly adjusted options are:

- `--memory-limit 2GiB`: query-engine memory budget;
- `--threads 4`: compute worker count;
- `--batch-size 8192`: target Arrow batch rows;
- `--io-concurrency 16`: concurrent scan tasks;
- `--metadata-cache 256MiB`: Parquet metadata cache;
- `--max-concurrent-queries 1`: admitted query count;
- `--spill-directory PATH`: query Spill root;
- `--spill-engine-limit` and `--spill-query-limit`: hard Spill quotas;
- `--runtime-filter-bytes 8MiB`: Join runtime-filter budget.

Sizes accept `B`, `KB`, `MB`, `GB`, `KiB`, `MiB`, and `GiB`.

## Interactive commands

- `.tables`: list registered external tables;
- `.help` or `.help en`: show English shell help;
- `.help zh`: show Simplified Chinese shell help;
- `.quit` or `.exit`: exit.

Result order is unspecified without an outer `ORDER BY`. Press Ctrl-C to cancel
the active query.
