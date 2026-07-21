# Rebuilding data for RustDB v1.0.0-beta.2

RustDB Beta 2 starts Native format epoch `4`. There is intentionally no in-place
migration from any earlier RustDB database, including `v1.0.0-beta.1`.

## Required procedure

1. Keep the earlier database and binary unchanged as a reference.
2. Export or identify the original CSV/Parquet objects used to build it.
3. Install the Beta 2 binary and create a new, empty Native database directory.
4. Import the source data into that directory and verify row counts and query
   checksums.
5. Create and verify a Beta 2 snapshot before redirecting local readers or the
   read-only HTTP service.

Opening an epoch `1`, `2`, or `3` marker with Beta 2 returns
`native.format_unsupported`. The check
happens before chmod, locking, WAL recovery, or cleanup, so inspecting an old
directory cannot mutate it. `rustdb migrate PATH` remains as a format validation
command; on an epoch `4` database it is a no-op, and on any earlier database it
explains that re-import is required. Beta 2 does not provide a compatibility,
migration, or rollback contract for Beta 1 artifacts.

Embedded callers that construct `S3Config` with a custom
`AwsCredentialProvider` must rebuild against `object_store` `0.14.1`. The
provider type is part of the public configuration boundary, so a provider
compiled against `0.13` is not source-compatible with Beta. The default
credential chain and CLI configuration require no code migration.

Review the [compatibility matrix](compatibility.md) before rebuilding. Use the
[operator guide](operator-guide.md) for supported filesystems, configuration,
backup verification, service identity, and the release gate. Do not copy alpha
HTTP state or retained results into a Beta deployment.

## 中文说明

Beta 2 启用 Native 格式 epoch `4`，不提供从任何旧版数据库（包括 Beta 1）原地
升级。请保留旧目录作为参考，在新的空 Beta 2 目录中从 CSV/Parquet 重新导入，校验
行数和查询 checksum，并在切流前创建及验证 Beta 2 快照。Beta 2 遇到 epoch `1`、
`2` 或 `3` marker 会返回 `native.format_unsupported`，且在报错前不会修改旧目录的
权限、锁、WAL 或临时文件。
重建前请核对[兼容矩阵](compatibility.md)，部署、身份、备份验证和发行门禁以
[中文运维指南](operator-guide.zh-CN.md)为准；不要把 alpha HTTP 状态或保留结果复制到
Beta 部署。

嵌入式调用方如果为 `S3Config` 注入自定义 `AwsCredentialProvider`，需要改用
`object_store` `0.14.1` 重新编译；基于 `0.13` 的 provider 类型与 Beta 不源码兼容。
使用默认凭证链或 CLI 配置的用户不需要代码迁移。
