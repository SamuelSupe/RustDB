# RustDB 诊断报告

`rustdb diagnostics` 生成一份小型、版本化 JSON，供支持和离线排障使用。命令复用
`rustdb native check` 的只读完整性检查，但不会为了恢复、迁移、修复或查询而打开
数据库。

```sh
rustdb diagnostics --database /srv/rustdb/analytics
rustdb diagnostics \
  --database /srv/rustdb/analytics \
  --output rustdb-diagnostics.json
```

不指定 `--output` 时写到 stdout。文件输出会在目标目录中以原子替换发布并执行
fsync；Unix 上权限为 `0600`。命令拒绝覆盖符号链接或非普通文件；为保持数据库
只读契约，输出文件必须位于 Native 数据库目录之外。

## Schema 与内容

顶层 `schema_version` 当前为 `1`。消费者遇到未知版本时必须拒绝，不能猜测字段
含义。报告包含：

- RustDB 版本及目标操作系统、CPU 架构；
- UTC 生成时间；
- 带域分隔的数据库路径 SHA-256 指纹，不包含明文路径；
- Native check 计数、格式/Catalog generation 和去重后的 issue code；
- 探针成功时的数据库字节数、文件系统总量和可用量；
- 非敏感的内存、并发、扫描、Spill、Native 配额与 S3 模式配置摘要。

报告明确排除数据库/文件路径、文件名、文件内容、SQL、查询结果、Token、Token
digest、凭证、S3 region/endpoint 具体值、credential provider 内容、表名，以及
Spill/result 目录。探针失败只记录稳定 code，不嵌入操作系统错误文本。

该 JSON 仍属于运维元数据；分享前应人工检查，并通过可信支持渠道传输。它不是
备份，不能用于修复或恢复数据库。
