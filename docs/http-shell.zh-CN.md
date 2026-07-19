# RustDB 只读 HTTP Shell

RustDB Beta 可以通过 HTTPS 把一个 Native 数据库提供给现有命令行查询体验。
服务端只执行只读 SQL；它不是浏览器 Shell、写入 API 或远程管理服务。

精确协议见 [OpenAPI v1](openapi-v1.yaml)，设计边界见
[Beta 中文运维指南](operator-guide.zh-CN.md)，英文说明见 [http-shell.md](http-shell.md)。

## 启动本地服务

最安全的默认行为是仅监听回环地址：

```sh
rustdb serve --database /srv/rustdb/analytics
```

首次启动会在操作系统用户状态目录生成本地 CA、短周期服务端证书、`admin`
principal、随机 Profile Token 和连接元数据。`principals.json` 只保存 Token 的
SHA-256 digest；明文只存在于权限受限的 Profile Token 文件且不会写入日志。旧版
服务端 `bearer.token` 不会被接受。这些敏感文件不会写入 Native 数据库目录。进程保持前台运行，
需要守护时请使用 systemd、launchd、Docker 等进程管理器。

允许远程连接时，必须同时显式配置监听地址和客户端可访问的 HTTPS URL：

```sh
rustdb serve \
  --database /srv/rustdb/analytics \
  --listen 0.0.0.0:7400 \
  --advertise-url https://analytics.example.com:7400
```

advertise URL 的主机名或 IP 会写入证书 SAN；RustDB 拒绝把 `0.0.0.0` 写入客户端
Profile。所有 TCP 监听均强制 TLS，不提供明文 HTTP 模式。

## 导出和导入 Profile

停止服务后，在服务端主机导出权限受限的连接包：

```sh
rustdb profile export \
  --database /srv/rustdb/analytics \
  --output analytics.rustdb-profile
```

通过 SSH、U 盘等可信离线渠道传输连接包，然后在客户端导入：

```sh
rustdb profile import analytics.rustdb-profile --name analytics
```

命名 Profile 只记录 advertise URL、CA 文件路径和 Token 文件路径。Token 文件使用
严格权限，命令行不接受明文 Token 参数。

principal 必须在服务停止时由本地 CLI 管理：

```sh
rustdb principal list --database /srv/rustdb/analytics
rustdb principal create --database /srv/rustdb/analytics --id analyst --role query
rustdb token list --database /srv/rustdb/analytics --principal analyst
rustdb token rotate --database /srv/rustdb/analytics --principal analyst
rustdb token revoke --database /srv/rustdb/analytics --token-id <UUID>
rustdb profile export --database /srv/rustdb/analytics \
  --token-id <UUID> --output analyst.rustdb-profile
```

轮换会先增加一个可重叠使用的新凭证，客户端切换后再显式撤销旧 Token。`token list`
只显示 UUID、principal、生命周期状态和有效期；`profile export --token-id` 复用服务端
已管理连接包的 URL 与 CA，需要明确覆盖 HTTPS origin 时再加 `--server-url`。query 角色
只能访问自己创建的 Query，admin 可以访问所有 Query；Idempotency Key 按提交 principal
隔离。`--no-auth` 仅用于显式开发配置，所有请求归属固定 anonymous Query owner，并且
不会获得 admin 权限。

## 使用远程 CLI 查询

启动交互式 Shell：

```sh
rustdb shell --profile analytics
```

协议层会创建后台 Query ID，但官方 CLI 会在背压下轮询有序 Arrow IPC batch，并按
本地查询方式输出。`Ctrl-C` 会请求取消远程查询。Beta 不提供 `\jobs`、`\fetch` 等
作业管理命令。

脚本模式也会等待查询结束，并根据最终 Query 状态设置进程退出码：

```sh
rustdb shell --profile analytics \
  -c "SELECT region, sum(amount) FROM sales GROUP BY region" \
  --format table

rustdb shell --profile analytics -f report.sql --format csv > report.csv
rustdb shell --profile analytics -f report.sql --format jsonl > report.jsonl
```

远程 Shell 保留 `table`、`csv`、`jsonl` 三种本地输出。完整输出成功后，CLI 会显式
删除服务端结果；输出中断或客户端断开时，结果保留到默认一小时 TTL，便于重试。

## 只读 SQL 边界

每次远程请求只能包含一条以下语句：

- `SELECT`，包括非递归 `WITH`；
- `VALUES`；
- `SHOW`、`DESCRIBE`；
- `EXPLAIN`、`EXPLAIN ANALYZE`。

远程查询可以读取 Native 表、系统表，以及服务端本地持久注册的 CSV/Parquet
数据源。`read_csv('/path')`、`read_parquet('s3://bucket/key')` 等直接文件函数会被
拒绝，DDL、DML、事务、维护、上传和数据源管理同样不开放。服务端校验解析后的
语句，并在展开持久 View 时再次执行策略，不依赖 SQL 关键字字符串过滤。

需要修改数据源时，应先停止 `rustdb serve`，使用服务端本地 CLI 更新持久注册，
再重启服务。S3 凭证只放在服务进程的默认凭证链中，不写入数据源元数据或 Profile。

```sh
rustdb datasource add-csv \
  --database /srv/rustdb/analytics \
  --name web_events \
  --location 's3://lake/events/*.csv.gz'

rustdb datasource add-parquet \
  --database /srv/rustdb/analytics \
  --name sales \
  --location 's3://lake/sales/*.parquet'

rustdb datasource list --database /srv/rustdb/analytics
rustdb datasource refresh --database /srv/rustdb/analytics --name sales
rustdb datasource remove --database /srv/rustdb/analytics --name web_events
```

定义保存在可选 Native Catalog 附属文件 `catalog/external-sources.json`，原子更新并
随 Native backup/restore 一起复制。文件只包含 location 和非敏感格式选项，不含
凭证。`rustdb serve` 持有数据库锁时，本地修改命令会自然失败。

## 类型化参数

HTTP 协议支持 `?` 或连续的 `$1` 参数，同一语句不能混用。每个值必须显式声明
类型：

```sh
curl --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: report-2026-07-19-001' \
  --data '{
    "sql": "SELECT * FROM sales WHERE day >= $1 AND region = $2",
    "parameters": [
      {"type": "date32", "value": 20653},
      {"type": "utf8", "value": "APAC"}
    ],
    "timeout_ms": 60000
  }' \
  https://analytics.example.com:7400/v1/queries
```

参数只能替代表达式值，不能替代表名、列名、数据源 pattern、S3 配置或表函数
选项。规范 wire type 包括 `boolean`、`int64`、`uint64`、`float64`、
`decimal128`、`utf8`、`binary`、`date32` 和 `timestamp_microsecond`。任一类型的
`value` 都可以使用 JSON `null` 表示对应 typed NULL；Decimal 还必须提供
`precision` 和 `scale`。准确结构见 OpenAPI 的 `TypedParameter`。

## 原始 HTTP 生命周期

每次提交都必须提供 Idempotency Key：

```sh
QUERY_ID=$(
  curl --silent --cacert ca.pem \
    -H "Authorization: Bearer $(<token)" \
    -H 'Content-Type: application/json' \
    -H 'Idempotency-Key: example-query-0001' \
    --data '{"sql":"SELECT count(*) AS rows FROM sales"}' \
    https://analytics.example.com:7400/v1/queries |
  jq -r .query_id
)
```

服务端比较 JSON 反序列化后的请求 envelope，而不是原始请求字节。空白和对象字段
顺序不参与比较；省略空 `parameters` 与显式传入 `"parameters": []` 等价。相同 Key
配合不同的已解码 envelope 会返回 `409 idempotency.key_conflict`。

轮询状态直到 Query 终止：

```sh
curl --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}"
```

状态时间使用 Unix epoch 毫秒：`created_at_ms` 必有，`started_at_ms` 和
`finished_at_ms` 按阶段出现。成功 Query 包含受限的 `metrics`；失败或取消 Query
改为包含 `error`。尚不存在的可选字段会被省略，而不是编码成 JSON `null`。

使用 offset 获取 JSON：

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/json' \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/results?offset=0&limit=1000"
```

也可以用上一页返回的不透明 Cursor 获取 NDJSON：

```sh
curl --compressed --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/x-ndjson' \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/results?cursor=${CURSOR}&limit=1000"
```

JSON/NDJSON 仅在 Query 成功完成后可读。Cursor 与 offset 互斥；页面不可变且可重复
读取，服务端没有共享读取游标。

执行期间需要增量、精确类型输出时，每次只请求一个 Arrow batch sequence：

```sh
curl --dump-header batch.headers --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  -H 'Accept: application/vnd.apache.arrow.file' \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/results?batch_seq=0" \
  --output batch-000.arrow
```

Arrow `200` 是独立 IPC file，且只包含一个 RecordBatch；带
`X-RustDB-Result-Complete: true` 的 schema-only IPC file 表示完成。请求序号尚未提交
时返回无 body 的 `204` 和 `Retry-After: 1`。客户端只能前进到服务端返回的精确
`X-RustDB-Next-Batch-Seq`，且不得把 `batch_seq` 与 `cursor`、`offset`、`limit` 混用。

取消运行中 Query，终止后再删除其状态和结果：

```sh
curl -X POST --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}/cancel"

curl -X DELETE --cacert ca.pem \
  -H "Authorization: Bearer $(<token)" \
  "https://analytics.example.com:7400/v1/queries/${QUERY_ID}"
```

## 结果编码

JSON 页面结构为 `{schema, rows, page}`；NDJSON 首行是 `schema`，之后每行一个
`row` 数组，末行是 `page`。结果行使用位置数组，因此可以保留列顺序和重复列名。
每个持久结果 batch 都带 SHA-256，读取及重启恢复时都会校验；不一致会使该结果失败并
失效，不会返回被修改的数据。

```json
{
  "schema": [
    {"name": "region", "data_type": "Utf8", "nullable": false},
    {"name": "total", "data_type": "Decimal128(38, 2)", "nullable": true}
  ],
  "rows": [["APAC", 1234.50]],
  "page": {
    "offset": 0,
    "row_count": 1,
    "complete": true
  }
}
```

等价 NDJSON framing 为：

```jsonl
{"type":"schema","columns":[{"name":"region","data_type":"Utf8","nullable":false},{"name":"total","data_type":"Decimal128(38, 2)","nullable":true}]}
{"type":"row","values":["APAC",1234.50]}
{"type":"page","page":{"offset":0,"row_count":1,"complete":true}}
```

整数、浮点数和 Decimal 使用 JSON number。官方 CLI 保留原始 JSON number token，
Int64、UInt64 和 Decimal 不经过浮点转换；把所有数字解析成浮点的
第三方客户端可能丢失精度。非有限浮点数使用字符串 `"NaN"`、`"Infinity"`、
`"-Infinity"`；Date 使用 ISO `YYYY-MM-DD`，无时区 Timestamp 使用不带 `Z` 的
微秒 ISO 文本，带时区 Timestamp 使用 RFC 3339，Binary 使用 Base64。

默认页面为 1,000 行；单页最多 10,000 行，编码后的行数组总量不超过 16 MiB，
Schema 和 JSON/NDJSON framing 会让 HTTP body 略大。非终页包含 `next_cursor`，终页
省略该字段。默认返回 JSON；精确的 `application/x-ndjson` media type 选择 NDJSON。
客户端声明 `Accept-Encoding: gzip` 时，值得压缩的响应会使用 gzip。

## 配置与运行

服务配置可以来自 TOML、`RUSTDB_*` 环境变量和 CLI，优先级为：

```text
CLI > 环境变量 > TOML > 默认值
```

默认同时执行 1 条 Query、加权公平排队 64 条、Query 超时 30 分钟、结果 TTL 1 小时；
结果存储硬上限为 10 GiB，单 Query 结果硬上限为 2 GiB。所有 principal 默认权重
相同，Admin 角色没有调度优先级。

部署默认值不合适时，可通过以下 `serve` 配置项调整：

| 目的 | CLI | TOML（`[server]` / `[engine]`） | 环境变量 |
| --- | --- | --- | --- |
| 结果目录与保留时间 | `--result-directory`、`--result-ttl-secs` | `result_directory`、`result_ttl_secs` | `RUSTDB_RESULT_DIRECTORY`、`RUSTDB_RESULT_TTL_SECS` |
| 全局/单 Query 结果配额 | `--result-global-limit`、`--result-query-limit` | `result_global_limit`、`result_query_limit` | `RUSTDB_RESULT_GLOBAL_LIMIT`、`RUSTDB_RESULT_QUERY_LIMIT` |
| Query 内存/Spill/结果 reservation | `--query-memory-limit`、`--query-spill-limit`、`--query-result-limit` | `query_memory_limit`、`query_spill_limit`、`query_result_limit` | `RUSTDB_HTTP_QUERY_MEMORY_LIMIT`、`RUSTDB_HTTP_QUERY_SPILL_LIMIT`、`RUSTDB_HTTP_QUERY_RESULT_LIMIT` |
| Principal 运行/排队上限 | `--principal-max-running`、`--principal-max-queued` | `principal_max_running`、`principal_max_queued` | `RUSTDB_HTTP_PRINCIPAL_MAX_RUNNING`、`RUSTDB_HTTP_PRINCIPAL_MAX_QUEUED` |
| Principal 资源 reservation | `--principal-memory-limit`、`--principal-spill-limit`、`--principal-result-limit` | `principal_memory_limit`、`principal_spill_limit`、`principal_result_limit` | `RUSTDB_HTTP_PRINCIPAL_MEMORY_LIMIT`、`RUSTDB_HTTP_PRINCIPAL_SPILL_LIMIT`、`RUSTDB_HTTP_PRINCIPAL_RESULT_LIMIT` |
| Principal 公平调度权重 | `--principal-weight` | `principal_weight` | `RUSTDB_HTTP_PRINCIPAL_WEIGHT` |
| Spill 硬上限 | `--spill-engine-limit`、`--spill-query-limit` | `spill_engine_limit`、`spill_query_limit` | `RUSTDB_SPILL_ENGINE_LIMIT`、`RUSTDB_SPILL_QUERY_LIMIT` |
| 认证开发开关 | `--no-auth` | `no_auth` | `RUSTDB_NO_AUTH` |
| AWS Region 与 endpoint | `--s3-region`、`--s3-endpoint` | `s3_region`、`s3_endpoint` | `RUSTDB_S3_REGION`、`RUSTDB_S3_ENDPOINT` |
| S3 请求模式 | `--s3-path-style`、`--s3-allow-http`、`--s3-anonymous` | `s3_path_style`、`s3_allow_http`、`s3_anonymous` | `RUSTDB_S3_PATH_STYLE`、`RUSTDB_S3_ALLOW_HTTP`、`RUSTDB_S3_ANONYMOUS` |

这些 S3 设置作用于服务端本地持久注册的数据源。`--s3-allow-http` 只适用于可信开发
endpoint，`--s3-anonymous` 只适用于公开对象；其他情况下由服务进程的默认凭证链提供
凭证。

`/healthz`、`/readyz` 无需认证，但只返回 `ok`、`ready` 或 `not_ready`。每个 `/v1`
请求默认都会在解析 body 或 query parameter 前完成认证；只有显式 no-auth 开发模式
例外。Prometheus `/metrics` 同样需要认证，并要求 Admin 权限。CORS 始终关闭。
HTTP trace 不记录请求 body 或 SQL 原文；私有轮转 JSONL audit 会记录身份和 SQL
fingerprint。

Token 只能在服务停止时管理。轮换会增加一枚可重叠 Token；分发并验证新 Profile 后，
再显式撤销旧 Token。长期 CA 保持不变，短周期服务端证书会在启动时自动续签。

## 常见问题

| 现象 | 含义和处理方式 |
| --- | --- |
| TLS 主机名错误 | 必须通过 advertise URL 连接。如果 URL 已变化，先停服，仅删除 `rustdb serve` 打印的自动连接包路径，重启后再导出/导入新包。 |
| `401` | 导入当前 Profile；Token 轮换后重新分发。 |
| `409 idempotency.key_conflict` | 换一个 Key，或使用相同的已解码请求 envelope 重试。 |
| `409 query.not_complete` | 请求 JSON/NDJSON 前继续轮询；运行期间已提交输出可用有序 Arrow IPC。 |
| `429` | 64 条队列已满，按 `Retry-After` 等待。 |
| `422 sql.unsupported` | 仅使用允许的只读语句和已注册关系。 |
| `query.resource_exhausted` | 缩小结果、消费/删除保留结果，或把 `--result-directory` 放到合适的文件系统。 |
| Query 在重启时仍为 queued/running | 重启恢复后会以 `query.server_restarted` 和 safe retry class 标记失败；请提交新请求。 |
| 已完成 Query 在重启后不存在 | 结果已过 TTL、被删除、校验失败，或属于其他 principal；重新提交前先检查服务日志。 |

错误响应包含稳定字符串 `error` 错误码、可读 `message`、必需的 `retry` 重试
分类，以及可选的 `request_id`、`query_id` 和结构化 `details`。客户端不得从
message 文本推断重试安全性。收集诊断信息时只提供这些 ID，不要提交 Token 或
Profile 连接包。

```json
{
  "error": "sql.unsupported",
  "message": "HTTP queries allow SELECT, WITH, VALUES, SHOW, DESCRIBE, EXPLAIN, and EXPLAIN ANALYZE only",
  "retry": "never",
  "request_id": "9ae24e09597d4b28aec5a455263a70b2"
}
```
