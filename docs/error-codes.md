# RustDB error contract / RustDB 错误契约

RustDB exposes a machine-readable code separately from the human-readable
message. Clients must branch on the code and retry classification, never on
message text. New codes may be added during beta; removing or changing an
existing code follows the two-beta deprecation policy.

RustDB 将机器可读错误码与人类可读消息分离。客户端必须依据错误码和重试分类处理，
不得解析 message 文本。Beta 期间可以增加错误码；删除或修改已有错误码必须遵守两个
Beta 版本的弃用周期。

## Retry classification / 重试分类

| Value | Contract / 契约 |
| --- | --- |
| `never` | Repeating the same operation cannot succeed without changing the request. / 不修改请求时不得重试。 |
| `safe` | Retrying is safe after the reported resource or transient condition changes. / 条件恢复后可安全重试。 |
| `reopen_required` | Discard the current transaction or handle and reopen before retrying. / 丢弃当前事务或句柄，重新打开后重试。 |
| `outcome_unknown` | Do not repeat the operation; reopen and reconcile its durable outcome. / 禁止重复操作，必须重新打开并核对持久结果。 |
| `unknown` | RustDB cannot prove that retrying the same operation is safe. / RustDB 无法证明原操作可安全重试。 |

## Core codes / 核心错误码

| Code | Category |
| --- | --- |
| `request.invalid` | Invalid typed argument or request envelope |
| `request.body_too_large` | HTTP request body exceeded the configured limit |
| `request.content_type` | HTTP request content type is unsupported |
| `request.invalid_json` | HTTP request body is not valid JSON |
| `request.invalid_query` | HTTP query-string parameters are invalid |
| `request.method_not_allowed` | HTTP method is not supported by the resource |
| `request.not_found` | HTTP resource path does not exist |
| `request.too_large` | Submitted SQL or parameter payload exceeded a query limit |
| `sql.parse` | SQL parsing failed |
| `sql.unsupported` | SQL is valid but outside the supported subset |
| `sql.catalog` | Name resolution or Catalog validation failed |
| `auth.required` | A bearer credential is required |
| `auth.invalid` | The bearer credential is invalid or revoked |
| `auth.forbidden` | The authenticated principal is not authorized |
| `idempotency.invalid_key` | The idempotency key is malformed |
| `idempotency.key_conflict` | The key was reused with a different request |
| `idempotency.required` | This request requires an idempotency key |
| `query.active` | The operation requires a terminal Query |
| `query.cancelled` | Query cancellation reached execution |
| `query.no_result` | A terminal Query has no consumable result |
| `query.not_complete` | The requested result is still running |
| `query.not_found` | Query is absent, expired, or hidden by ownership rules |
| `query.resource_exhausted` | Query memory or managed resource admission failed |
| `query.timeout` | Query exceeded its configured deadline |
| `admission.queue_full` | Engine or principal admission queue is full; retry after the advertised delay |
| `admission.resource_limit` | One Query's configured resource reservation exceeds an admission layer |
| `admission.unavailable` | A queued admission decision could not be completed safely |
| `query.server_restarted` | An active durable Query was interrupted by server restart |
| `query.result_invalidated` | A persisted result belongs to an incompatible producer version |
| `query.result_unavailable` | Durable Query metadata references a missing result |
| `query.state_conflict` | Journal, ownership, idempotency, or result state is contradictory |
| `query.journal_failed` | A Query state transition could not be journaled durably |
| `query.delete_failed` | Query deletion could not be completed durably |
| `server.shutting_down` | Server is draining and rejects new work |
| `storage.io` | Local filesystem I/O failed |
| `storage.parquet` | Parquet decoding or metadata validation failed |
| `storage.object_store` | Object-store I/O or consistency validation failed |
| `execution.arrow` | Arrow array or kernel processing failed |
| `execution.failed` | Physical query execution failed |
| `native.disk_quota_exceeded` | Native engine or table disk quota rejected a write |
| `native.storage` | Native storage validation or I/O invariant failed |
| `native.format_unsupported` | The database belongs to an alpha or unsupported Beta format epoch |
| `native.repair_refused` | Conservative Native repair could not prove that the requested mutation is safe |
| `native.import_conflict` | An import id was reused with a different table, source, format, or CSV option |
| `transaction.conflict` | Snapshot-isolation write conflict |
| `transaction.closed` | Transaction handle is already closed |
| `transaction.outcome_unknown` | Durable commit outcome requires reopen reconciliation |
| `transaction.post_commit_failure` | Native commit is durable but response handling failed |
| `copy.post_commit_failure` | COPY output is durable but response handling failed |
| `server.internal` | Internal invariant or unexpected server failure |

The complete source-of-truth mapping, including HTTP gateway codes, lives in
`ErrorCode::as_str()`. HTTP
Problem responses include `error`, `message`, `retry`, and optional correlation
or structured detail fields. The same catalog will be used by the Beta CLI JSON
error mode.

包括 HTTP 网关错误在内的完整映射以 `ErrorCode::as_str()` 为准。HTTP Problem 响应包含 `error`、
`message`、`retry`，以及可选的关联 ID 和结构化详情。Beta CLI 的 JSON 错误模式
也将复用同一目录。
