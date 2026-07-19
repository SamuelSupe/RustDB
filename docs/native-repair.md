# Native check and repair / Native 检查与修复

`rustdb native check PATH` performs a strictly read-only integrity scan. It
validates the database marker, active catalog generation, referenced table
manifests, segments, predicate sidecars, and delete vectors. `--json` emits the
same report as structured JSON.

`rustdb native check PATH` 执行严格只读的完整性扫描，验证数据库 marker、当前
Catalog generation，以及被引用的表 manifest、segment、谓词 sidecar 和 delete
vector。使用 `--json` 可输出相同报告的结构化 JSON。

`rustdb native repair PATH` is also read-only: it prints a conservative plan.
Writes require an explicit `--apply`. The apply path acquires the existing
database lock, rebuilds the plan, creates a private sibling metadata backup,
applies individually durable actions, and runs the complete check again.

`rustdb native repair PATH` 同样只读，只输出保守修复计划。只有显式指定
`--apply` 才会写入。执行时会获取已有数据库锁、重新生成计划、创建私有的同级元数据
备份、逐项持久化安全操作，最后再次运行完整检查。

Supported repairs are intentionally narrow:

- set private permissions on verified RustDB directories and files;
- delete RustDB-owned staging or identical atomic temporary files only after
  they have been inactive for at least 24 hours and are not catalog/WAL
  referenced;
- restore `catalog/CURRENT` only when the highest valid catalog generation has
  matching database identity, complete table verification, and conservative
  WAL/checkpoint proof.

支持的修复范围刻意保持很窄：

- 修正已验证 RustDB 目录和文件的私有权限；
- 仅删除至少 24 小时未活动、由 RustDB 所有、且未被 Catalog/WAL 引用的 staging
  或与目标完全相同的原子临时文件；
- 仅当最高有效 Catalog generation 的数据库身份一致、全部表引用验证通过且具有
  保守的 WAL/checkpoint 证明时，恢复 `catalog/CURRENT`。

Repair never rebuilds or edits segments, sidecars, or delete vectors and never
guesses missing data. Any unresolved data error blocks mutation with
`native.repair_refused`. Metadata backups contain checksums but intentionally do
not copy segment data. Keep the backup until a successful post-repair check has
been reviewed.

修复不会重建或修改 segment、sidecar、delete vector，也不会猜测缺失数据。任何未
解决的数据错误都会以 `native.repair_refused` 阻止修改。元数据备份带校验和，但刻意
不复制 segment 数据；请在确认修复后检查成功前保留该备份。
