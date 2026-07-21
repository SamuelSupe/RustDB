# RustDB v1.0 Beta contract

`v1.0.0-beta.2` is a reliability reset for the existing single-node OLAP
engine. It does not expand the supported data-format or SQL surface and does
not provide migration compatibility with Beta 1.

`v1.0.0-beta.2` 是现有单机 OLAP 引擎的可靠性重置版本，不新增数据格式或 SQL
范围，也不提供从 Beta 1 迁移的兼容承诺。

This document is the Beta completion contract, not evidence that the release
gate has passed. Operational procedures live in the
[operator guide](operator-guide.md) and [中文运维指南](operator-guide.zh-CN.md);
the tag-specific outcome belongs in the
[Beta 2 release notes](releases/v1.0.0-beta.2.md).

## Compatibility epoch

- The Beta 2 Native database marker is epoch `4`. Earlier epochs `1`, `2`, and `3` are
  rejected before RustDB changes permissions, acquires a database lock, replays
  WAL, or removes temporary files. Earlier data must be imported into a fresh
  Beta 2 database.
- Beta 2 intentionally provides no Beta 1 migration or rollback contract. Its
  Native format, service config, and HTTP contract are versioned explicitly so
  old artifacts fail before mutation instead of being guessed compatible.

## Beta completion gates

1. **Crash safety:** atomic Native publication, WAL recovery, read-only
   integrity checking, conservative repair, and verified snapshots.
2. **HTTP lifecycle:** multiple principals with query/admin roles, hashed and
   rotatable credentials, query ownership, persistent query state, incremental
   Arrow IPC results, exact JSON/NDJSON, resume by batch sequence, and bounded
   backpressure.
3. **Resource governance:** engine, principal, and query limits; weighted fair
   admission; bounded background maintenance; disk reserve of both 10% and
   1 GiB; retained Native storage no greater than 2x source bytes plus bounded
   per-table metadata, with write admission accounting for old and staged
   snapshots.
4. **Operations:** structured logs, Prometheus metrics, JSONL audit records,
   diagnostics, strict versioned configuration, backup verification, and an
   operator handbook.
5. **Supply chain:** non-root OCI images for Linux x86_64/arm64, SBOM, dependency
   and secret scanning, signed release artifacts, and commit-bound acceptance
   evidence.
6. **Acceptance:** one complete OrbStack run on a 4-core/16-GiB-class machine,
   followed by an explicit equivalent local/MinIO CSV-or-Parquet workload with
   2-GiB and 4-GiB engine limits, at least 100 GiB / 10,000 files or objects,
   eight concurrent clients, and one preloaded ClickBench pass. ClickBench is
   fixed to four CPUs, a 12-GiB container limit, a 4-GiB engine budget, batch
   8192, and I/O concurrency 16. Canonical official queries remain the pinned
   source/full profile; functional acceptance runs a versioned deterministic
   derivative with complete tie-breakers and binds both SHA-256 identities.
   Beta 2 records a functional baseline and makes no cross-engine performance
   claim. A digest-pinned `clickhouse-local` rowset is used only for correctness
   oracle generation and review, not during release acceptance.

   中文：ClickBench 只运行一遍，固定使用 4 CPU、12 GiB 容器上限、4 GiB Engine
   上限、batch 8192 和 I/O 并发 16。官方规范查询继续作为固定的来源/full profile；
   功能验收执行带完整 tie-breaker 的版本化确定性派生查询，并同时绑定两份 SHA-256。
   按 digest 固定的 `clickhouse-local` 行集只用于生成和复核正确性 oracle，不进入发行
   验收，也不用于性能比较。

## Deliberate limits

HTTP remains read-only. Writes are available only through the embedded Rust API
and local CLI. Beta does not add HA, distributed execution, remote DML, Windows,
FlightSQL, PostgreSQL Wire Protocol, SDKs, a graphical UI, built-in at-rest
encryption, point-in-time recovery, Iceberg, JSON, or ORC.

Beta validation is deliberately a focused release gate, not a long-running soak
or production availability claim. RustDB provides no production SLA during the
Beta series.

## Release evidence

The final acceptance is run once with `scripts/ci/beta_acceptance.sh` on the
stated host. It rejects a dirty worktree, implicit fixture downloads, missing
large inputs, and an output path inside the repository. It writes normalized
fixture inventories, step logs, four external-run reports, the ClickBench
manifest, and a sealed `<output>/evidence.json`. Failed or interrupted runs also
retain failed evidence. Dedicated low-memory Spill stress is not repeated.

The distribution workflow requires an annotated tag with these exact trailers:

```text
RustDB-Acceptance-SHA: <40-hex commit>
RustDB-Acceptance-Status: passed
```

The tag assertion is commit-bound but remains Beta acceptance evidence, not a
production warranty or SLA. The Beta 2 fresh-start boundary is documented in
[migration-v1-beta.md](migration-v1-beta.md), and the authoritative SQL,
platform, and object-store boundary is in [compatibility.md](compatibility.md).
