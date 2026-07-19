# RustDB v1.0 Beta contract

`v1.0.0-beta.1` is the first compatibility-bearing RustDB release. It turns the
existing single-node OLAP engine into a feature-complete Beta without expanding
the supported data-format or SQL surface.

`v1.0.0-beta.1` 是首个承担兼容性承诺的 RustDB 版本。它把现有单机 OLAP
引擎提升为功能完整的 Beta，但不新增数据格式或 SQL 范围。

This document is the Beta completion contract, not evidence that the release
gate has passed. Operational procedures live in the
[operator guide](operator-guide.md) and [中文运维指南](operator-guide.zh-CN.md);
the tag-specific outcome belongs in the
[beta.1 release notes](releases/v1.0.0-beta.1.md).

## Compatibility epoch

- The Beta Native database marker is epoch `3`. Alpha epochs `1` and `2` are
  rejected before RustDB changes permissions, acquires a database lock, replays
  WAL, or removes temporary files. Alpha data must be imported into a fresh Beta
  database.
- Compatibility starts at beta.1. During the Beta series RustDB supports the
  current and previous two Beta format/API revisions. A removal requires at
  least two Beta releases of deprecation notice.
- `/v1`, the CLI, the versioned configuration file, stable error codes, retry
  classes, and the outer Rust API are compatibility surfaces. Internal plans,
  operators, and on-disk implementation details remain private.

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
   eight concurrent clients, and one preloaded ClickBench pass. beta.1 records
   a functional baseline and makes no cross-engine performance claim.

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
production warranty or SLA. Alpha-to-Beta handling is documented in
[migration-v1-beta.md](migration-v1-beta.md), and the authoritative SQL,
platform, and object-store boundary is in [compatibility.md](compatibility.md).
