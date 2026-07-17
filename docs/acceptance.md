# Acceptance and CI

RustDB keeps routine code quality, native portability, TPC-H correctness, and
resource-heavy performance work as separate gates. This prevents every
platform job from downloading or generating the same analytical dataset while
still requiring one real S3-compatible integration run.

## Active v0.7 release gate

The v0.7 release decision is functionality-first and supersedes comparative
performance gates elsewhere in this historical document. Run the 43 official
ClickBench queries once on the official 1M Parquet partition:

```sh
benchmarks/clickbench/run.sh
```

The runner uses an OrbStack container capped at four CPUs and 16 GiB, with four
RustDB compute threads and a 12-GiB engine budget. Acceptance requires 43/43
successful, fully consumed results, retained typed checksums and provenance,
and clean terminal query resources. There is no cross-engine score, repeated
timing requirement, or 100M download requirement. The optional full dataset
profile is `CLICKBENCH_PROFILE=full`.

The TPC-H differential, old fixed-hardware measurements, and low-memory suites
below remain useful regression tools, but they are not blocking v0.7 unless a
focused correctness issue explicitly calls for them.

## Local quality gate

The supported local entrypoint uses the pinned development image and live
MinIO services through OrbStack:

```sh
scripts/ci/orbstack.sh all
```

It runs formatting, strict Clippy, all Cargo targets, required MinIO tests, and
the portable release build. Thin-LTO release targets are linked one at a time
by default to stay within the pinned OrbStack VM memory; set
`RUSTDB_RELEASE_BUILD_JOBS` explicitly on a larger builder. `scripts/ci/check.sh` contains the same Cargo
commands without depending on a particular CI product. To run the live MinIO
test gate directly on a Linux Docker host:

```sh
scripts/ci/with_minio.sh scripts/ci/check.sh test
```

`with_minio.sh` creates an isolated Compose project, provisions the private and
anonymous test buckets, exports only fixed test credentials, and removes the
project and volumes when the command exits. `check.sh test` refuses to run
unless MinIO is marked as required and has a configured endpoint, preventing
the S3 integration suite from being silently skipped.

## Hosted runner selection

The hosted workflow uses explicit image labels rather than `*-latest`:

| Gate | Runner label | Architecture | Scope |
| --- | --- | --- | --- |
| Quality, representative tests with live MinIO, CLI release | `ubuntu-24.04` | Linux x64 | Required |
| Native compile portability | `ubuntu-24.04-arm` | Linux arm64 | Required |
| Native compile portability | `macos-15` | macOS M1 arm64 | Required |

GitHub's current standard-runner table lists these labels for public and
private repositories. It also lists 14 GB of runner storage; public Linux jobs
receive 4 CPUs/16 GB RAM, private Linux jobs receive 2 CPUs/8 GB, and standard
arm64 macOS jobs receive 3 M1 CPUs/7 GB RAM. See the
[GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
Linux arm64 became available to private repositories in January 2026; GitHub's
[official announcement](https://github.blog/changelog/2026-01-29-arm64-standard-runners-are-now-available-in-private-repositories/)
also confirms the `ubuntu-24.04-arm` label.

GitHub documents that arm64 macOS hosted runners do not support nested
virtualization. Consequently, the Linux x64 job is the single hosted
live-MinIO integration gate. It runs the core library, CLI, CSV, Parquet, and
MinIO suites once, while excluding the dedicated low-memory Spill integration
binaries and the long extreme-memory unit case. The complete suite remains
available through the OrbStack release-candidate gate above. Linux arm64 and
macOS arm64 compile every target natively, while the Distribution workflow
builds and validates their release packages. This split exercises the
supported architectures without repeating the same resource-heavy suite on
every runner or pretending a containerized service ran on macOS.
GitHub-provided Actions are documented as arm64 compatible; the workflows
therefore use only `actions/checkout` and `actions/cache`, not community setup
Actions.

Cargo caches contain registry and Git dependency sources only, are separated
by operating system and architecture, and do not cache `target/`. GitHub notes
that cache entries are immutable and must not contain secrets in its
[dependency caching reference](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching).

## TPC-H correctness

Run the focused DuckDB 1.4.3 SQL-semantics differential before TPC-H:

```sh
tools/sql/differential.sh
```

It compares truth predicates, projection aliases, grouping ordinals, HAVING
aliases, hidden sort expressions, scalar/temporal functions, aggregate
DISTINCT, correlated scalar/IN/EXISTS NULL semantics, v0.4 window and
`QUALIFY` behavior, DISTINCT-core set operations, and `RIGHT`/`FULL`/`USING`
joins through canonicalized result checksums. Runtime cardinality failures and
explicitly unsupported frame cases are required outcomes, not allow-failure
cases; `INTERSECT ALL` and `EXCEPT ALL` are positive compatibility cases.

The 22-query checksum gate is deliberately a separate, single Linux x64 job.
Run the fast local development gate with:

```sh
tools/tpch/run.sh 0.01
```

Run the local half of the release correctness gate with SF1:

```sh
tools/tpch/run.sh 1
```

The `TPC-H acceptance` workflow can be started manually with either scale. Its
weekly schedule runs SF1. Generated Parquet data is cached under
`data/tpch-sf<scale>` with a key containing the pinned DuckDB version and the
generation/query scripts. No platform matrix repeats the download or
generation. A cache miss must regenerate and validate the dataset rather than
being treated as a failure.

Acceptance requires sorted checksums to match DuckDB 1.4.3 for Q1 through Q22
on both local Parquet and MinIO. The hosted gate uploads the exact generated
dataset and reruns the same complete query list through `s3://`. To reproduce
the remote half locally:

```sh
uri=$(tools/tpch/upload_minio.sh 1 | tail -n 1)
tools/tpch/compare.sh --report \
  --queries benchmarks/tpch/cases/sf1-minio.txt \
  --rustdb-root "$uri" 1
```

A release record should include the dataset manifest, local and MinIO status
tables/checksums, their generated `provenance.json` files, and the workflow run
URL. Provenance records the exact Git/worktree state, binary and query hashes,
dataset manifest, data root, and execution configuration even when
`TPCH_SKIP_BUILD=1` is used.

## v0.4 operator and pruning gates

The v0.4 differential cases must cover recursive/parenthesized set trees,
positional type coercion, NULL equality, output-name/ordinal sorting, window
peer ties and empty partitions, aggregate windows before and after grouped
aggregation, named windows, `QUALIFY` aliases, outer-join residuals, duplicate
keys, NULL keys, and `USING` output layout. Run focused operator tests under a
64/128 MiB query budget and require non-zero Spill where the fixture exceeds
the budget. The complete result must match DuckDB, peak reservation must stay
within the configured limit, and query directories must be removed after
success, failure, cancellation, panic, and abandoned consumers.

Parquet deep-pruning acceptance runs both local and live MinIO fixtures. It
must demonstrate all of the following from `QueryMetricsSnapshot` and the
object-store request/byte counters:

- unfiltered zero-column `COUNT(*)` reads no page index or Bloom filter;
- page-index selection reduces decoded rows without changing the residual
  predicate result;
- a negative Bloom lookup removes an equality row group, while a positive or
  unsupported lookup keeps it;
- `Disabled` mode returns the same result without deep-metadata reads;
- exhausted query/per-file metadata budgets increment the skip metric and do
  not change results;
- invalid index fields, a Bloom length without an offset, or a malformed Bloom
  header returns a structured error; the legal legacy offset-only Bloom layout
  remains a bounded conservative skip;
- conditional S3 reads still fail if the query-fixed object changes.

The Arrow/Parquet `59.1.0`, object-store `0.13.2`, sqlparser `0.62.0`, and
async-compression `0.4.42` pins are part of the release gate. A dependency
update must be handled as a separate coordinated compatibility change.

## v0.5 low-amplification and CSV gates

The SQL differential suite additionally covers multiset `INTERSECT`/`EXCEPT`,
parser-visible Left Semi/Anti joins, and `ntile`/`percent_rank`/`cume_dist`.
Its final case executes RustDB through `PreparedStatement::execute` and DuckDB
through SQL `PREPARE`/`EXECUTE`, then compares the canonicalized typed result.

CSV acceptance runs through the ordinary local and required-MinIO OrbStack
gate. Raw, concatenated gzip, and multi-frame zstd inputs must return identical
checksums. Tests include magic detection without a matching extension, quoted
newlines across source chunks, a record larger than the morsel target, bounded
sampling, slow consumers, `LIMIT`, cancellation, and abandoned results.
Metrics must distinguish transferred source bytes from decompressed bytes and
report real parser lanes and record-aligned morsels.

Parquet tests cover same-column constant OR/IN pruning at row-group, page-index,
and Bloom levels, including mixed positive/negative candidates and the empty
runtime-filter set. Runtime filters are optimization-only: disabled and budget-
rejected runs must have the same result and residual SQL filter. Concurrent
footer/page-index misses must produce one loader; waiters must cancel promptly
and object-identity changes must use a different cache key.

On the fixed M5 Max SF10 release machine, a 128 MiB query budget must meet:

- Q17 cumulative Spill writes at most 8 GiB, active Spill peak at most 2 GiB,
  and maximum repartition depth one;
- Q21 p50 at least 50% below `v0.4.0-alpha.1`, with p95/p50 at most 1.30;
- Join Spill writes at most three times scanned input, and no more than 512
  active Spill files per query; Spill reads are recorded but have no 3x cap;
- a 10 GiB single uncompressed CSV reaches at least 1.8x parser throughput with
  four compute threads versus one.

Generate (or reuse) the deterministic 10 GiB fixture and enforce that CSV gate
with:

```sh
benchmarks/run_csv_scaling.sh
```

This command is the strict release mode. It refuses a dirty or non-40-character
candidate, non-M5-Max hardware, a non-10-GiB fixture, a version other than
`0.5.0-alpha.1`, non-native/non-release builds, or reports that do not share the
current executable SHA-256. It fixes the engine settings to 1 GiB, batch 8192,
I/O concurrency 32, metadata cache zero, an 8 MiB CSV morsel, no warmup and one
measured run; the 1.8x threshold and these settings cannot be weakened by
environment variables.

The gate checks the measured run: one and four threads must return the fixture's
expected row count and each report a complete source/decompressed-byte read with
cross-thread-identical counters before the throughput ratio is accepted. It
records physical batch counts separately because record-aligned morsel boundaries
may differ. Counters may exceed the fixture size because schema sampling uses the
same measured input path. Configurable harness checks must use
`benchmarks/run_csv_scaling.sh --smoke`; their summary is explicitly marked
`"mode": "smoke"` and `"release_qualified": false` and is not release evidence.

Generate and validate the SF10 resource evidence from a clean candidate commit
with the repeatable runner. Supply the completed low-memory run from the same
commit so its six 128 MiB Join reports can be reused:

```sh
LOW='benchmarks/results/low-memory/<candidate-low-memory-run>'
benchmarks/run_v05_resource_gate.sh \
  --dataset-root data/tpch-sf10 \
  --low-memory-run "$LOW" \
  --output benchmarks/results/v05/<candidate-resource-run>
```

The runner fixes metadata cache to zero and all other resource settings to 128
MiB / four compute lanes / batch 8192 / I/O concurrency 32. It measures Q17 and
candidate Q21, runs each through `tools/tpch/compare_query.sh` and saves the
DuckDB-matched checksum, creates a temporary detached `v0.4.0-alpha.1`
worktree for the comparable Q21 baseline, emits enclosing dataset manifests,
and calls the strict checker. Because the v0.4 benchmark schema predates dataset
provenance, this runner explicitly enables the baseline-only compatibility
exception; candidate and low-memory provenance remain strict. The output path
must not already exist. The runner cleans only its uniquely-created temporary
worktree and never pushes or tags; a failed evidence directory is retained for
diagnosis and cannot be silently reused.

Candidate Spill cleanup is checked after each measured query and rechecked after
the detached baseline, making delayed shared-mount residue a release failure.
The v0.4 process writes to an isolated baseline Spill root; any legacy ghost
directories are counted in the resource manifest and removed only after that
process has exited, without weakening the candidate assertion.

The shared-mount regression is also available as a focused OrbStack gate. It
forces a real aggregate Spill in one container, waits for delayed VirtioFS
writeback, then verifies the same directory through a fresh read-only mount:

```sh
scripts/ci/spill_fresh_mount.sh
```

The default wait is 30 seconds and the probe runs once. Set
`RUSTDB_SPILL_REPLAY_WAIT_SECONDS` only for filesystem diagnostics; preserving
the probe directory requires `RUSTDB_KEEP_SPILL_REPLAY_PROBE=1`.

The candidate Q17 and Q21 reports must live below a run directory whose
`manifest.json` contains the same complete `dataset` object as the clean
candidate low-memory manifest. Keep the rendered canonical `q17.sql` and
`q21.sql` files referenced by the reports; the checker validates their full
contents and common dataset root, not just their filenames.

The checker validates all 24 low-memory manifest entries: exactly 12 cases at
64 and 128 MiB, verified correctness and cleanup assertions, `require_spill`,
unique existing reports/checksum artifacts, candidate build/binary/config,
memory bounds, terminal cleanup, and non-zero Spill read/write evidence. The
six Join arguments must be exactly the 128 MiB Join entries from that manifest.
The manual checker invocation below replays already-captured evidence:

```sh
V05='benchmarks/results/v05/<candidate-resource-run>'
LOW_RUN='benchmarks/results/low-memory/<candidate-low-memory-run>'
LOW="$LOW_RUN/reports"
python3 -B benchmarks/check_v05_resource_gate.py \
  --q17 "$V05/q17.json" \
  --q17-checksum "$V05/q17.checksum.txt" \
  --q21-candidate "$V05/q21.json" \
  --q21-checksum "$V05/q21.checksum.txt" \
  --q21-baseline "$V05/baseline/q21.json" \
  --low-memory-manifest "$LOW_RUN/manifest.json" \
  --allow-legacy-v04-missing-dataset \
  --join "$LOW/inner-join-134217728.json" \
  --join "$LOW/left-join-134217728.json" \
  --join "$LOW/right-join-134217728.json" \
  --join "$LOW/full-join-134217728.json" \
  --join "$LOW/semi-join-134217728.json" \
  --join "$LOW/anti-join-134217728.json"
```

The default checker is release-strict. It requires an Apple M5 Max; a clean,
exact 40-character candidate commit; native release reports from one rebuilt
candidate binary; SF10 manifest provenance; and 128 MiB / four compute lanes /
batch 8192 / I/O concurrency 32. Q21 candidate and baseline must additionally
use comparable build, environment, and metadata-cache settings, with no
warmup and one measured run. The single sample is both p50 and p95. It resolves the local
`v0.4.0-alpha.1` tag and rejects a baseline from another commit.
Candidate Q17, Q21, and the low-memory run must have an identical complete
dataset generation object and manifest digest. Dataset paths may differ only
in spelling or location; their referenced manifest contents must hash to the
recorded digest.

The v0.4 benchmark JSON schema did not carry dataset provenance. Prefer an
enclosing manifest with the same SF10 `dataset` object. If the original v0.4
JSON cannot be enriched, preserve its canonical rendered `q21.sql` next to
`q21.json` and add `--allow-legacy-v04-missing-dataset`. This switch relaxes
only the baseline dataset field, is never implicit, emits a warning, and sets
`legacy_v04_missing_dataset` in the result. Candidate or Join provenance is
never waived. Record use of this compatibility exception in the release
report.

Every constrained run must match its reference checksum, remain within the
query reservation, end with zero active task/reservation/Spill/I/O jobs, and
leave no query directory. Hardware thresholds are release evidence, not hosted
CI timing gates.

## Resource and performance gates

SF10 and fixed-hardware measurements are not routine hosted-CI jobs: the 14 GB
hosted-runner disk limit and shared hardware make them unsuitable for a stable
performance baseline. After generating SF10, run the 64/128 MiB spill suite on
the nominated machine:

```sh
benchmarks/run_low_memory.sh data/tpch-sf10
```

The correlated/DISTINCT constrained regression suite runs selected TPC-H
queries with the same checksum comparison and a hard 128 MiB query budget:

```sh
TPCH_MEMORY_LIMIT_BYTES=134217728 TPCH_REQUIRE_SPILL=1 \
tools/tpch/compare.sh --report \
  --queries benchmarks/tpch/cases/sf10-128m.txt 10
```

The constrained checksum command writes its status and checksums below
`data/tpch-sf10/results/latest`. `benchmarks/run_low_memory.sh` separately
writes timestamped manifests and per-case JSON reports below
`benchmarks/results/low-memory/`. Before an alpha is promoted, verify:

- Sort, Aggregate, Inner/Left/Right/Full/Semi/Anti Join, DISTINCT and ALL set
  operations (including Repeat), and Window checksums are correct.
- The Semi/Anti RustDB templates intentionally exercise parser-visible Left
  Semi/Anti Join with `LEFT SEMI JOIN` and `LEFT ANTI JOIN`. Their `.duckdb.sql`
  companions express the same semantics with `EXISTS` and `NOT EXISTS`; the
  checksum runner selects those companions only for DuckDB 1.4.3.
- Each checksum execution uses its stated 64/128 MiB limit and must itself
  report non-zero Spill before the separately measured run is accepted.
- Peak engine reservation stays within the configured memory limit.
- Spill bytes are non-zero for the forced-spill cases.
- Every query-scoped spill directory is empty after success, error, or cancel.
- Startup orphan tests preserve an old directory while `.rustdb-active` is
  locked, then remove it after lock release; unknown directories stay intact.
- The suite manifest records the RustDB build identifier and generated dataset
  manifest digest.
- Local NVMe and MinIO benchmark results record hardware, cache state,
  executable versions, p50/p95, first-batch latency, RSS, throughput, S3
  requests, and spill metrics.

The two 1,000-iteration lifecycle soaks are deliberately ignored by routine
`cargo test` runs. They remain optional deep diagnostics; the v0.5 alpha release
uses the single full OrbStack lifecycle run plus the focused fresh-mount probe,
so it does not repeat these stress loops by default:

```sh
docker compose run --rm --no-deps dev sh -c '
  cargo test --locked --release --lib \
    runtime::task_group::tests::one_thousand_mixed_lifecycle_release_soak \
    -- --ignored --exact &&
  cargo test --locked --release --lib \
    runtime::compute::tests::one_thousand_abandoned_consumers_release_soak \
    -- --ignored --exact
'
```

Use a small forward run to validate benchmark plumbing before spending time on
the full matrix:

```sh
THREADS_LIST=1 BATCH_SIZES=1024 CACHE_MODES=cold \
  benchmarks/run_baseline.sh --smoke \
  --output benchmarks/results/smoke-forward
```

The v0.5 release-blocking parallel performance sample is local SF10 on an Apple M5
Max. Capture the candidate with only the required matrix dimensions (the
baseline suite may still emit its other query cases; the gate ignores them):

`metadata-warm` is the retained selector for a positive metadata-cache budget;
this single-run gate performs no explicit warmup.

```sh
THREADS_LIST="1 4" BATCH_SIZES=8192 CACHE_MODES=warm \
MEMORY_LIMIT_BYTES=1073741824 WARMUP=0 ITERATIONS=1 START_MINIO=0 \
benchmarks/run_baseline.sh \
  --local-root data/tpch-sf10 \
  --output benchmarks/results/baseline/<candidate-run>

python3 -B benchmarks/check_parallel_gate.py \
  --candidate benchmarks/results/baseline/<candidate-run>/manifest.json \
  --baseline benchmarks/results/baseline/v04-9e1b98c-sf10-strict/manifest.json
```

The baseline must come from a clean `v0.4.0-alpha.1` build over the same SF10
manifest. The gate resolves that local tag and rejects a different baseline
build identifier. The candidate must be the exact 40-character commit of the
current clean worktree. Each target/thread/batch configuration is checksum-run
independently; sharing one checksum path between t1 and t4 is rejected. Runner,
helper-library, and checksum-runner digests are stored in each manifest and
recomputed from the corresponding candidate/baseline worktree. The dataset
manifest digest is also recomputed. Candidate reports contain a SHA-256 of the
running executable; the host-side gate detects the actual CPU with `sysctl`,
rebuilds `rustdb-bench` from the clean HEAD in OrbStack with native flags, and
requires its digest to match every selected report. The gate also fails on
missing or duplicate matrix entries,
unverified checksums, a dataset mismatch, non-M5-Max hardware, a non-release or
non-native build, settings other than local metadata-warm / batch 8192 / 1 GiB
/ no warmup / one measurement, or a report whose build/config does not match
its manifest. Missing evidence is a failure, never a pass.

For both `scan-filter` and `aggregate`, the candidate must satisfy:

- `t1_p50 / t4_p50 >= 2.0` (the four-thread throughput multiplier);
- candidate one-thread p50 no more than 10% slower than v0.4;
- identical result checksums for candidate/baseline at one and four threads.

The runner refuses an existing output directory, builds `rustdb-bench` with
`-C target-cpu=native`, rejects mismatched build-ID/CPU overrides, records the
executable, build, and dataset fingerprints, and checksum-validates results
before timing. Portable CI release builds do not
use native CPU flags, and hosted CI checks parallel correctness without using
this hardware timing gate.

SF1 remains the routine TPC-H correctness gate. SF10 is the separately recorded
resource/performance release gate; absence of its dataset, v0.4 baseline, or
nominated hardware must be reported as not run, never as a pass.
