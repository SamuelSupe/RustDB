#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$SCRIPT_DIR/suites/lib.sh"

usage() {
  cat >&2 <<'EOF'
usage: benchmarks/run_v05_resource_gate.sh \
  --dataset-root ROOT --low-memory-run DIR [options]

Required:
  --dataset-root ROOT   workspace-local SF10 Parquet root
  --low-memory-run DIR  clean-candidate low-memory run containing six 128 MiB Join reports

Options:
  --output DIR          default: benchmarks/results/v05/<UTC timestamp>
  --baseline-tag TAG    default: v0.4.0-alpha.1
  -h, --help            show this help

The runner requires a clean candidate commit and a complete verified 24-case
low-memory manifest. It records Q17 once, records Q21 after two warmups for five
iterations on both the candidate and detached v0.4 baseline, and records
DuckDB-matched Q17/Q21 checksums. All reports use 128 MiB, four compute lanes,
batch size 8192, I/O concurrency 32, and metadata cache 0.
EOF
  exit 2
}

dataset_argument=
low_memory_argument=
output_argument=
baseline_tag=v0.4.0-alpha.1
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dataset-root)
      [ "$#" -ge 2 ] || usage
      dataset_argument=$2
      shift 2
      ;;
    --low-memory-run)
      [ "$#" -ge 2 ] || usage
      low_memory_argument=$2
      shift 2
      ;;
    --output)
      [ "$#" -ge 2 ] || usage
      output_argument=$2
      shift 2
      ;;
    --baseline-tag)
      [ "$#" -ge 2 ] || usage
      baseline_tag=$2
      shift 2
      ;;
    -h|--help) usage ;;
    *) die "unknown argument: $1" ;;
  esac
done
[ -n "$dataset_argument" ] || usage
[ -n "$low_memory_argument" ] || usage

memory_limit=134217728
threads=4
batch_size=8192
io_concurrency=32
metadata_cache_bytes=0
output_argument=${output_argument:-benchmarks/results/v05/$(date -u '+%Y%m%dT%H%M%SZ')}

cd "$WORKSPACE"
candidate_commit=$(git rev-parse --verify HEAD 2>/dev/null) || die "cannot resolve candidate HEAD"
printf '%s\n' "$candidate_commit" | grep -Eq '^[0-9a-f]{40}$' || \
  die "candidate HEAD must be an exact 40-character commit"
[ -z "$(git status --porcelain --untracked-files=normal)" ] || \
  die "candidate worktree must be clean before collecting resource evidence"
baseline_commit=$(git rev-parse --verify "$baseline_tag^{commit}" 2>/dev/null) || \
  die "cannot resolve local baseline tag: $baseline_tag"
host_cpu=$(benchmark_cpu_model)
case "$host_cpu" in
  *'M5 Max'*) ;;
  *) die "the fixed-hardware resource gate requires an Apple M5 Max, got '$host_cpu'" ;;
esac

case "$dataset_argument" in
  s3://*) die "the fixed-hardware resource gate requires local SF10" ;;
esac
require_tpch_parquet_root "$dataset_argument"
dataset_relative=$(workspace_relative_path "$dataset_argument" "dataset root")
dataset_host=$(host_dataset_root "$dataset_argument")
dataset_root=$(container_dataset_root "$dataset_argument")
[ -f "$dataset_host/manifest.json" ] || \
  die "missing dataset metadata: $dataset_host/manifest.json"
[ -f "$dataset_host/manifest.sha256" ] || \
  die "missing dataset manifest: $dataset_host/manifest.sha256"
scale_factor=$(python3 -c \
  'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8")).get("scale_factor", ""))' \
  "$dataset_host/manifest.json")
[ "$scale_factor" = 10 ] || die "the fixed-hardware resource gate requires SF10"

low_memory_relative=$(workspace_relative_path "$low_memory_argument" "low-memory run")
low_memory_host=$WORKSPACE/$low_memory_relative
[ -f "$low_memory_host/manifest.json" ] || \
  die "missing low-memory manifest: $low_memory_host/manifest.json"
for query in inner-join left-join right-join full-join semi-join anti-join; do
  [ -f "$low_memory_host/reports/$query-$memory_limit.json" ] || \
    die "missing 128 MiB Join report: $low_memory_host/reports/$query-$memory_limit.json"
done

output_relative=$(workspace_relative_path "$output_argument" "output directory")
output_host=$WORKSPACE/$output_relative
output_container=/workspace/$output_relative
[ ! -e "$output_host" ] || \
  die "output directory already exists; choose a new path: $output_relative"
mkdir -p "$output_host/rendered" "$output_host/baseline" "$output_host/spill"

temporary_root=
baseline_worktree=
worktree_added=0
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  rm -f "$output_host/manifest.json.tmp" "$output_host/baseline/q21.json.tmp" \
    "$output_host/q17.checksum.txt.tmp" "$output_host/q21.checksum.txt.tmp"
  if [ "$worktree_added" = 1 ]; then
    if ! git -C "$WORKSPACE" worktree remove --force "$baseline_worktree"; then
      echo "error: failed to remove temporary baseline worktree: $baseline_worktree" >&2
      [ "$status" -ne 0 ] || status=1
    fi
  fi
  if [ -n "$temporary_root" ] && ! rmdir "$temporary_root" 2>/dev/null; then
    echo "error: temporary directory is not empty: $temporary_root" >&2
    [ "$status" -ne 0 ] || status=1
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

render_query "$SCRIPT_DIR/tpch/q17.sql" "$dataset_root" "$output_host/rendered/q17.sql"
render_query "$SCRIPT_DIR/tpch/q21.sql" "$dataset_root" "$output_host/rendered/q21.sql"
render_query "$SCRIPT_DIR/tpch/q21.sql" "$dataset_root" "$output_host/baseline/q21.sql"

build_benchmark_binary
[ "$BENCHMARK_BUILD_ID" = "$candidate_commit" ] || \
  die "benchmark helper did not record the clean candidate commit"
candidate_binary_sha256=$BENCHMARK_BINARY_SHA256
candidate_rustflags=$BENCHMARK_RUSTFLAGS
candidate_rustc=$BENCHMARK_RUSTC_VERSION
candidate_cpu=$BENCHMARK_CPU_MODEL
for query in inner-join left-join right-join full-join semi-join anti-join; do
  assert_benchmark_report_config \
    "$low_memory_host/reports/$query-$memory_limit.json" \
    "$memory_limit" "$threads" "$batch_size" "$io_concurrency" \
    "$metadata_cache_bytes" "$candidate_binary_sha256"
done

echo "v0.5 resource: candidate Q17" >&2
run_benchmark_report \
  "$output_container/rendered/q17.sql" "$output_host/q17.json" \
  "$memory_limit" 0 1 "$threads" "$batch_size" "$io_concurrency" \
  "$metadata_cache_bytes" "$output_container/spill" 1 local
assert_no_query_directories "$output_host/spill"

echo "v0.5 resource: candidate Q21" >&2
run_benchmark_report \
  "$output_container/rendered/q21.sql" "$output_host/q21.json" \
  "$memory_limit" 2 5 "$threads" "$batch_size" "$io_concurrency" \
  "$metadata_cache_bytes" "$output_container/spill" 0 local
assert_no_query_directories "$output_host/spill"

collect_tpch_checksum() (
  template=$1
  destination=$2
  skip_build=$3
  echo "v0.5 resource: DuckDB differential $(basename "$template" .sql)" >&2
  TPCH_SKIP_BUILD=$skip_build \
  TPCH_MEMORY_LIMIT_BYTES=$memory_limit \
  TPCH_THREADS=$threads \
  TPCH_BATCH_SIZE=$batch_size \
  TPCH_IO_CONCURRENCY=$io_concurrency \
  TPCH_REQUIRE_SPILL=0 \
  TPCH_RUSTFLAGS=$candidate_rustflags \
    "$WORKSPACE/tools/tpch/compare_query.sh" \
      "$template" "$dataset_relative" "$dataset_relative" \
      < /dev/null > "$destination.tmp"
  assert_sha256_file "$destination.tmp"
  mv "$destination.tmp" "$destination"
)

collect_tpch_checksum \
  "$SCRIPT_DIR/tpch/q17.sql" "$output_host/q17.checksum.txt" 0
collect_tpch_checksum \
  "$SCRIPT_DIR/tpch/q21.sql" "$output_host/q21.checksum.txt" 1

mkdir -p "$WORKSPACE/tmp"
temporary_root=$(mktemp -d "$WORKSPACE/tmp/v05-v04-resource.XXXXXX")
baseline_worktree=$temporary_root/worktree
git worktree add --quiet --detach "$baseline_worktree" "$baseline_commit"
worktree_added=1
baseline_relative=$(workspace_relative_path "$baseline_worktree" "baseline worktree")
baseline_container=/workspace/$baseline_relative
baseline_binary=$baseline_container/target/release/rustdb-bench

echo "v0.5 resource: build detached $baseline_tag" >&2
docker compose run --rm --no-deps --no-TTY \
  --env "RUSTFLAGS=$candidate_rustflags" dev \
  cargo build --locked --quiet --release --bin rustdb-bench \
  --manifest-path "$baseline_container/Cargo.toml" \
  --target-dir "$baseline_container/target"
baseline_binary_sha256=$(docker compose run --rm --no-deps --no-TTY dev \
  sha256sum "$baseline_binary" | awk '/^[0-9a-f]{64}[[:space:]]/ {print $1; exit}')
printf '%s\n' "$baseline_binary_sha256" | grep -Eq '^[0-9a-f]{64}$' || \
  die "cannot determine the baseline benchmark executable SHA-256"

echo "v0.5 resource: baseline Q21" >&2
set -- "$baseline_binary" \
  --query "$output_container/baseline/q21.sql" \
  --build-id "$baseline_commit" \
  --build-profile release \
  "--build-rustflags=$candidate_rustflags" \
  --rustc-version "$candidate_rustc" \
  --cpu-model "$candidate_cpu" \
  --warmup 2 --iterations 5 \
  --memory-limit "$memory_limit" \
  --threads "$threads" \
  --batch-size "$batch_size" \
  --io-concurrency "$io_concurrency" \
  --metadata-cache-bytes "$metadata_cache_bytes" \
  --spill-directory "$output_container/spill"
if ! docker compose run --rm --no-deps --no-TTY dev "$@" \
    < /dev/null > "$output_host/baseline/q21.json.tmp"; then
  exit 1
fi
assert_benchmark_report_config \
  "$output_host/baseline/q21.json.tmp" "$memory_limit" "$threads" \
  "$batch_size" "$io_concurrency" "$metadata_cache_bytes" "$baseline_binary_sha256"
mv "$output_host/baseline/q21.json.tmp" "$output_host/baseline/q21.json"
assert_no_query_directories "$output_host/spill"

generation_json=$(tr -d '\r\n' < "$dataset_host/manifest.json")
manifest_digest=$(sha256_file "$dataset_host/manifest.sha256")
dataset_json=$(printf '{"generation":%s,"manifest":"%s","manifest_sha256":"%s"}' \
  "$generation_json" "$(json_escape "$dataset_relative/manifest.sha256")" "$manifest_digest")
{
  printf '{\n'
  printf '  "suite":"rustdb-v05-resource-v1",\n'
  printf '  "generated_at_utc":"%s",\n' "$(utc_timestamp)"
  printf '  "dataset":%s,\n' "$dataset_json"
  printf '  "candidate":{"build_id":"%s","binary_sha256":"%s"},\n' \
    "$candidate_commit" "$candidate_binary_sha256"
  printf '  "baseline":{"tag":"%s","build_id":"%s","binary_sha256":"%s"},\n' \
    "$(json_escape "$baseline_tag")" "$baseline_commit" "$baseline_binary_sha256"
  printf '  "config":{"memory_limit_bytes":%s,"compute_threads":%s,' \
    "$memory_limit" "$threads"
  printf '"batch_size":%s,"io_concurrency":%s,"metadata_cache_bytes":%s},\n' \
    "$batch_size" "$io_concurrency" "$metadata_cache_bytes"
  printf '  "reports":{"q17":"%s/q17.json","q21_candidate":"%s/q21.json",' \
    "$output_relative" "$output_relative"
  printf '"q21_baseline":"%s/baseline/q21.json"},\n' \
    "$output_relative"
  printf '  "checksums":{"q17":"%s/q17.checksum.txt",' "$output_relative"
  printf '"q21":"%s/q21.checksum.txt"},\n' "$output_relative"
  printf '  "low_memory_manifest":"%s/manifest.json"\n' "$low_memory_relative"
  printf '}\n'
} > "$output_host/manifest.json.tmp"
mv "$output_host/manifest.json.tmp" "$output_host/manifest.json"

set -- python3 -B "$SCRIPT_DIR/check_v05_resource_gate.py" \
  --q17 "$output_host/q17.json" \
  --q17-checksum "$output_host/q17.checksum.txt" \
  --q21-candidate "$output_host/q21.json" \
  --q21-checksum "$output_host/q21.checksum.txt" \
  --q21-baseline "$output_host/baseline/q21.json" \
  --low-memory-manifest "$low_memory_host/manifest.json" \
  --baseline-tag "$baseline_tag" \
  --dataset-manifest "$dataset_host/manifest.sha256"
for query in inner-join left-join right-join full-join semi-join anti-join; do
  set -- "$@" --join "$low_memory_host/reports/$query-$memory_limit.json"
done
"$@"
echo "$output_host/manifest.json"
