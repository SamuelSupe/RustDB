#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$SCRIPT_DIR/suites/lib.sh"

usage() {
  cat >&2 <<'EOF'
usage: benchmarks/run_low_memory.sh TPCH_PARQUET_ROOT [OUTPUT_DIR]

Runs Sort, Aggregate, Inner Join, and Left Join at 64 MiB and 128 MiB.
TPCH_PARQUET_ROOT must be workspace-relative, below /workspace, or an s3:// URI.

Environment overrides:
  MEMORY_LIMITS_BYTES  default: "67108864 134217728"
  WARMUP                default: 0
  ITERATIONS            default: 1
  THREADS               default: 4
  BATCH_SIZE            default: 8192
  IO_CONCURRENCY        default: 32
  RUSTDB_BENCH_RUSTFLAGS default: "-C target-cpu=native"
  CHECKSUM_RUNNER       default: tools/tpch/compare_query.sh
  SKIP_CHECKSUM         default: 0; set to 1 only for runner diagnostics
  REFERENCE_DATASET_ROOT required local equivalent when TPCH_PARQUET_ROOT is S3
  MINIO_ENDPOINT        default: http://minio:9000 for s3:// input
  MINIO_REGION          default: us-east-1
EOF
  exit 2
}

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || usage
dataset_argument=$1
output_argument=${2:-benchmarks/results/low-memory/$(date -u '+%Y%m%dT%H%M%SZ')}

memory_limits=${MEMORY_LIMITS_BYTES:-67108864 134217728}
warmup=${WARMUP:-0}
iterations=${ITERATIONS:-1}
threads=${THREADS:-4}
batch_size=${BATCH_SIZE:-8192}
io_concurrency=${IO_CONCURRENCY:-32}
checksum_runner=${CHECKSUM_RUNNER:-$WORKSPACE/tools/tpch/compare_query.sh}
skip_checksum=${SKIP_CHECKSUM:-0}
reference_argument=${REFERENCE_DATASET_ROOT:-}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_REGION=${MINIO_REGION:-us-east-1}

require_nonnegative_integer "$warmup" WARMUP
require_positive_integer "$iterations" ITERATIONS
require_positive_integer "$threads" THREADS
require_positive_integer "$batch_size" BATCH_SIZE
require_positive_integer "$io_concurrency" IO_CONCURRENCY
case "$skip_checksum" in 0|1) ;; *) die "SKIP_CHECKSUM must be 0 or 1" ;; esac
for memory_limit in $memory_limits; do
  require_positive_integer "$memory_limit" MEMORY_LIMITS_BYTES
done

cd "$WORKSPACE"
require_tpch_parquet_root "$dataset_argument"
dataset_root=$(container_dataset_root "$dataset_argument")
validate_template_root "$dataset_root"
output_relative=$(workspace_relative_path "$output_argument" "output directory")
output_host=$WORKSPACE/$output_relative
output_container=/workspace/$output_relative
rendered_host=$output_host/rendered
reports_host=$output_host/reports
spill_host=$output_host/spill
entries_file=$output_host/.manifest-entries
manifest_file=$output_host/manifest.json
[ ! -e "$output_host" ] || \
  die "output directory already exists; choose a new path or remove it: $output_relative"
mkdir -p "$rendered_host" "$reports_host" "$spill_host"
trap 'rm -f "$entries_file" "$manifest_file.tmp"' EXIT HUP INT TERM

case "$dataset_root" in
  s3://*) target_kind=minio ;;
  *) target_kind=local ;;
esac
if [ "$skip_checksum" = 0 ]; then
  [ -x "$checksum_runner" ] || die "CHECKSUM_RUNNER is not executable: $checksum_runner"
  if [ "$target_kind" = local ]; then
    reference_argument=$dataset_argument
    checksum_target=$(workspace_relative_path "$dataset_argument" "dataset root")
  else
    [ -n "$reference_argument" ] || \
      die "REFERENCE_DATASET_ROOT is required for checksum validation of S3 data"
    checksum_target=$dataset_argument
  fi
  require_tpch_parquet_root "$reference_argument"
  reference_relative=$(workspace_relative_path "$reference_argument" "reference dataset root")
  validate_template_root "$reference_relative"
else
  reference_relative=
fi

metadata_argument=$dataset_argument
if [ "$target_kind" = minio ]; then
  metadata_argument=$reference_argument
fi
dataset_json=null
if [ -n "$metadata_argument" ]; then
  metadata_host=$(host_dataset_root "$metadata_argument")
  [ -f "$metadata_host/manifest.json" ] || die "missing dataset metadata: $metadata_host/manifest.json"
  [ -f "$metadata_host/manifest.sha256" ] || die "missing dataset manifest: $metadata_host/manifest.sha256"
  metadata_relative=$(workspace_relative_path "$metadata_argument" "metadata dataset root")
  generation_json=$(tr -d '\r\n' < "$metadata_host/manifest.json")
  manifest_digest=$(sha256_file "$metadata_host/manifest.sha256")
  dataset_json=$(printf '{"generation":%s,"manifest":"%s","manifest_sha256":"%s"}' \
    "$generation_json" "$(json_escape "$metadata_relative/manifest.sha256")" "$manifest_digest")
fi

build_benchmark_binary
first_entry=1
while IFS='|' read -r case_name query_filename; do
  case "$case_name" in ''|'#'*) continue ;; esac
  template=$SCRIPT_DIR/suites/low-memory/$query_filename
  rendered_relative=$output_relative/rendered/$case_name.sql
  rendered_host=$WORKSPACE/$rendered_relative
  rendered_container=/workspace/$rendered_relative
  render_query "$template" "$dataset_root" "$rendered_host"

  for memory_limit in $memory_limits; do
    checksum_relative=
    if [ "$skip_checksum" = 0 ]; then
      checksum_relative=$output_relative/reports/$case_name-$memory_limit.checksum.txt
      echo "checksum: case=$case_name memory_limit=$memory_limit" >&2
      TPCH_S3_ENDPOINT=$MINIO_ENDPOINT \
      TPCH_S3_REGION=$MINIO_REGION \
      TPCH_S3_PATH_STYLE=1 \
      TPCH_MEMORY_LIMIT_BYTES=$memory_limit \
      TPCH_THREADS=$threads \
      TPCH_BATCH_SIZE=$batch_size \
      TPCH_IO_CONCURRENCY=$io_concurrency \
      TPCH_REQUIRE_SPILL=1 \
      TPCH_RUSTFLAGS=$BENCHMARK_RUSTFLAGS \
        "$checksum_runner" "$template" "$reference_relative" "$checksum_target" \
        < /dev/null > "$WORKSPACE/$checksum_relative"
      assert_sha256_file "$WORKSPACE/$checksum_relative"
    fi

    report_name=$case_name-$memory_limit.json
    report_relative=$output_relative/reports/$report_name
    report_host=$WORKSPACE/$report_relative
    echo "low-memory: case=$case_name memory_limit=$memory_limit" >&2
    run_benchmark_report \
      "$rendered_container" "$report_host" "$memory_limit" \
      "$warmup" "$iterations" "$threads" "$batch_size" \
      "$io_concurrency" 0 "$output_container/spill" 1 "$target_kind"
    assert_no_query_directories "$spill_host"

    [ "$first_entry" = 1 ] || printf ',\n' >> "$entries_file"
    first_entry=0
    if [ -n "$checksum_relative" ]; then
      checksum_json="\"$(json_escape "$checksum_relative")\""
    else
      checksum_json=null
    fi
    printf '    {"case":"%s","memory_limit_bytes":%s,"require_spill":true,"report":"%s","checksum_report":%s}' \
      "$(json_escape "$case_name")" "$memory_limit" \
      "$(json_escape "$report_relative")" "$checksum_json" >> "$entries_file"
  done
done < "$SCRIPT_DIR/suites/low-memory/cases.tsv"

{
  printf '{\n'
  printf '  "suite":"rustdb-low-memory-v1",\n'
  printf '  "generated_at_utc":"%s",\n' "$(utc_timestamp)"
  printf '  "dataset_root":"%s",\n' "$(json_escape "$dataset_root")"
  printf '  "rustdb_build_id":"%s",\n' "$(json_escape "$BENCHMARK_BUILD_ID")"
  printf '  "benchmark_binary_sha256":"%s",\n' "$BENCHMARK_BINARY_SHA256"
  printf '  "build":{"cargo_profile":"%s","rustflags":"%s","rustc_version":"%s"},\n' \
    "$(json_escape "$BENCHMARK_BUILD_PROFILE")" "$(json_escape "$BENCHMARK_RUSTFLAGS")" \
    "$(json_escape "$BENCHMARK_RUSTC_VERSION")"
  printf '  "dataset":%s,\n' "$dataset_json"
  if [ "$skip_checksum" = 0 ]; then
    printf '  "correctness":{"verified":true,"scope":"each memory-limited measured dataset query","memory_limited":true,"spill_required":true,"target":"%s","runner":"%s","reference_root":"/workspace/%s"},\n' \
      "$target_kind" "$(json_escape "$checksum_runner")" "$(json_escape "$reference_relative")"
  else
    printf '  "correctness":{"verified":false,"reason":"SKIP_CHECKSUM=1"},\n'
  fi
  printf '  "config":{"threads":%s,"batch_size":%s,"io_concurrency":%s,"warmup":%s,"iterations":%s},\n' \
    "$threads" "$batch_size" "$io_concurrency" "$warmup" "$iterations"
  printf '  "assertions":{"full_result_consumed":true,"peak_memory_within_limit":true,"spill_required":true,"spill_directories_cleaned":true},\n'
  printf '  "runs":[\n'
  cat "$entries_file"
  printf '\n  ]\n}\n'
} > "$manifest_file.tmp"
mv "$manifest_file.tmp" "$manifest_file"
rm -f "$entries_file"
trap - EXIT HUP INT TERM
echo "$manifest_file"
