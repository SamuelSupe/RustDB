#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$SCRIPT_DIR/suites/lib.sh"

usage() {
  cat >&2 <<'EOF'
usage: benchmarks/run_baseline.sh --local-root ROOT [options]
       benchmarks/run_baseline.sh --smoke [--output DIR]

Options:
  --local-root ROOT   workspace-local TPC-H Parquet root
  --minio-root URI    matching s3:// TPC-H root already uploaded to MinIO
  --output DIR        workspace-local output directory
  --smoke             use tests/fixtures/employees.csv for a quick runner check

Environment matrix:
  THREADS_LIST         default: "1 4"
  BATCH_SIZES          default: "4096 8192"
  CACHE_MODES          default: "cold warm" (cold means metadata-cache cold)
  MEMORY_LIMIT_BYTES   default: 1073741824
  COLD_ITERATIONS      default: 1
  WARMUP               default: 2
  ITERATIONS           default: 5
  IO_CONCURRENCY       default: 32
  METADATA_CACHE_BYTES default: 67108864
  RUSTDB_BENCH_RUSTFLAGS default: "-C target-cpu=native"
  MINIO_ENDPOINT       default: http://minio:9000
  MINIO_REGION         default: us-east-1
  START_MINIO          default: 1 when --minio-root is present
  CHECKSUM_RUNNER      default: tools/tpch/compare_query.sh
  MINIO_MANIFEST_RUNNER default: tools/tpch/verify_minio.sh
  SKIP_CHECKSUM        default: 0; set to 1 only for runner diagnostics
EOF
  exit 2
}

local_argument=
minio_argument=
output_argument=
smoke=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --local-root)
      [ "$#" -ge 2 ] || usage
      local_argument=$2
      shift 2
      ;;
    --minio-root)
      [ "$#" -ge 2 ] || usage
      minio_argument=$2
      shift 2
      ;;
    --output)
      [ "$#" -ge 2 ] || usage
      output_argument=$2
      shift 2
      ;;
    --smoke)
      smoke=1
      shift
      ;;
    -h|--help) usage ;;
    *) die "unknown argument: $1" ;;
  esac
done

if [ "$smoke" = 1 ]; then
  [ -z "$local_argument" ] || die "--smoke cannot be combined with --local-root"
  [ -z "$minio_argument" ] || die "--smoke cannot be combined with --minio-root"
  local_argument=tests/fixtures
  suite_name=smoke
else
  [ -n "$local_argument" ] || usage
  suite_name=baseline
fi
output_argument=${output_argument:-benchmarks/results/$suite_name/$(date -u '+%Y%m%dT%H%M%SZ')}

threads_list=${THREADS_LIST:-1 4}
batch_sizes=${BATCH_SIZES:-4096 8192}
cache_modes=${CACHE_MODES:-cold warm}
memory_limit=${MEMORY_LIMIT_BYTES:-1073741824}
cold_iterations=${COLD_ITERATIONS:-1}
warmup_iterations=${WARMUP:-2}
measured_iterations=${ITERATIONS:-5}
io_concurrency=${IO_CONCURRENCY:-32}
metadata_cache_bytes=${METADATA_CACHE_BYTES:-67108864}
MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_REGION=${MINIO_REGION:-us-east-1}
start_minio=${START_MINIO:-1}
checksum_runner=${CHECKSUM_RUNNER:-$WORKSPACE/tools/tpch/compare_query.sh}
minio_manifest_runner=${MINIO_MANIFEST_RUNNER:-$WORKSPACE/tools/tpch/verify_minio.sh}
skip_checksum=${SKIP_CHECKSUM:-0}

require_positive_integer "$memory_limit" MEMORY_LIMIT_BYTES
require_positive_integer "$cold_iterations" COLD_ITERATIONS
require_nonnegative_integer "$warmup_iterations" WARMUP
require_positive_integer "$measured_iterations" ITERATIONS
require_positive_integer "$io_concurrency" IO_CONCURRENCY
require_nonnegative_integer "$metadata_cache_bytes" METADATA_CACHE_BYTES
for threads in $threads_list; do require_positive_integer "$threads" THREADS_LIST; done
for batch_size in $batch_sizes; do require_positive_integer "$batch_size" BATCH_SIZES; done
for cache_mode in $cache_modes; do
  case "$cache_mode" in cold|warm) ;; *) die "CACHE_MODES accepts only 'cold' and 'warm'" ;; esac
done
case "$start_minio" in 0|1) ;; *) die "START_MINIO must be 0 or 1" ;; esac
case "$skip_checksum" in 0|1) ;; *) die "SKIP_CHECKSUM must be 0 or 1" ;; esac

cd "$WORKSPACE"
if [ "$suite_name" = baseline ]; then
  require_tpch_parquet_root "$local_argument"
fi
local_root=$(container_dataset_root "$local_argument")
local_relative=$(workspace_relative_path "$local_argument" "local dataset root")
validate_template_root "$local_root"
dataset_json=null
if [ "$suite_name" = baseline ]; then
  dataset_host=$(host_dataset_root "$local_argument")
  [ -f "$dataset_host/manifest.json" ] || die "missing dataset metadata: $dataset_host/manifest.json"
  [ -f "$dataset_host/manifest.sha256" ] || die "missing dataset manifest: $dataset_host/manifest.sha256"
  generation_json=$(tr -d '\r\n' < "$dataset_host/manifest.json")
  manifest_digest=$(sha256_file "$dataset_host/manifest.sha256")
  dataset_json=$(printf '{"generation":%s,"manifest":"%s","manifest_sha256":"%s"}' \
    "$generation_json" "$(json_escape "$local_relative/manifest.sha256")" "$manifest_digest")
fi
if [ -n "$minio_argument" ]; then
  case "$minio_argument" in s3://*) ;; *) die "--minio-root must be an s3:// URI" ;; esac
  validate_template_root "$minio_argument"
fi

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

remote_manifest_verified=false
if [ -n "$minio_argument" ] && [ "$start_minio" = 1 ]; then
  docker compose up -d --wait minio
  docker compose run --rm --no-deps minio-init
  if [ "$suite_name" = baseline ]; then
    [ -x "$minio_manifest_runner" ] || \
      die "MINIO_MANIFEST_RUNNER is not executable: $minio_manifest_runner"
    "$minio_manifest_runner" "$local_relative" "$minio_argument"
    remote_manifest_verified=true
  fi
fi
if [ "$skip_checksum" = 0 ]; then
  [ -x "$checksum_runner" ] || die "CHECKSUM_RUNNER is not executable: $checksum_runner"
fi
build_benchmark_binary

first_entry=1
run_target() {
  target_kind=$1
  target_root=$2
  checksum_root=$3
  while IFS='|' read -r case_name query_filename; do
    case "$case_name" in ''|'#'*) continue ;; esac
    template=$SCRIPT_DIR/suites/$suite_name/$query_filename
    rendered_relative=$output_relative/rendered/$target_kind-$case_name.sql
    rendered_host=$WORKSPACE/$rendered_relative
    rendered_container=/workspace/$rendered_relative
    render_query "$template" "$target_root" "$rendered_host"

    checksum_relative=
    if [ "$skip_checksum" = 0 ]; then
      checksum_relative=$output_relative/reports/$target_kind-$case_name.checksum.txt
      echo "checksum: target=$target_kind case=$case_name" >&2
      TPCH_S3_ENDPOINT=$MINIO_ENDPOINT \
      TPCH_S3_REGION=$MINIO_REGION \
      TPCH_S3_PATH_STYLE=1 \
      TPCH_RUSTFLAGS=$BENCHMARK_RUSTFLAGS \
        "$checksum_runner" "$template" "$local_relative" "$checksum_root" \
        < /dev/null > "$WORKSPACE/$checksum_relative"
      assert_sha256_file "$WORKSPACE/$checksum_relative"
    fi

    for cache_mode in $cache_modes; do
      if [ "$cache_mode" = cold ]; then
        run_warmup=0
        run_iterations=$cold_iterations
        run_cache_bytes=0
      else
        run_warmup=$warmup_iterations
        run_iterations=$measured_iterations
        run_cache_bytes=$metadata_cache_bytes
      fi
      for threads in $threads_list; do
        for batch_size in $batch_sizes; do
          report_name=$target_kind-$cache_mode-$case_name-t$threads-b$batch_size.json
          report_relative=$output_relative/reports/$report_name
          report_host=$WORKSPACE/$report_relative
          echo "baseline: target=$target_kind cache=$cache_mode case=$case_name threads=$threads batch=$batch_size" >&2
          run_benchmark_report \
            "$rendered_container" "$report_host" "$memory_limit" \
            "$run_warmup" "$run_iterations" "$threads" "$batch_size" \
            "$io_concurrency" "$run_cache_bytes" "$output_container/spill" \
            0 "$target_kind"
          assert_no_query_directories "$spill_host"

          [ "$first_entry" = 1 ] || printf ',\n' >> "$entries_file"
          first_entry=0
          if [ -n "$checksum_relative" ]; then
            checksum_json="\"$(json_escape "$checksum_relative")\""
          else
            checksum_json=null
          fi
          printf '    {"target":"%s","cache_mode":"metadata-%s","case":"%s","threads":%s,"batch_size":%s,"warmup":%s,"iterations":%s,"report":"%s","checksum_report":%s}' \
            "$target_kind" "$cache_mode" "$(json_escape "$case_name")" \
            "$threads" "$batch_size" "$run_warmup" "$run_iterations" \
            "$(json_escape "$report_relative")" "$checksum_json" >> "$entries_file"
        done
      done
    done
  done < "$SCRIPT_DIR/suites/$suite_name/cases.tsv"
}

run_target local "$local_root" "$local_relative"
if [ -n "$minio_argument" ]; then
  run_target minio "$minio_argument" "$minio_argument"
fi

{
  printf '{\n'
  printf '  "suite":"rustdb-%s-v1",\n' "$(json_escape "$suite_name")"
  printf '  "generated_at_utc":"%s",\n' "$(utc_timestamp)"
  printf '  "local_root":"%s",\n' "$(json_escape "$local_root")"
  if [ -n "$minio_argument" ]; then
    printf '  "minio_root":"%s",\n' "$(json_escape "$minio_argument")"
  else
    printf '  "minio_root":null,\n'
  fi
  printf '  "memory_limit_bytes":%s,\n' "$memory_limit"
  printf '  "rustdb_build_id":"%s",\n' "$(json_escape "${RUSTDB_BUILD_ID:-$(benchmark_build_id)}")"
  printf '  "build":{"cargo_profile":"%s","rustflags":"%s","rustc_version":"%s"},\n' \
    "$(json_escape "$BENCHMARK_BUILD_PROFILE")" "$(json_escape "$BENCHMARK_RUSTFLAGS")" \
    "$(json_escape "$BENCHMARK_RUSTC_VERSION")"
  printf '  "dataset":%s,\n' "$dataset_json"
  if [ -n "$minio_argument" ]; then
    case "$MINIO_ENDPOINT" in http://*) allow_http=true ;; *) allow_http=false ;; esac
    printf '  "s3_config":{"endpoint":"%s","region":"%s","path_style":true,"allow_http":%s},\n' \
      "$(json_escape "$MINIO_ENDPOINT")" "$(json_escape "$MINIO_REGION")" "$allow_http"
  else
    printf '  "s3_config":null,\n'
  fi
  if [ "$skip_checksum" = 0 ]; then
    if [ -n "$minio_argument" ]; then correctness_targets='["local","minio"]'; else correctness_targets='["local"]'; fi
    printf '  "correctness":{"verified":true,"scope":"every benchmark target","targets":%s,"runner":"%s","reference_root":"%s","remote_manifest_verified":%s},\n' \
      "$correctness_targets" "$(json_escape "$checksum_runner")" \
      "$(json_escape "$local_root")" "$remote_manifest_verified"
  else
    printf '  "correctness":{"verified":false,"reason":"SKIP_CHECKSUM=1"},\n'
  fi
  printf '  "cache_definition":{"metadata-cold":"new engine, zero metadata cache, no warmup","metadata-warm":"same engine/session warmups before measured runs","os_page_cache_flushed":false},\n'
  printf '  "runs":[\n'
  cat "$entries_file"
  printf '\n  ]\n}\n'
} > "$manifest_file.tmp"
mv "$manifest_file.tmp" "$manifest_file"
rm -f "$entries_file"
trap - EXIT HUP INT TERM
echo "$manifest_file"
