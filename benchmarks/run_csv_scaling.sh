#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$SCRIPT_DIR/suites/lib.sh"

mode=release
if [ "${1:-}" = --smoke ]; then
  mode=smoke
  shift
fi

csv_argument=${1:-data/csv-scaling-10g.csv}
output_argument=${2:-benchmarks/results/csv-scaling/$(date -u '+%Y%m%dT%H%M%SZ')}

if [ "$mode" = release ]; then
  target_bytes=10737418240
  warmup=2
  iterations=5
  minimum_speedup=1.8
  memory_limit=1073741824
  [ "${TARGET_BYTES:-$target_bytes}" = "$target_bytes" ] || \
    die "release TARGET_BYTES is fixed at $target_bytes; use --smoke for custom runs"
  [ "${WARMUP:-$warmup}" = "$warmup" ] || \
    die "release WARMUP is fixed at $warmup; use --smoke for custom runs"
  [ "${ITERATIONS:-$iterations}" = "$iterations" ] || \
    die "release ITERATIONS is fixed at $iterations; use --smoke for custom runs"
  [ "${MINIMUM_SPEEDUP:-$minimum_speedup}" = "$minimum_speedup" ] || \
    die "release MINIMUM_SPEEDUP is fixed at $minimum_speedup"
  [ "${MEMORY_LIMIT_BYTES:-$memory_limit}" = "$memory_limit" ] || \
    die "release MEMORY_LIMIT_BYTES is fixed at $memory_limit; use --smoke for custom runs"
  [ "${RUSTDB_BENCH_RUSTFLAGS:--C target-cpu=native}" = "-C target-cpu=native" ] || \
    die "release RUSTDB_BENCH_RUSTFLAGS must be '-C target-cpu=native'"
  RUSTDB_BENCH_RUSTFLAGS='-C target-cpu=native'
  export RUSTDB_BENCH_RUSTFLAGS

  build_id=$(benchmark_build_id)
  printf '%s\n' "$build_id" | grep -Eq '^[0-9a-f]{40}$' || \
    die "release CSV gate requires a clean exact 40-character candidate commit"
  cpu_model=$(benchmark_cpu_model)
  case "$cpu_model" in
    *'M5 Max'*) ;;
    *) die "release CSV gate requires an Apple M5 Max, detected '$cpu_model'" ;;
  esac
else
  target_bytes=${TARGET_BYTES:-67108864}
  warmup=${WARMUP:-0}
  iterations=${ITERATIONS:-1}
  minimum_speedup=${MINIMUM_SPEEDUP:-1.8}
  memory_limit=${MEMORY_LIMIT_BYTES:-1073741824}
fi

require_positive_integer "$target_bytes" TARGET_BYTES
require_nonnegative_integer "$warmup" WARMUP
require_positive_integer "$iterations" ITERATIONS
require_positive_integer "$memory_limit" MEMORY_LIMIT_BYTES

cd "$WORKSPACE"
csv_relative=$(workspace_relative_path "$csv_argument" "CSV fixture")
output_relative=$(workspace_relative_path "$output_argument" "output directory")
csv_host=$WORKSPACE/$csv_relative
output_host=$WORKSPACE/$output_relative
[ ! -e "$output_host" ] || die "output directory already exists: $output_relative"
mkdir -p "$(dirname "$csv_host")" "$output_host/spill"

if [ ! -f "$csv_host" ] || [ "$(wc -c < "$csv_host" | tr -d '[:space:]')" -lt "$target_bytes" ]; then
  python3 - "$csv_host" "$target_bytes" <<'PY'
import os
import sys

path, requested = sys.argv[1], int(sys.argv[2])
line = b"1," + (b"x" * 61) + b"\n"
chunk = line * (8 * 1024 * 1024 // len(line))
with open(path, "wb") as handle:
    handle.write(b"id,payload\n")
    while handle.tell() < requested:
        remaining = requested - handle.tell()
        if remaining >= len(chunk):
            handle.write(chunk)
        else:
            rows = max(1, (remaining + len(line) - 1) // len(line))
            handle.write(line * rows)
PY
fi

actual_bytes=$(wc -c < "$csv_host" | tr -d '[:space:]')
[ "$actual_bytes" -ge "$target_bytes" ] || die "CSV fixture is smaller than TARGET_BYTES"
header_bytes=11
record_bytes=64
[ "$actual_bytes" -gt "$header_bytes" ] || die "CSV fixture contains no data records"
data_bytes=$((actual_bytes - header_bytes))
[ $((data_bytes % record_bytes)) -eq 0 ] || \
  die "CSV fixture does not match the deterministic 64-byte record format"
if [ "$mode" = release ] && [ "$actual_bytes" -ge $((target_bytes + record_bytes)) ]; then
  die "release CSV fixture must be the fixed 10 GiB fixture, got $actual_bytes bytes"
fi
query_host=$output_host/query.sql
printf "SELECT * FROM read_csv('/workspace/%s', header = 'true');\n" "$csv_relative" > "$query_host"

build_benchmark_binary
for threads in 1 4; do
  report=$output_host/threads-$threads.json
  run_benchmark_report \
    "/workspace/$output_relative/query.sql" "$report" "$memory_limit" \
    "$warmup" "$iterations" "$threads" 8192 32 0 \
    "/workspace/$output_relative/spill" 0 local
  assert_no_query_directories "$output_host/spill"
done

set -- \
  --one "$output_host/threads-1.json" \
  --four "$output_host/threads-4.json" \
  --source-file "$csv_host" \
  --mode "$mode"
if [ "$mode" = smoke ]; then
  set -- "$@" --minimum-speedup "$minimum_speedup"
fi
python3 -B "$SCRIPT_DIR/csv_scaling_gate.py" "$@" > "$output_host/summary.json"

echo "CSV scaling evidence mode: $mode" >&2
echo "$output_host/summary.json"
