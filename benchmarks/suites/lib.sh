#!/bin/sh

# Shared, POSIX-shell helpers for the benchmark suite entrypoints.

die() {
  echo "error: $*" >&2
  exit 2
}

require_positive_integer() {
  value=$1
  label=$2
  case "$value" in
    ''|*[!0-9]*) die "$label must be a positive integer, got '$value'" ;;
  esac
  [ "$value" -gt 0 ] || die "$label must be greater than zero"
}

require_nonnegative_integer() {
  value=$1
  label=$2
  case "$value" in
    ''|*[!0-9]*) die "$label must be a non-negative integer, got '$value'" ;;
  esac
}

reject_unsafe_relative_path() {
  path=$1
  label=$2
  case "/$path/" in
    *'/../'*|*'/./'*) die "$label must not contain '.' or '..' path components" ;;
  esac
}

workspace_relative_path() {
  path=$1
  label=$2
  case "$path" in
    /workspace/*)
      relative=${path#/workspace/}
      ;;
    "$WORKSPACE"/*)
      relative=${path#"$WORKSPACE"/}
      ;;
    /*)
      die "$label must be inside $WORKSPACE so the dev container can access it"
      ;;
    *)
      relative=$path
      ;;
  esac
  [ -n "$relative" ] || die "$label must not be the workspace root"
  reject_unsafe_relative_path "$relative" "$label"
  printf '%s\n' "$relative"
}

container_dataset_root() {
  root=$1
  case "$root" in
    s3://*) printf '%s\n' "$root" ;;
    *)
      relative=$(workspace_relative_path "$root" "dataset root")
      printf '/workspace/%s\n' "$relative"
      ;;
  esac
}

host_dataset_root() {
  root=$1
  case "$root" in
    s3://*) die "an S3 dataset has no host filesystem path" ;;
    *)
      relative=$(workspace_relative_path "$root" "dataset root")
      printf '%s/%s\n' "$WORKSPACE" "$relative"
      ;;
  esac
}

require_tpch_parquet_root() {
  root=$1
  case "$root" in
    s3://*) return ;;
  esac
  host_root=$(host_dataset_root "$root")
  for table in customer orders lineitem; do
    table_dir=$host_root/$table
    [ -d "$table_dir" ] || die "missing TPC-H table directory: $table_dir"
    if ! find "$table_dir" -type f -name '*.parquet' -print -quit | grep -q .; then
      die "no Parquet files found below $table_dir"
    fi
  done
}

validate_template_root() {
  dataset_root=$1
  case "$dataset_root" in
    *'|'*|*'&'*|*'\'*|*"'"*)
      die "dataset root contains a character unsupported by SQL template rendering"
      ;;
  esac
}

render_query() {
  template=$1
  dataset_root=$2
  destination=$3
  [ -f "$template" ] || die "query template not found: $template"
  validate_template_root "$dataset_root"
  sed "s|__TPCH_ROOT__|$dataset_root|g" "$template" > "$destination"
}

json_escape() {
  value=$1
  printf '%s' "$value" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

assert_sha256_file() {
  checksum_file=$1
  [ -s "$checksum_file" ] || die "checksum runner produced no output: $checksum_file"
  checksum_lines=$(wc -l < "$checksum_file" | tr -d '[:space:]')
  [ "$checksum_lines" = 1 ] || \
    die "checksum runner must emit exactly one line: $checksum_file"
  grep -Eq '^[0-9a-f]{64}$' "$checksum_file" || \
    die "checksum runner did not emit a lowercase SHA-256: $checksum_file"
}

utc_timestamp() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

sha256_file() {
  file=$1
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    die "sha256sum or shasum is required"
  fi
}

build_benchmark_binary() {
  BENCHMARK_BUILD_PROFILE=release
  BENCHMARK_RUSTFLAGS=${RUSTDB_BENCH_RUSTFLAGS:--C target-cpu=native}
  BENCHMARK_RUSTC_VERSION=$(docker compose run --rm --no-deps --no-TTY dev \
    rustc --version)
  docker compose run --rm --no-deps \
    --env "RUSTFLAGS=$BENCHMARK_RUSTFLAGS" dev \
    cargo build --quiet --release --bin rustdb-bench
}

benchmark_build_id() {
  commit=$(git -C "$WORKSPACE" rev-parse --verify HEAD 2>/dev/null || printf unknown)
  if [ -n "$(git -C "$WORKSPACE" status --porcelain --untracked-files=normal 2>/dev/null)" ]; then
    printf '%s-dirty\n' "$commit"
  else
    printf '%s\n' "$commit"
  fi
}

benchmark_cpu_model() {
  if command -v sysctl >/dev/null 2>&1; then
    model=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)
  else
    model=
  fi
  if [ -z "$model" ] && command -v lscpu >/dev/null 2>&1; then
    model=$(lscpu 2>/dev/null | awk -F: '/^Model name:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}')
  fi
  if [ -z "$model" ] && [ -r /proc/cpuinfo ]; then
    model=$(awk -F: '/^(model name|Hardware)[[:space:]]*:/ {sub(/^[[:space:]]+/, "", $2); print $2; exit}' /proc/cpuinfo)
  fi
  printf '%s\n' "${model:-unknown}"
}

run_benchmark_report() {
  query=$1
  report=$2
  memory_limit=$3
  warmup=$4
  iterations=$5
  threads=$6
  batch_size=$7
  io_concurrency=$8
  metadata_cache_bytes=$9
  shift 9
  temp_dir=$1
  require_spill=$2
  target_kind=$3

  set -- target/release/rustdb-bench \
    --query "$query" \
    --build-id "${RUSTDB_BUILD_ID:-$(benchmark_build_id)}" \
    --build-profile "$BENCHMARK_BUILD_PROFILE" \
    "--build-rustflags=$BENCHMARK_RUSTFLAGS" \
    --rustc-version "$BENCHMARK_RUSTC_VERSION" \
    --cpu-model "${RUSTDB_CPU_MODEL:-$(benchmark_cpu_model)}" \
    --warmup "$warmup" \
    --iterations "$iterations" \
    --memory-limit "$memory_limit" \
    --threads "$threads" \
    --batch-size "$batch_size" \
    --io-concurrency "$io_concurrency" \
    --metadata-cache-bytes "$metadata_cache_bytes" \
    --temp-dir "$temp_dir"
  if [ "$require_spill" = 1 ]; then
    set -- "$@" --require-spill
  fi
  if [ "$target_kind" = minio ]; then
    set -- "$@" \
      --s3-endpoint "$MINIO_ENDPOINT" \
      --s3-region "$MINIO_REGION" \
      --s3-path-style
    case "$MINIO_ENDPOINT" in
      http://*) set -- "$@" --s3-allow-http ;;
    esac
  fi

  temporary_report=$report.tmp
  rm -f "$temporary_report"
  if ! docker compose run --rm --no-deps --no-TTY dev "$@" \
      < /dev/null > "$temporary_report"; then
    rm -f "$temporary_report"
    return 1
  fi
  mv "$temporary_report" "$report"
}

assert_no_query_directories() {
  spill_root=$1
  if find "$spill_root" -mindepth 1 -maxdepth 1 -type d -name 'query-*' \
      -print -quit | grep -q .; then
    find "$spill_root" -mindepth 1 -maxdepth 1 -type d -name 'query-*' -print >&2
    die "query spill directories remain below $spill_root"
  fi
}
