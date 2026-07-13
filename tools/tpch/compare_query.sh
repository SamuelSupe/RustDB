#!/bin/sh

set -eu

. "$(dirname -- "$0")/common.sh"

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
  tpch_die "usage: $0 QUERY_TEMPLATE REFERENCE_DATASET_ROOT [RUSTDB_DATASET_ROOT]"
fi

query_argument=$1
reference_relative=$2
rustdb_argument=${3:-$reference_relative}
case "$query_argument" in
  /*) query_template=$query_argument ;;
  *) query_template="$TPCH_ROOT/$query_argument" ;;
esac
case "$query_template" in
  "$TPCH_ROOT"/*) ;;
  *) tpch_die "query template must be inside the RustDB workspace" ;;
esac
case "$reference_relative" in
  /*|../*|*/../*|*/..) tpch_die "dataset root must be workspace-relative" ;;
esac
case "$rustdb_argument" in
  s3://*) rustdb_root=$rustdb_argument ;;
  /*|../*|*/../*|*/..) tpch_die "RustDB dataset root must be workspace-relative or s3://" ;;
  *) rustdb_root=/workspace/$rustdb_argument ;;
esac
case "$rustdb_root" in
  *'|'*|*'&'*|*'\'*|*"'"*) tpch_die "RustDB dataset root contains an unsupported character" ;;
esac

[ -f "$query_template" ] || tpch_die "missing query template: $query_argument"
duckdb_template=$query_template
duckdb_companion=${query_template%.sql}.duckdb.sql
if [ -f "$duckdb_companion" ]; then
  duckdb_template=$duckdb_companion
fi
[ -d "$TPCH_ROOT/$reference_relative" ] || tpch_die "missing dataset: $reference_relative"
case "$rustdb_argument" in
  s3://*) ;;
  *) [ -d "$TPCH_ROOT/$rustdb_argument" ] || tpch_die "missing RustDB dataset: $rustdb_argument" ;;
esac
tpch_require docker
tpch_require python3
tpch_require sed

if [ "${TPCH_SKIP_BUILD:-0}" != 1 ]; then
  tpch_build_reference
  tpch_build_rustdb
fi

mkdir -p "$TPCH_ROOT/data"
work=$(mktemp -d "$TPCH_ROOT/data/.tpch-query.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
work_relative=${work#"$TPCH_ROOT/"}
rustdb_query="$work/rustdb.sql"
duckdb_query="$work/duckdb.sql"
rustdb_csv="$work/rustdb.csv"
duckdb_csv="$work/duckdb.csv"
rustdb_canonical="$work/rustdb.canonical.jsonl"
duckdb_canonical="$work/duckdb.canonical.jsonl"
rustdb_stderr="$work/rustdb.stderr"
spill_root="$work/spill"
mkdir -p "$spill_root"

sed "s|__TPCH_ROOT__|$rustdb_root|g" "$query_template" > "$rustdb_query"
sed "s|__TPCH_ROOT__|/workspace/$reference_relative|g" "$duckdb_template" > "$duckdb_query"

set -- docker compose --project-directory "$TPCH_ROOT" run --rm --no-deps --no-TTY dev \
  /workspace/target/release/rustdb --format csv --csv-null __RUSTDB_NULL__
case "$rustdb_argument" in
  s3://*)
    s3_endpoint=${TPCH_S3_ENDPOINT-http://minio:9000}
    s3_region=${TPCH_S3_REGION:-us-east-1}
    case "${TPCH_S3_PATH_STYLE:-1}" in
      1) set -- "$@" --s3-path-style ;;
      0) ;;
      *) tpch_die "TPCH_S3_PATH_STYLE must be 0 or 1" ;;
    esac
    set -- "$@" --s3-region "$s3_region"
    if [ -n "$s3_endpoint" ]; then
      set -- "$@" --s3-endpoint "$s3_endpoint"
      case "$s3_endpoint" in http://*) set -- "$@" --s3-allow-http ;; esac
    fi
    ;;
esac
require_spill=${TPCH_REQUIRE_SPILL:-0}
case "$require_spill" in 0|1) ;; *) tpch_die "TPCH_REQUIRE_SPILL must be 0 or 1" ;; esac
validate_optional_positive_integer() {
  value=$1
  setting=$2
  case "$value" in
    ''|*[!0-9]*) [ -z "$value" ] || tpch_die "$setting must be a positive integer" ;;
    0) tpch_die "$setting must be greater than zero" ;;
  esac
}
validate_optional_positive_integer "${TPCH_MEMORY_LIMIT_BYTES:-}" TPCH_MEMORY_LIMIT_BYTES
validate_optional_positive_integer "${TPCH_THREADS:-}" TPCH_THREADS
validate_optional_positive_integer "${TPCH_BATCH_SIZE:-}" TPCH_BATCH_SIZE
validate_optional_positive_integer "${TPCH_IO_CONCURRENCY:-}" TPCH_IO_CONCURRENCY
if [ -n "${TPCH_MEMORY_LIMIT_BYTES:-}" ]; then
  set -- "$@" --memory-limit "$TPCH_MEMORY_LIMIT_BYTES"
fi
if [ -n "${TPCH_THREADS:-}" ]; then set -- "$@" --threads "$TPCH_THREADS"; fi
if [ -n "${TPCH_BATCH_SIZE:-}" ]; then set -- "$@" --batch-size "$TPCH_BATCH_SIZE"; fi
if [ -n "${TPCH_IO_CONCURRENCY:-}" ]; then
  set -- "$@" --io-concurrency "$TPCH_IO_CONCURRENCY"
fi
if [ "$require_spill" = 1 ]; then
  set -- "$@" --metadata-cache 0 --metrics --spill-directory "/workspace/$work_relative/spill"
fi
set -- "$@" -f "/workspace/$work_relative/rustdb.sql"
if ! "$@" > "$rustdb_csv" 2> "$rustdb_stderr"; then
  sed -n '1,200p' "$rustdb_stderr" >&2
  tpch_die "RustDB query execution failed"
fi
if [ "$require_spill" = 1 ]; then
  metrics_line=$(grep 'peak_memory=.*spill_bytes=' "$rustdb_stderr" | tail -n 1 || true)
  [ -n "$metrics_line" ] || tpch_die "RustDB did not emit required query metrics"
  spill_bytes=$(printf '%s\n' "$metrics_line" | sed -n 's/.* spill_bytes=\([0-9][0-9]*\).*/\1/p')
  peak_memory=$(printf '%s\n' "$metrics_line" | sed -n 's/.* peak_memory=\([0-9][0-9]*\).*/\1/p')
  [ -n "$spill_bytes" ] && [ "$spill_bytes" -gt 0 ] || \
    tpch_die "constrained correctness query completed without Spill"
  if [ -n "${TPCH_MEMORY_LIMIT_BYTES:-}" ]; then
    [ -n "$peak_memory" ] && [ "$peak_memory" -le "$TPCH_MEMORY_LIMIT_BYTES" ] || \
      tpch_die "constrained correctness query exceeded its memory limit"
  fi
  if find "$spill_root" -mindepth 1 -maxdepth 1 -type d -name 'query-*' \
      -print -quit | grep -q .; then
    tpch_die "constrained correctness query left a Spill directory behind"
  fi
fi
docker run --rm --interactive \
  --volume "$TPCH_ROOT:/workspace" \
  --workdir /workspace \
  "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
  < "$duckdb_query" > "$duckdb_csv"

rustdb_checksum=$(python3 "$TPCH_TOOLS/canonicalize.py" "$rustdb_csv" "$rustdb_canonical")
duckdb_checksum=$(python3 "$TPCH_TOOLS/canonicalize.py" "$duckdb_csv" "$duckdb_canonical")
if [ "$rustdb_checksum" != "$duckdb_checksum" ]; then
  diff -u "$duckdb_canonical" "$rustdb_canonical" | sed -n '1,200p' >&2 || true
  tpch_die "checksum mismatch: rustdb=$rustdb_checksum duckdb=$duckdb_checksum"
fi
printf '%s\n' "$rustdb_checksum"
