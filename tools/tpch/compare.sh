#!/bin/sh

set -eu

. "$(dirname -- "$0")/common.sh"

usage() {
  cat >&2 <<EOF
usage: $0 [--queries WORKSPACE_FILE] [--rustdb-root ROOT] [--report] [SCALE_FACTOR]

Compares every selected query with the local DuckDB 1.4.3 reference dataset.
ROOT may be workspace-relative or s3://. --report prints one PASS/FAIL record
per query and preserves status.tsv plus per-query diagnostics. Any failed or
unsupported query still makes the command fail.
EOF
  exit 2
}

query_list_argument=benchmarks/tpch/queries.txt
rustdb_argument=
rustdb_argument_set=0
report_mode=0
scale=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --queries)
      [ "$#" -ge 2 ] || usage
      query_list_argument=$2
      shift 2
      ;;
    --rustdb-root)
      [ "$#" -ge 2 ] || usage
      rustdb_argument=$2
      rustdb_argument_set=1
      [ -n "$rustdb_argument" ] || tpch_die "--rustdb-root must not be empty"
      shift 2
      ;;
    --report)
      report_mode=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    --*)
      usage
      ;;
    *)
      [ -z "$scale" ] || usage
      scale=$1
      shift
      ;;
  esac
done

scale=${scale:-0.01}
tpch_validate_scale "$scale"
tpch_require docker
tpch_require python3
tpch_require sed

relative=$(tpch_dataset_relative "$scale")
dataset="$TPCH_ROOT/$relative"
tpch_verify_dataset "$dataset"
case "$query_list_argument" in
  /*) query_list=$query_list_argument ;;
  *) query_list=$TPCH_ROOT/$query_list_argument ;;
esac
case "$query_list" in
  "$TPCH_ROOT"/*) ;;
  *) tpch_die "query list must be inside the RustDB workspace" ;;
esac
[ -f "$query_list" ] || tpch_die "missing query list: $query_list_argument"
if [ "$rustdb_argument_set" = 0 ]; then
  rustdb_argument=$relative
fi
case "$rustdb_argument" in
  s3://*) tpch_validate_s3_uri "$rustdb_argument" ;;
esac
case "${TPCH_SKIP_BUILD:-0}" in
  0)
    tpch_build_reference
    tpch_build_rustdb
    ;;
  1) ;;
  *) tpch_die "TPCH_SKIP_BUILD must be 0 or 1" ;;
esac

results="$dataset/results"
run="$results/.run.$$"
mkdir -p "$run"
trap 'rm -rf "$run"' EXIT HUP INT TERM

: > "$run/checksums.sha256"
: > "$run/queries.seen"
printf 'query\tstatus\tchecksum\tdiagnostic\n' > "$run/status.tsv"
mkdir -p "$run/errors"
if ! binary_digest_output=$(docker compose --project-directory "$TPCH_ROOT" run \
    --rm --no-deps --no-TTY dev sha256sum /workspace/target/release/rustdb); then
  tpch_die "cannot hash the RustDB release binary; build it before using TPCH_SKIP_BUILD=1"
fi
binary_sha256=$(printf '%s\n' "$binary_digest_output" | \
  awk '/^[0-9a-f]{64}[[:space:]]/ {print $1; exit}')
printf '%s\n' "$binary_sha256" | grep -Eq '^[0-9a-f]{64}$' || \
  tpch_die "cannot determine the RustDB release binary SHA-256"
rustdb_manifest=
provenance_s3_endpoint=
provenance_s3_region=
provenance_s3_path_style=
case "$rustdb_argument" in
  s3://*)
    provenance_s3_endpoint=${TPCH_S3_ENDPOINT-http://minio:9000}
    provenance_s3_region=${TPCH_S3_REGION:-us-east-1}
    provenance_s3_path_style=${TPCH_S3_PATH_STYLE:-1}
    ;;
  *)
    if [ -f "$TPCH_ROOT/$rustdb_argument/manifest.sha256" ]; then
      rustdb_manifest=$TPCH_ROOT/$rustdb_argument/manifest.sha256
    fi
    ;;
esac
python3 "$TPCH_TOOLS/provenance.py" \
  --output "$run/provenance.json" \
  --workspace "$TPCH_ROOT" \
  --query-list "$query_list" \
  --reference-manifest "$dataset/manifest.sha256" \
  --rustdb-manifest "$rustdb_manifest" \
  --binary-path "target/release/rustdb" \
  --binary-sha256 "$binary_sha256" \
  --rustdb-root "$rustdb_argument" \
  --memory-limit "${TPCH_MEMORY_LIMIT_BYTES:-}" \
  --threads "${TPCH_THREADS:-}" \
  --batch-size "${TPCH_BATCH_SIZE:-}" \
  --io-concurrency "${TPCH_IO_CONCURRENCY:-}" \
  --require-spill "${TPCH_REQUIRE_SPILL:-0}" \
  --skip-build "${TPCH_SKIP_BUILD:-0}" \
  --s3-endpoint "$provenance_s3_endpoint" \
  --s3-region "$provenance_s3_region" \
  --s3-path-style "$provenance_s3_path_style"
failures=0
queries=0
while IFS= read -r query; do
  case "$query" in ''|'#'*) continue ;; esac
  case "$query" in
    q[0-9][0-9]) ;;
    *) tpch_die "invalid query id '$query' in $query_list_argument" ;;
  esac
  if grep -Fqx "$query" "$run/queries.seen"; then
    tpch_die "duplicate query id '$query' in $query_list_argument"
  fi
  printf '%s\n' "$query" >> "$run/queries.seen"
  queries=$((queries + 1))
  template="$TPCH_ROOT/benchmarks/tpch/$query.sql"
  [ -f "$template" ] || tpch_die "missing query template: $template"
  checksum_file=$run/$query.checksum
  error_file=$run/errors/$query.stderr
  if TPCH_SKIP_BUILD=1 "$TPCH_TOOLS/compare_query.sh" \
      "benchmarks/tpch/$query.sql" "$relative" "$rustdb_argument" \
      </dev/null > "$checksum_file" 2> "$error_file"; then
    rustdb_checksum=$(cat "$checksum_file")
    checksum_lines=$(wc -l < "$checksum_file" | tr -d '[:space:]')
    if [ "$checksum_lines" != 1 ] || \
        ! printf '%s\n' "$rustdb_checksum" | grep -Eq '^[0-9a-f]{64}$'; then
      printf 'error: checksum runner emitted an invalid checksum\n' > "$error_file"
      failures=$((failures + 1))
      printf 'FAIL  %s\n' "$query" >> "$run/checksums.sha256"
      printf '%s\tfail\t-\terrors/%s.stderr\n' "$query" "$query" >> "$run/status.tsv"
      if [ "$report_mode" = 1 ]; then
        printf 'FAIL\t%s\terrors/%s.stderr\n' "$query" "$query"
      else
        printf 'FAIL  %s\n' "$query" >&2
      fi
      continue
    fi
    printf '%s  %s\n' "$rustdb_checksum" "$query" >> "$run/checksums.sha256"
    printf '%s\tpass\t%s\t-\n' "$query" "$rustdb_checksum" >> "$run/status.tsv"
    if [ "$report_mode" = 1 ]; then
      printf 'PASS\t%s\t%s\n' "$query" "$rustdb_checksum"
    else
      printf '%s  %s\n' "$rustdb_checksum" "$query"
    fi
    rm "$checksum_file"
    [ -s "$error_file" ] || rm "$error_file"
  else
    failures=$((failures + 1))
    printf 'FAIL  %s\n' "$query" >> "$run/checksums.sha256"
    printf '%s\tfail\t-\terrors/%s.stderr\n' "$query" "$query" >> "$run/status.tsv"
    if [ "$report_mode" = 1 ]; then
      printf 'FAIL\t%s\terrors/%s.stderr\n' "$query" "$query"
    else
      printf 'FAIL  %s\n' "$query" >&2
    fi
    sed -n '1,200p' "$error_file" >&2
  fi
done < "$query_list"
[ "$queries" -gt 0 ] || tpch_die "query list is empty: $query_list_argument"
rm "$run/queries.seen"

rm -rf "$results/latest"
mv "$run" "$results/latest"
trap - EXIT HUP INT TERM
if [ "$report_mode" = 1 ]; then
  printf 'TPC-H report: %s/results/latest/status.tsv\n' "$relative"
fi
[ "$failures" -eq 0 ] || tpch_die "$failures of $queries TPC-H query checksum comparison(s) failed"
echo "TPC-H SF${scale}: all $queries query checksums match"
