#!/bin/sh

set -eu

. "$(dirname -- "$0")/common.sh"

if [ "$#" -gt 1 ]; then
  tpch_die "usage: $0 [SCALE_FACTOR]"
fi

scale=${1:-0.01}
tpch_validate_scale "$scale"
tpch_require docker
tpch_require python3
tpch_require sed

relative=$(tpch_dataset_relative "$scale")
dataset="$TPCH_ROOT/$relative"
tpch_verify_dataset "$dataset"
tpch_build_reference
tpch_build_rustdb

results="$dataset/results"
run="$results/.run.$$"
mkdir -p "$run"
trap 'rm -rf "$run"' EXIT HUP INT TERM

: > "$run/checksums.sha256"
failures=0
while IFS= read -r query; do
  [ -n "$query" ] || continue
  template="$TPCH_ROOT/benchmarks/tpch/$query.sql"
  [ -f "$template" ] || tpch_die "missing query template: $template"
  if rustdb_checksum=$(TPCH_SKIP_BUILD=1 "$TPCH_TOOLS/compare_query.sh" \
      "benchmarks/tpch/$query.sql" "$relative" </dev/null); then
    printf '%s  %s\n' "$rustdb_checksum" "$query" | tee -a "$run/checksums.sha256"
  else
    failures=$((failures + 1))
    printf 'FAIL  %s\n' "$query" | tee -a "$run/checksums.sha256" >&2
  fi
done < "$TPCH_ROOT/benchmarks/tpch/queries.txt"

rm -rf "$results/latest"
mv "$run" "$results/latest"
trap - EXIT HUP INT TERM
[ "$failures" -eq 0 ] || tpch_die "$failures TPC-H query checksum comparison(s) failed"
echo "TPC-H SF${scale}: all query checksums match"
