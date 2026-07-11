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
if [ -d "$dataset" ]; then
  tpch_verify_dataset "$dataset" || tpch_die "existing dataset is incomplete; remove $relative and retry"
  echo "TPC-H SF${scale}: reusing verified $relative"
  exit 0
fi

tpch_build_reference

stage_relative="data/.tpch-sf${scale}.$$"
stage="$TPCH_ROOT/$stage_relative"
trap 'rm -rf "$stage"' EXIT HUP INT TERM
for table in customer lineitem nation orders part partsupp region supplier; do
  mkdir -p "$stage/$table"
done

sed \
  -e "s|__SCALE__|$scale|g" \
  -e "s|__OUTPUT__|/workspace/$stage_relative|g" \
  "$TPCH_TOOLS/generate.sql.in" > "$stage/generate.sql"

docker run --rm --interactive \
  --volume "$TPCH_ROOT:/workspace" \
  --workdir /workspace \
  "$TPCH_DUCKDB_IMAGE" :memory: -batch < "$stage/generate.sql"

rm "$stage/generate.sql"
python3 "$TPCH_TOOLS/manifest.py" create "$stage"
printf '{"duckdb":"1.4.3","scale_factor":"%s","compression":"zstd:3","row_group_size":122880}\n' \
  "$scale" > "$stage/manifest.json"
tpch_verify_dataset "$stage"
mv "$stage" "$dataset"
trap - EXIT HUP INT TERM
echo "TPC-H SF${scale}: generated $relative"
