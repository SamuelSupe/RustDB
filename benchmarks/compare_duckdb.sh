#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: $0 QUERY_TEMPLATE.sql TPCH_PARQUET_ROOT" >&2
  exit 2
fi

query_template=$1
dataset_relative=$2
duckdb_version=${DUCKDB_VERSION:-v1.4.3}

case "$dataset_relative" in
  /*|../*|*/../*)
    echo "TPCH_PARQUET_ROOT must be relative to the RustDB workspace" >&2
    exit 2
    ;;
esac

command -v duckdb >/dev/null 2>&1 || {
  echo "duckdb CLI is required" >&2
  exit 2
}
duckdb --version | grep -F "$duckdb_version" >/dev/null || {
  echo "expected DuckDB $duckdb_version; got: $(duckdb --version)" >&2
  exit 2
}

mkdir -p benchmarks/results
rustdb_query=$(mktemp benchmarks/.rustdb-query.XXXXXX.sql)
duckdb_query=$(mktemp benchmarks/.duckdb-query.XXXXXX.sql)
rustdb_csv=$(mktemp benchmarks/results/.rustdb.XXXXXX.csv)
duckdb_csv=$(mktemp benchmarks/results/.duckdb.XXXXXX.csv)
rustdb_sorted=$(mktemp benchmarks/results/.rustdb-sorted.XXXXXX.csv)
duckdb_sorted=$(mktemp benchmarks/results/.duckdb-sorted.XXXXXX.csv)
trap 'rm -f "$rustdb_query" "$duckdb_query" "$rustdb_csv" "$duckdb_csv" "$rustdb_sorted" "$duckdb_sorted"' EXIT

host_root=$(pwd)/$dataset_relative
container_root=/workspace/$dataset_relative
escaped_host=$(printf '%s' "$host_root" | sed 's/[&|]/\\&/g')
escaped_container=$(printf '%s' "$container_root" | sed 's/[&|]/\\&/g')
sed "s|__TPCH_ROOT__|$escaped_host|g" "$query_template" > "$duckdb_query"
sed "s|__TPCH_ROOT__|$escaped_container|g" "$query_template" > "$rustdb_query"

docker compose run --rm --no-deps dev \
  cargo run --quiet --release --bin rustdb -- \
  --format csv -f "/workspace/$rustdb_query" > "$rustdb_csv"
duckdb -csv -header -c "$(cat "$duckdb_query")" > "$duckdb_csv"

{
  head -n 1 "$rustdb_csv"
  tail -n +2 "$rustdb_csv" | LC_ALL=C sort
} > "$rustdb_sorted"
{
  head -n 1 "$duckdb_csv"
  tail -n +2 "$duckdb_csv" | LC_ALL=C sort
} > "$duckdb_sorted"

rustdb_checksum=$(shasum -a 256 "$rustdb_sorted" | awk '{print $1}')
duckdb_checksum=$(shasum -a 256 "$duckdb_sorted" | awk '{print $1}')
if [ "$rustdb_checksum" != "$duckdb_checksum" ]; then
  echo "checksum mismatch: rustdb=$rustdb_checksum duckdb=$duckdb_checksum" >&2
  diff -u "$duckdb_sorted" "$rustdb_sorted" || true
  exit 1
fi

echo "$rustdb_checksum  $(basename "$query_template")"
