#!/bin/sh

set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
. "$ROOT/tools/tpch/common.sh"

tpch_require docker
tpch_require python3
tpch_require sed
tpch_build_reference
if [ "${RUSTDB_SQL_DIFF_SKIP_BUILD:-0}" != 1 ]; then
  tpch_build_rustdb
fi

mkdir -p "$ROOT/data"
work=$(mktemp -d "$ROOT/data/.sql-differential.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM
work_relative=${work#"$ROOT/"}
data="$work/input.csv"
printf 'x,y\n2,b\n1,c\n1,a\n' > "$data"
nullable_data="$work/nullable.csv"
printf 'id,grp,val,text\n1,a,10,Alpha\n2,a,10,Beta\n3,a,,Gamma\n4,b,20,Delta\n5,b,30,Echo\n6,b,20,Foxtrot\n7,c,,Golf\n' \
  > "$nullable_data"
outer_data="$work/correlated-outer.csv"
printf 'case_id,grp,probe\n1,match,1\n2,miss,9\n3,rhs_null,9\n4,lhs_null,\n5,empty,9\n6,multi,1\n' \
  > "$outer_data"
inner_data="$work/correlated-inner.csv"
printf 'grp,key\nmatch,1\nmiss,1\nrhs_null,\nlhs_null,1\nmulti,1\nmulti,2\n' \
  > "$inner_data"

for template in "$ROOT"/tools/sql/cases/*.sql; do
  name=$(basename "$template" .sql)
  query="$work/$name.sql"
  rustdb_csv="$work/$name.rustdb.csv"
  duckdb_csv="$work/$name.duckdb.csv"
  rustdb_rows="$work/$name.rustdb.jsonl"
  duckdb_rows="$work/$name.duckdb.jsonl"
  sed \
    -e "s|__DATA__|/workspace/$work_relative/input.csv|g" \
    -e "s|__NULL_DATA__|/workspace/$work_relative/nullable.csv|g" \
    -e "s|__OUTER_DATA__|/workspace/$work_relative/correlated-outer.csv|g" \
    -e "s|__INNER_DATA__|/workspace/$work_relative/correlated-inner.csv|g" \
    "$template" > "$query"

  case "$name" in
    distinct-hidden-order-error)
      rustdb_error="$work/$name.rustdb.err"
      if docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
        /workspace/target/release/rustdb --format csv --csv-null __RUSTDB_NULL__ \
        -f "/workspace/$work_relative/$name.sql" > /dev/null 2> "$rustdb_error"; then
        tpch_die "$name unexpectedly succeeded in RustDB"
      fi
      pattern_file=${template%.sql}.rustdb-pattern
      pattern=$(cat "$pattern_file")
      grep -F "$pattern" "$rustdb_error" >/dev/null || {
        sed -n '1,80p' "$rustdb_error" >&2
        tpch_die "$name RustDB error did not contain '$pattern'"
      }
      grep -E 'at line [1-9][0-9]*, column [1-9][0-9]*' "$rustdb_error" >/dev/null || {
        sed -n '1,80p' "$rustdb_error" >&2
        tpch_die "$name RustDB error did not contain an AST source position"
      }
      # DuckDB accepts this query. RustDB deliberately limits hidden sort
      # expressions to non-DISTINCT blocks, so this is an explicit
      # compatibility-boundary assertion rather than a same-outcome case.
      docker run --rm --interactive \
        --volume "$ROOT:/workspace" --workdir /workspace \
        "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
        < "$query" > /dev/null
      printf '%s rustdb-expected-error\n' "$name"
      continue
      ;;
    *-error)
      rustdb_error="$work/$name.rustdb.err"
      duckdb_error="$work/$name.duckdb.err"
      if docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
        /workspace/target/release/rustdb --format csv --csv-null __RUSTDB_NULL__ \
        -f "/workspace/$work_relative/$name.sql" > /dev/null 2> "$rustdb_error"; then
        tpch_die "$name unexpectedly succeeded in RustDB"
      fi
      if docker run --rm --interactive \
        --volume "$ROOT:/workspace" --workdir /workspace \
        "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
        < "$query" > /dev/null 2> "$duckdb_error"; then
        tpch_die "$name unexpectedly succeeded in DuckDB 1.4.3"
      fi
      pattern_file=${template%.sql}.rustdb-pattern
      [ -f "$pattern_file" ] || tpch_die "missing RustDB error pattern: $pattern_file"
      pattern=$(cat "$pattern_file")
      grep -F "$pattern" "$rustdb_error" >/dev/null || {
        sed -n '1,80p' "$rustdb_error" >&2
        tpch_die "$name RustDB error did not contain '$pattern'"
      }
      case "$name" in
        *-runtime-error)
          grep -F 'execution error:' "$rustdb_error" >/dev/null || {
            sed -n '1,80p' "$rustdb_error" >&2
            tpch_die "$name RustDB runtime error was not structured as an execution error"
          }
          ;;
        *)
          grep -E 'at line [1-9][0-9]*, column [1-9][0-9]*' "$rustdb_error" >/dev/null || {
            sed -n '1,80p' "$rustdb_error" >&2
            tpch_die "$name RustDB error did not contain an AST source position"
          }
          ;;
      esac
      printf '%s expected-error\n' "$name"
      continue
      ;;
  esac

  docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
    /workspace/target/release/rustdb --format csv --csv-null __RUSTDB_NULL__ \
    -f "/workspace/$work_relative/$name.sql" > "$rustdb_csv"
  docker run --rm --interactive \
    --volume "$ROOT:/workspace" --workdir /workspace \
    "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
    < "$query" > "$duckdb_csv"

  canonical_args=
  if grep -Eiq 'ORDER[[:space:]]+BY' "$query"; then
    canonical_args=--preserve-order
  fi
  rustdb_checksum=$(python3 "$ROOT/tools/tpch/canonicalize.py" \
    $canonical_args "$rustdb_csv" "$rustdb_rows")
  duckdb_checksum=$(python3 "$ROOT/tools/tpch/canonicalize.py" \
    $canonical_args "$duckdb_csv" "$duckdb_rows")
  if [ "$rustdb_checksum" != "$duckdb_checksum" ]; then
    diff -u "$duckdb_rows" "$rustdb_rows" | sed -n '1,160p' >&2 || true
    tpch_die "$name differs from DuckDB 1.4.3"
  fi
  printf '%s %s\n' "$name" "$rustdb_checksum"
done
