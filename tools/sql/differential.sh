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

render_template() {
  sed \
    -e "s|__DATA__|/workspace/$work_relative/input.csv|g" \
    -e "s|__NULL_DATA__|/workspace/$work_relative/nullable.csv|g" \
    -e "s|__OUTER_DATA__|/workspace/$work_relative/correlated-outer.csv|g" \
    -e "s|__INNER_DATA__|/workspace/$work_relative/correlated-inner.csv|g" \
    "$1" > "$2"
}

for template in "$ROOT"/tools/sql/cases/*.sql; do
  name=$(basename "$template" .sql)
  query="$work/$name.sql"
  rustdb_csv="$work/$name.rustdb.csv"
  duckdb_csv="$work/$name.duckdb.csv"
  rustdb_rows="$work/$name.rustdb.jsonl"
  duckdb_rows="$work/$name.duckdb.jsonl"
  render_template "$template" "$query"
  duckdb_query=$query
  duckdb_template=${template%.sql}.duckdb
  if [ -f "$duckdb_template" ]; then
    duckdb_query="$work/$name.duckdb.sql"
    render_template "$duckdb_template" "$duckdb_query"
  fi

  case "$name" in
    distinct-hidden-order-error|*-rustdb-error)
      rustdb_error="$work/$name.rustdb.err"
      if docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
        /workspace/target/release/rustdb --format csv --csv-null __RUSTDB_NULL__ \
        -f "/workspace/$work_relative/$name.sql" > /dev/null 2> "$rustdb_error"; then
        tpch_die "$name unexpectedly succeeded in RustDB"
      fi
      pattern_file=${template%.sql}.rustdb-pattern
      [ -f "$pattern_file" ] || tpch_die "missing RustDB error pattern: $pattern_file"
      pattern=$(cat "$pattern_file")
      grep -F "$pattern" "$rustdb_error" >/dev/null || {
        sed -n '1,80p' "$rustdb_error" >&2
        tpch_die "$name RustDB error did not contain '$pattern'"
      }
      if [ "$name" = distinct-hidden-order-error ]; then
        grep -E 'at line [1-9][0-9]*, column [1-9][0-9]*' "$rustdb_error" >/dev/null || {
          sed -n '1,80p' "$rustdb_error" >&2
          tpch_die "$name RustDB error did not contain an AST source position"
        }
      fi
      # These are explicit compatibility boundaries: RustDB must reject with
      # its documented error while DuckDB must accept the same SQL.
      docker run --rm --interactive \
        --volume "$ROOT:/workspace" --workdir /workspace \
        "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
        < "$duckdb_query" > /dev/null
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
        < "$duckdb_query" > /dev/null 2> "$duckdb_error"; then
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
        *-invalid-argument-error)
          grep -F 'error: invalid argument:' "$rustdb_error" >/dev/null || {
            sed -n '1,80p' "$rustdb_error" >&2
            tpch_die "$name was not structured as an invalid argument error"
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
    < "$duckdb_query" > "$duckdb_csv"

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

prepared_rustdb_csv="$work/prepared-api.rustdb.csv"
prepared_duckdb_csv="$work/prepared-api.duckdb.csv"
prepared_rustdb_rows="$work/prepared-api.rustdb.jsonl"
prepared_duckdb_rows="$work/prepared-api.duckdb.jsonl"
docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
  cargo build --quiet --locked --release --example prepared_differential
docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
  /workspace/target/release/examples/prepared_differential > "$prepared_rustdb_csv"
prepared_error_check=$(docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
  /workspace/target/release/examples/prepared_differential --verify-errors)
[ "$prepared_error_check" = "prepared-errors,ok" ] || \
  tpch_die "prepared API parameter-count/mixed-style validation did not run"
docker run --rm --interactive \
  "$TPCH_DUCKDB_IMAGE" :memory: -csv -header -nullvalue __RUSTDB_NULL__ -batch \
  > "$prepared_duckdb_csv" <<'SQL'
PREPARE rustdb_prepared AS
SELECT $1 + $1 AS repeated_total,
       $2 = CAST('18446744073709551615' AS UBIGINT) AS uint64_ok,
       $3 = CAST('3.5' AS DOUBLE) AS float64_ok,
       $4 = CAST('ABC' AS BLOB) AS binary_ok,
       $5 = TIMESTAMP '2024-01-02 03:04:05.123456' AS timestamp_ok,
       $6 IS NULL AS typed_null_ok,
       $7 AS flag,
       $8 AS text_value,
       $9 = DATE '2024-01-02' AS date_ok,
       $10 = CAST('12.34' AS DECIMAL(10, 2)) AS decimal_ok;
EXECUTE rustdb_prepared(
  21,
  18446744073709551615::UBIGINT,
  3.5::DOUBLE,
  CAST('ABC' AS BLOB),
  TIMESTAMP '2024-01-02 03:04:05.123456',
  CAST(NULL AS BIGINT),
  true,
  'prepared',
  DATE '2024-01-02',
  12.34::DECIMAL(10, 2)
);
SQL
prepared_rustdb_checksum=$(python3 "$ROOT/tools/tpch/canonicalize.py" \
  --preserve-order "$prepared_rustdb_csv" "$prepared_rustdb_rows")
prepared_duckdb_checksum=$(python3 "$ROOT/tools/tpch/canonicalize.py" \
  --preserve-order "$prepared_duckdb_csv" "$prepared_duckdb_rows")
if [ "$prepared_rustdb_checksum" != "$prepared_duckdb_checksum" ]; then
  diff -u "$prepared_duckdb_rows" "$prepared_rustdb_rows" | sed -n '1,160p' >&2 || true
  tpch_die "prepared Rust API differs from DuckDB 1.4.3"
fi
printf 'prepared-api %s\n' "$prepared_rustdb_checksum"
