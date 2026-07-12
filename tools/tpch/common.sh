#!/bin/sh

set -eu

TPCH_TOOLS=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TPCH_ROOT=$(CDPATH= cd -- "$TPCH_TOOLS/../.." && pwd)
TPCH_DUCKDB_IMAGE=${TPCH_DUCKDB_IMAGE:-rustdb-duckdb-tpch:1.4.3}

tpch_die() {
  echo "error: $*" >&2
  exit 2
}

tpch_require() {
  command -v "$1" >/dev/null 2>&1 || tpch_die "required command not found: $1"
}

tpch_validate_scale() {
  printf '%s\n' "$1" | awk '
    /^[0-9]+([.][0-9]+)?$/ && ($0 + 0) > 0 { valid = 1 }
    END { exit valid ? 0 : 1 }
  ' || tpch_die "scale factor must be a positive decimal: $1"
}

tpch_validate_s3_uri() {
  uri=$1
  case "$uri" in
    s3://?*) ;;
    *) tpch_die "S3 dataset root must be a non-empty s3:// URI" ;;
  esac
  bucket_and_path=${uri#s3://}
  bucket=${bucket_and_path%%/*}
  [ -n "$bucket" ] || tpch_die "S3 dataset root must include a bucket"
}

tpch_dataset_relative() {
  printf 'data/tpch-sf%s\n' "$1"
}

tpch_build_reference() {
  if docker image inspect "$TPCH_DUCKDB_IMAGE" >/dev/null 2>&1 \
      && docker run --rm "$TPCH_DUCKDB_IMAGE" --version 2>/dev/null \
        | grep --quiet --fixed-strings 'v1.4.3' \
      && docker run --rm "$TPCH_DUCKDB_IMAGE" :memory: -batch -no-stdin \
        -c 'LOAD tpch;' >/dev/null 2>&1; then
    return
  fi
  docker build \
    --file "$TPCH_TOOLS/Dockerfile" \
    --tag "$TPCH_DUCKDB_IMAGE" \
    "$TPCH_ROOT"
}

tpch_build_rustdb() {
  if ! docker image inspect rustdb-dev:1.97 >/dev/null 2>&1; then
    docker compose --project-directory "$TPCH_ROOT" build dev
  fi
  docker compose --project-directory "$TPCH_ROOT" run --rm --no-deps --no-TTY \
    --env "RUSTFLAGS=${TPCH_RUSTFLAGS:-}" dev \
    cargo build --quiet --release --bin rustdb
}

tpch_verify_dataset() {
  python3 "$TPCH_TOOLS/manifest.py" verify "$1"
}
