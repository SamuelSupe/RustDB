#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT=${1:-/tmp/rustdb-v07-native-sf1-rustonly.json}
ROUNDS=${ROUNDS:-10}
CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
DATASET="$WORKSPACE/data/tpch-sf1"
MANIFEST="$SCRIPT_DIR/suites/native/tpch-sf1.json"
TEMP_ROOT=$(mktemp -d /tmp/rustdb-v07-native-rustonly.XXXXXX)
trap 'rm -rf "$TEMP_ROOT"' EXIT HUP INT TERM
mkdir -p "$TEMP_ROOT/rustdb"

cd "$WORKSPACE"
docker compose run --rm --no-deps --no-TTY \
  --env CARGO_BUILD_JOBS="$CARGO_BUILD_JOBS" \
  --env CARGO_TARGET_DIR=/workspace/target dev \
  cargo build --locked --quiet --release \
    --manifest-path benchmarks/v07/rustdb-runner/Cargo.toml
RUSTDB_BUILD_ID=$(docker compose run --rm --no-deps --no-TTY dev \
  sha256sum /workspace/target/release/rustdb-v07-runner | awk '{print $1}')

RUSTDB_COMMAND=$(python3 - "$WORKSPACE" "$DATASET" "$TEMP_ROOT/rustdb" "$RUSTDB_BUILD_ID" <<'PY'
import json
import sys

workspace, dataset, temporary, build_id = sys.argv[1:]
print(json.dumps([
    "docker", "compose", "--project-directory", workspace,
    "run", "--rm", "--no-deps", "--no-TTY",
    "--volume", f"{dataset}:/bench-data:ro",
    "--volume", f"{temporary}:/bench-tmp", "dev",
    "/workspace/target/release/rustdb-v07-runner",
    "--threads", "4", "--memory-limit", "2147483648",
    "--concurrency", "1", "--batch-size", "8192",
    "--metadata-cache-bytes", "0",
    "--spill-directory", "/bench-tmp/spill",
    "--database", "/bench-tmp/database",
    "--build-id", build_id,
]))
PY
)

python3 -B benchmarks/v07/rustdb_only.py \
  --query benchmarks/v07/queries/native-q6.sql \
  --dataset data/tpch-sf1 \
  --native-manifest "$MANIFEST" \
  --output "$OUTPUT" \
  --threads 4 \
  --memory-limit 2147483648 \
  --concurrency 1 \
  --batch-size 8192 \
  --rounds "$ROUNDS" \
  --rustdb-command-json "$RUSTDB_COMMAND"
