#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT=${1:-/tmp/rustdb-v07-native-sf1.json}
ITERATIONS=${ITERATIONS:-10}
CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
DIAGNOSTIC_ONLY=${DIAGNOSTIC_ONLY:-0}
BATCH_SIZE=${BATCH_SIZE:-8192}
DATASET="$WORKSPACE/data/tpch-sf1"
MANIFEST="$SCRIPT_DIR/suites/native/tpch-sf1.json"
TEMP_ROOT=$(mktemp -d /tmp/rustdb-v07-native-sf1.XXXXXX)
trap 'rm -rf "$TEMP_ROOT"' EXIT HUP INT TERM
mkdir -p "$TEMP_ROOT/rustdb" "$TEMP_ROOT/duckdb"

SOURCE_FACTS=$(python3 - "$DATASET" "$MANIFEST" <<'PY'
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
manifest = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
files = {
    path.resolve()
    for table in manifest["tables"]
    for path in root.glob(table["path"])
    if path.is_file()
}
print(sum(path.stat().st_size for path in files), len(manifest["tables"]))
PY
)
set -- $SOURCE_FACTS
SOURCE_BYTES=$1
TABLE_COUNT=$2
# One general-purpose database per engine may use at most 2x the shared source,
# plus the bounded per-table metadata allowance and one MiB for harness markers.
MAX_NATIVE_WORKSPACE_BYTES=$((SOURCE_BYTES * 2 + TABLE_COUNT * 65536 + 1048576))

cd "$WORKSPACE"
docker compose run --rm --no-deps --no-TTY \
  --env CARGO_BUILD_JOBS="$CARGO_BUILD_JOBS" \
  --env CARGO_TARGET_DIR=/workspace/target dev \
  cargo build --locked --quiet --release \
    --manifest-path benchmarks/v07/rustdb-runner/Cargo.toml
docker build --quiet \
  --file tools/duckdb-bench/Dockerfile \
  --tag rustdb-duckdb-bench:1.5.4 .
RUSTDB_BUILD_ID=$(docker compose run --rm --no-deps --no-TTY dev \
  sha256sum /workspace/target/release/rustdb-v07-runner | awk '{print $1}')
DUCKDB_BUILD_ID=$(docker image inspect --format '{{.Id}}' rustdb-duckdb-bench:1.5.4)
DUCKDB_BUILD_ID=${DUCKDB_BUILD_ID#sha256:}

RUSTDB_COMMAND=$(python3 - "$WORKSPACE" "$DATASET" "$TEMP_ROOT/rustdb" "$RUSTDB_BUILD_ID" "$BATCH_SIZE" <<'PY'
import json
import sys

workspace, dataset, temporary, build_id, batch_size = sys.argv[1:]
print(json.dumps([
    "docker", "compose", "--project-directory", workspace,
    "run", "--rm", "--no-deps", "--no-TTY",
    "--volume", f"{dataset}:/bench-data:ro",
    "--volume", f"{temporary}:/bench-tmp", "dev",
    "/workspace/target/release/rustdb-v07-runner",
    "--threads", "4", "--memory-limit", "2147483648",
    "--concurrency", "1", "--batch-size", batch_size,
    "--metadata-cache-bytes", "0",
    "--spill-directory", "/bench-tmp/spill",
    "--database", "/bench-tmp/database",
    "--build-id", build_id,
]))
PY
)
DUCKDB_COMMAND=$(python3 - "$WORKSPACE" "$DATASET" "$TEMP_ROOT/duckdb" "$DUCKDB_BUILD_ID" "$BATCH_SIZE" <<'PY'
import json
import sys

workspace, dataset, temporary, build_id, batch_size = sys.argv[1:]
print(json.dumps([
    "docker", "run", "--rm", "--interactive",
    "--volume", f"{workspace}:/workspace:ro",
    "--volume", f"{dataset}:/bench-data:ro",
    "--volume", f"{temporary}:/bench-tmp",
    "--workdir", "/workspace", "rustdb-duckdb-bench:1.5.4",
    "--threads", "4", "--memory-limit", "2147483648",
    "--concurrency", "1", "--batch-size", batch_size,
    "--temp-directory", "/bench-tmp/spill",
    "--database", "/bench-tmp/benchmark.duckdb",
    "--build-id", build_id,
]))
PY
)

DIAGNOSTIC_FLAG=""
if [ "$DIAGNOSTIC_ONLY" = 1 ]; then
  DIAGNOSTIC_FLAG="--diagnostic-only"
fi
python3 -B benchmarks/v07/coordinator.py \
  --query benchmarks/v07/queries/native-q6.sql \
  --dataset data/tpch-sf1 \
  --native-manifest "$MANIFEST" \
  --max-native-workspace-bytes "$MAX_NATIVE_WORKSPACE_BYTES" \
  --storage-track native \
  --storage-medium local-nvme \
  --output "$OUTPUT" \
  --threads 4 \
  --memory-limit 2147483648 \
  --concurrency 1 \
  --batch-size "$BATCH_SIZE" \
  --warmup 0 \
  --iterations "$ITERATIONS" \
  $DIAGNOSTIC_FLAG \
  --rustdb-command-json "$RUSTDB_COMMAND" \
  --duckdb-command-json "$DUCKDB_COMMAND"
python3 -B benchmarks/v07/contract.py "$OUTPUT"
