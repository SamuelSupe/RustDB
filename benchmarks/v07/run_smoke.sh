#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORKSPACE=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
OUTPUT=${1:-/tmp/rustdb-v07-smoke-report.json}
THREADS=${THREADS:-4}
MEMORY_LIMIT=${MEMORY_LIMIT:-2147483648}
CONCURRENCY=${CONCURRENCY:-1}
TEMP_ROOT=$(mktemp -d /tmp/rustdb-v07-smoke.XXXXXX)
trap 'rm -rf "$TEMP_ROOT"' EXIT HUP INT TERM
mkdir -p "$TEMP_ROOT/rustdb" "$TEMP_ROOT/duckdb"

case $THREADS in ''|*[!0-9]*|0) echo "THREADS must be a positive integer" >&2; exit 2;; esac
case $MEMORY_LIMIT in ''|*[!0-9]*|0) echo "MEMORY_LIMIT must be a positive integer" >&2; exit 2;; esac
case $CONCURRENCY in ''|*[!0-9]*|0) echo "CONCURRENCY must be a positive integer" >&2; exit 2;; esac

cd "$WORKSPACE"
docker compose run --rm --no-deps --no-TTY \
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

RUSTDB_COMMAND=$(python3 - "$WORKSPACE" "$TEMP_ROOT/rustdb" "$RUSTDB_BUILD_ID" "$THREADS" "$MEMORY_LIMIT" "$CONCURRENCY" <<'PY'
import json
import sys

workspace, temporary, build_id, threads, memory, concurrency = sys.argv[1:]
print(json.dumps([
    "docker", "compose", "--project-directory", workspace,
    "run", "--rm", "--no-deps", "--no-TTY",
    "--volume", f"{temporary}:/bench-tmp", "dev",
    "/workspace/target/release/rustdb-v07-runner",
    "--threads", threads, "--memory-limit", memory,
    "--concurrency", concurrency, "--batch-size", "8192",
    "--metadata-cache-bytes", "0",
    "--spill-directory", "/bench-tmp/spill",
    "--build-id", build_id,
]))
PY
)
DUCKDB_COMMAND=$(python3 - "$WORKSPACE" "$TEMP_ROOT/duckdb" "$DUCKDB_BUILD_ID" "$THREADS" "$MEMORY_LIMIT" "$CONCURRENCY" <<'PY'
import json
import sys

workspace, temporary, build_id, threads, memory, concurrency = sys.argv[1:]
print(json.dumps([
    "docker", "run", "--rm", "--interactive",
    "--volume", f"{workspace}:/workspace:ro",
    "--volume", f"{temporary}:/bench-tmp",
    "--workdir", "/workspace", "rustdb-duckdb-bench:1.5.4",
    "--threads", threads, "--memory-limit", memory,
    "--concurrency", concurrency, "--batch-size", "8192",
    "--temp-directory", "/bench-tmp/spill",
    "--database", "/bench-tmp/benchmark.duckdb",
    "--build-id", build_id,
]))
PY
)

python3 -B benchmarks/v07/coordinator.py \
  --query benchmarks/v07/suites/smoke/csv_aggregate.sql \
  --dataset tests/fixtures/employees.csv \
  --storage-track csv \
  --storage-medium local-nvme \
  --output "$OUTPUT" \
  --threads "$THREADS" \
  --memory-limit "$MEMORY_LIMIT" \
  --concurrency "$CONCURRENCY" \
  --batch-size 8192 \
  --warmup 1 \
  --iterations 1 \
  --rustdb-command-json "$RUSTDB_COMMAND" \
  --duckdb-command-json "$DUCKDB_COMMAND"
python3 -B benchmarks/v07/contract.py "$OUTPUT"
