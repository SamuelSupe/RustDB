#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DATA_DIR=${CLICKBENCH_DATA_DIR:-"$ROOT/data/clickbench"}
QUERY_FILE="$DATA_DIR/queries.sql"
TARGET_DIR=${CLICKBENCH_TARGET_DIR:-/private/tmp/rustdb-clickbench-target}
RESULT_ROOT=${CLICKBENCH_RESULT_ROOT:-"$ROOT/benchmarks/results/clickbench"}
RUN_NAME=${CLICKBENCH_RUN_NAME:-"$(date -u +%Y%m%dT%H%M%SZ)"}
OUTPUT="$RESULT_ROOT/$RUN_NAME"
QUERY_URL=${CLICKBENCH_QUERY_URL:-https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse/queries.sql}
CONTAINER_MEMORY=${CLICKBENCH_CONTAINER_MEMORY:-16g}
ENGINE_MEMORY_BYTES=${CLICKBENCH_ENGINE_MEMORY_BYTES:-12884901888}
QUERY_TIMEOUT_SECONDS=${CLICKBENCH_QUERY_TIMEOUT_SECONDS:-3600}
PROFILE=${CLICKBENCH_PROFILE:-functional}

case "$PROFILE" in
  functional)
    DATA_NAME=hits-1m.parquet
    DEFAULT_DATA_URL=https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
    DEFAULT_DATA_BYTES=122446530
    DEFAULT_DATA_ETAG='"843c108848a3929260d44588b39ec1b6-6"'
    DATA_ADAPTER=(--binary-as-string)
    ;;
  full)
    DATA_NAME=hits-100m.parquet
    DEFAULT_DATA_URL=https://datasets.clickhouse.com/hits_compatible/hits.parquet
    DEFAULT_DATA_BYTES=14779976446
    DEFAULT_DATA_ETAG='"6b028bb94eecf0ff4e6cde62a0f8fa48-829"'
    DATA_ADAPTER=()
    ;;
  *)
    echo "unknown CLICKBENCH_PROFILE '$PROFILE'; expected functional or full" >&2
    exit 2
    ;;
esac

DATA_FILE="$DATA_DIR/$DATA_NAME"
DATA_URL=${CLICKBENCH_DATA_URL:-$DEFAULT_DATA_URL}
EXPECTED_DATA_BYTES=${CLICKBENCH_EXPECTED_DATA_BYTES:-$DEFAULT_DATA_BYTES}
EXPECTED_DATA_ETAG=${CLICKBENCH_EXPECTED_DATA_ETAG:-$DEFAULT_DATA_ETAG}

mkdir -p "$DATA_DIR" "$TARGET_DIR" "$RESULT_ROOT"
if [[ ! -f "$QUERY_FILE" ]]; then
  curl --fail --location --output "$QUERY_FILE.part" "$QUERY_URL"
  mv "$QUERY_FILE.part" "$QUERY_FILE"
fi

if [[ ! -f "$DATA_FILE" ]] || [[ $(wc -c < "$DATA_FILE") -ne $EXPECTED_DATA_BYTES ]]; then
  if command -v aria2c >/dev/null 2>&1; then
    aria2c \
      --continue=true \
      --auto-file-renaming=false \
      --file-allocation=none \
      --max-connection-per-server=16 \
      --split=16 \
      --min-split-size=16M \
      --dir="$DATA_DIR" \
      --out="$DATA_NAME" \
      "$DATA_URL"
  elif [[ -e "$DATA_FILE.aria2" ]]; then
    echo "aria2 resume metadata exists but aria2c is unavailable: $DATA_FILE.aria2" >&2
    exit 1
  else
    python3 -B "$ROOT/benchmarks/clickbench/download.py" \
      --url "$DATA_URL" \
      --output "$DATA_FILE" \
      --size "$EXPECTED_DATA_BYTES" \
      --etag "$EXPECTED_DATA_ETAG" \
      --connections 32 \
      --chunk-bytes 134217728
  fi
fi
ACTUAL_DATA_BYTES=$(wc -c < "$DATA_FILE")
if [[ $ACTUAL_DATA_BYTES -ne $EXPECTED_DATA_BYTES ]]; then
  echo "ClickBench data size is $ACTUAL_DATA_BYTES, expected $EXPECTED_DATA_BYTES" >&2
  exit 1
fi
if [[ -e "$DATA_FILE.aria2" ]]; then
  echo "ClickBench aria2 control file remains after download: $DATA_FILE.aria2" >&2
  exit 1
fi
if [[ -e "$OUTPUT" ]]; then
  echo "ClickBench output already exists: $OUTPUT" >&2
  exit 1
fi

docker compose -f "$ROOT/compose.yaml" run --rm --no-deps -T \
  --volume "$TARGET_DIR:/clickbench-target" \
  --env CARGO_TARGET_DIR=/clickbench-target \
  --env CARGO_BUILD_JOBS=2 \
  --env 'RUSTFLAGS=-C target-cpu=native' \
  dev cargo build --locked --release --bin rustdb-bench

BUILD_ID=$(git -C "$ROOT" rev-parse HEAD)
RUSTC_VERSION=$(docker run --rm rustdb-dev:1.97 rustc --version)
CPU_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)

docker run --rm --cpus 4 --memory "$CONTAINER_MEMORY" \
  --volume "$ROOT:/workspace:ro" \
  --volume "$DATA_DIR:/data:ro" \
  --volume "$TARGET_DIR:/clickbench-target:ro" \
  --volume "$RESULT_ROOT:/results" \
  rustdb-dev:1.97 \
  python3 -B /workspace/benchmarks/clickbench/run.py \
    --queries /data/queries.sql \
    --data "/data/$DATA_NAME" \
    --dataset-profile "$PROFILE" \
    --data-etag "$EXPECTED_DATA_ETAG" \
    --output "/results/$RUN_NAME" \
    --binary /clickbench-target/release/rustdb-bench \
    --threads 4 \
    --memory-limit "$ENGINE_MEMORY_BYTES" \
    --batch-size 8192 \
    --io-concurrency 16 \
    --metadata-cache-bytes 268435456 \
    --timeout-seconds "$QUERY_TIMEOUT_SECONDS" \
    --build-id "$BUILD_ID" \
    --rustc-version "$RUSTC_VERSION" \
    --cpu-model "$CPU_MODEL" \
    "${DATA_ADAPTER[@]}"

echo "ClickBench manifest: $OUTPUT/manifest.json"
