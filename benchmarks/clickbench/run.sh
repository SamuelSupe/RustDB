#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DATA_DIR=${CLICKBENCH_DATA_DIR:-"$ROOT/data/clickbench"}
QUERY_FILE="$DATA_DIR/queries.sql"
ORACLE_FILE="$ROOT/benchmarks/clickbench/functional-oracle-v1.json"
TARGET_DIR=${CLICKBENCH_TARGET_DIR:-/private/tmp/rustdb-clickbench-target}
RESULT_ROOT=${CLICKBENCH_RESULT_ROOT:-"$ROOT/benchmarks/results/clickbench"}
RUN_NAME=${CLICKBENCH_RUN_NAME:-"$(date -u +%Y%m%dT%H%M%SZ)"}
OUTPUT="$RESULT_ROOT/$RUN_NAME"
QUERY_URL=${CLICKBENCH_QUERY_URL:-https://raw.githubusercontent.com/ClickHouse/ClickBench/main/clickhouse/queries.sql}
CONTAINER_MEMORY=${CLICKBENCH_CONTAINER_MEMORY:-16g}
ENGINE_MEMORY_BYTES=${CLICKBENCH_ENGINE_MEMORY_BYTES:-12884901888}
QUERY_TIMEOUT_SECONDS=${CLICKBENCH_QUERY_TIMEOUT_SECONDS:-3600}
PROFILE=${CLICKBENCH_PROFILE:-functional}
OFFLINE=${CLICKBENCH_OFFLINE:-0}
EXPECTED_QUERY_SHA256=a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219
EXPECTED_ORACLE_SHA256=3040ce083db2647e6efef0a185b0b7772f4a5722898e0f351b8a1f8e46ac43b7

case "$OFFLINE" in
  0|1) ;;
  *)
    echo "CLICKBENCH_OFFLINE must be 0 or 1" >&2
    exit 2
    ;;
esac

case "$PROFILE" in
  functional)
    DATA_NAME=hits-1m.parquet
    DEFAULT_DATA_URL=https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
    DEFAULT_DATA_BYTES=122446530
    DEFAULT_DATA_ETAG='"843c108848a3929260d44588b39ec1b6-6"'
    DEFAULT_DATA_SHA256=fa134fe101e68324e0de851146fda69624f5cbb707d387141d1c2a88a219a16d
    DATA_ADAPTER=(--binary-as-string)
    ORACLE_ARGS=(
      --oracle /workspace/benchmarks/clickbench/functional-oracle-v1.json
      --expected-oracle-sha256 "$EXPECTED_ORACLE_SHA256"
    )
    ;;
  full)
    DATA_NAME=hits-100m.parquet
    DEFAULT_DATA_URL=https://datasets.clickhouse.com/hits_compatible/hits.parquet
    DEFAULT_DATA_BYTES=14779976446
    DEFAULT_DATA_ETAG='"6b028bb94eecf0ff4e6cde62a0f8fa48-829"'
    DEFAULT_DATA_SHA256=
    DATA_ADAPTER=()
    ORACLE_ARGS=()
    ;;
  *)
    echo "unknown CLICKBENCH_PROFILE '$PROFILE'; expected functional or full" >&2
    exit 2
    ;;
esac

if [[ $OFFLINE = 1 && -z $DEFAULT_DATA_SHA256 ]]; then
  echo "ClickBench $PROFILE has no trusted SHA-256 and is not eligible for an offline acceptance run" >&2
  exit 1
fi

DATA_FILE="$DATA_DIR/$DATA_NAME"
DATA_URL=${CLICKBENCH_DATA_URL:-$DEFAULT_DATA_URL}
EXPECTED_DATA_BYTES=$DEFAULT_DATA_BYTES
EXPECTED_DATA_ETAG=$DEFAULT_DATA_ETAG
EXPECTED_DATA_SHA256=$DEFAULT_DATA_SHA256
DATA_IDENTITY_ARGS=()
if [[ -n $EXPECTED_DATA_SHA256 ]]; then
  DATA_IDENTITY_ARGS=(--expected-data-sha256 "$EXPECTED_DATA_SHA256")
fi

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mkdir -p "$DATA_DIR" "$TARGET_DIR" "$RESULT_ROOT"
if [[ ! -f "$QUERY_FILE" ]]; then
  if [[ $OFFLINE = 1 ]]; then
    echo "ClickBench offline mode requires an existing query file: $QUERY_FILE" >&2
    exit 1
  fi
  curl --fail --location --output "$QUERY_FILE.part" "$QUERY_URL"
  mv "$QUERY_FILE.part" "$QUERY_FILE"
fi
ACTUAL_QUERY_SHA256=$(sha256 "$QUERY_FILE")
if [[ $ACTUAL_QUERY_SHA256 != "$EXPECTED_QUERY_SHA256" ]]; then
  echo "ClickBench queries.sql SHA-256 is $ACTUAL_QUERY_SHA256, expected $EXPECTED_QUERY_SHA256" >&2
  exit 1
fi
if [[ $PROFILE = functional ]]; then
  ACTUAL_ORACLE_SHA256=$(sha256 "$ORACLE_FILE")
  if [[ $ACTUAL_ORACLE_SHA256 != "$EXPECTED_ORACLE_SHA256" ]]; then
    echo "ClickBench oracle SHA-256 is $ACTUAL_ORACLE_SHA256, expected $EXPECTED_ORACLE_SHA256" >&2
    exit 1
  fi
fi

if [[ ! -f "$DATA_FILE" ]] || [[ $(wc -c < "$DATA_FILE") -ne $EXPECTED_DATA_BYTES ]]; then
  if [[ $OFFLINE = 1 ]]; then
    echo "ClickBench offline mode requires the exact existing fixture: $DATA_FILE" >&2
    exit 1
  fi
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
if [[ -n $EXPECTED_DATA_SHA256 ]]; then
  ACTUAL_DATA_SHA256=$(sha256 "$DATA_FILE")
  if [[ $ACTUAL_DATA_SHA256 != "$EXPECTED_DATA_SHA256" ]]; then
    echo "ClickBench $DATA_NAME SHA-256 is $ACTUAL_DATA_SHA256, expected $EXPECTED_DATA_SHA256" >&2
    exit 1
  fi
else
  echo "ClickBench $PROFILE records an observed SHA-256 but has no pinned acceptance identity" >&2
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
    --expected-query-sha256 "$EXPECTED_QUERY_SHA256" \
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
    "${ORACLE_ARGS[@]}" \
    "${DATA_IDENTITY_ARGS[@]}" \
    "${DATA_ADAPTER[@]}"

echo "ClickBench manifest: $OUTPUT/manifest.json"
