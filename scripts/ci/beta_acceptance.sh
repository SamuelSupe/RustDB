#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
INPUT_HELPER="$ROOT/scripts/ci/beta_acceptance_inputs.py"
EVIDENCE_HELPER="$ROOT/scripts/ci/beta_acceptance_evidence.py"
LOCAL_HELPER="$ROOT/scripts/ci/beta_acceptance_local.py"
MINIO_HELPER="$ROOT/scripts/ci/beta_acceptance_minio.py"
TIMEOUT_HELPER="$ROOT/scripts/ci/beta_acceptance_timeout.py"
TPCH_HELPER="$ROOT/scripts/ci/beta_acceptance_tpch.py"
EXTERNAL_RUNNER="$ROOT/benchmarks/v07/rustdb_external_only.py"
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
CURRENT_STEP=bootstrap
TEMP_ROOT=

if [[ -z ${RUSTDB_BETA_ACCEPTANCE_OUTPUT:-} ]]; then
  echo "RUSTDB_BETA_ACCEPTANCE_OUTPUT is required" >&2
  exit 2
fi

OUTPUT=$(python3 -B "$INPUT_HELPER" create-output \
  --workspace "$ROOT" \
  --output "$RUSTDB_BETA_ACCEPTANCE_OUTPUT")
STEPS_FILE="$OUTPUT/steps.jsonl"

finish() {
  local exit_code=$?
  local status finalize_status failed_step
  trap - EXIT
  trap '' INT TERM
  if [[ $exit_code -eq 0 ]]; then
    status=passed
    failed_step=
  else
    status=failed
    failed_step=$CURRENT_STEP
  fi
  set +e
  python3 -B "$EVIDENCE_HELPER" finalize \
    --workspace "$ROOT" \
    --output "$OUTPUT" \
    --status "$status" \
    --exit-code "$exit_code" \
    --failed-step "$failed_step" \
    --started-at "$STARTED_AT"
  finalize_status=$?
  if [[ $finalize_status -ne 0 && $exit_code -eq 0 ]]; then
    exit_code=$finalize_status
  fi
  if [[ -n $TEMP_ROOT && -d $TEMP_ROOT ]]; then
    rm -rf "$TEMP_ROOT"
  fi
  exit "$exit_code"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

record_step() {
  local name=$1
  local phase=$2
  local exit_code=${3:-}
  local log=${4:-}
  local args=(
    record-step --file "$STEPS_FILE" --name "$name" --phase "$phase"
    --at "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  )
  if [[ -n $exit_code ]]; then
    args+=(--exit-code "$exit_code")
  fi
  if [[ -n $log ]]; then
    args+=(--log "$log")
  fi
  python3 -B "$EVIDENCE_HELPER" "${args[@]}"
}

run_step() {
  local name=$1
  shift
  local log="$OUTPUT/logs/$name.log"
  local exit_code
  CURRENT_STEP=$name
  record_step "$name" started "" "$log"
  set +e
  "$@" 2>&1 | tee "$log"
  exit_code=${PIPESTATUS[0]}
  set -e
  record_step "$name" finished "$exit_code" "$log"
  if [[ $exit_code -ne 0 ]]; then
    return "$exit_code"
  fi
}

preflight() {
  python3 -B "$INPUT_HELPER" preflight \
    --workspace "$ROOT" \
    --output "$OUTPUT" \
    --local-fixture "${RUSTDB_BETA_LOCAL_FIXTURE:-}" \
    --local-format "${RUSTDB_BETA_LOCAL_FORMAT:-}" \
    --minio-manifest "${RUSTDB_BETA_MINIO_MANIFEST:-}" \
    --minio-format "${RUSTDB_BETA_MINIO_FORMAT:-}" \
    --clickbench-data-dir "${RUSTDB_BETA_CLICKBENCH_DATA_DIR:-}" \
    --clickbench-profile "${RUSTDB_BETA_CLICKBENCH_PROFILE:-}"
}

verify_minio_fixture() {
  local phase=$1
  local manifest="$OUTPUT/minio-fixture-manifest.json"
  local listing="$OUTPUT/minio-$phase-listing.jsonl"
  local verification="$OUTPUT/minio-$phase-verification.json"
  local alias_path
  alias_path=$(python3 -B "$MINIO_HELPER" alias-path --manifest "$manifest") || return $?
  docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY \
    --entrypoint /bin/sh minio-init -c \
    'mc alias set rustdb http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls --recursive --json "rustdb/$1"' \
    shell "$alias_path" >"$listing" || return $?
  python3 -B "$MINIO_HELPER" verify \
    --manifest "$manifest" --listing "$listing" --output "$verification"
}

verify_local_fixture() {
  python3 -B "$LOCAL_HELPER" \
    --manifest "$OUTPUT/local-fixture-manifest.json" \
    --root "$RUSTDB_BETA_LOCAL_FIXTURE" \
    --output "$OUTPUT/local-after-verification.json"
}

build_runner() {
  local digest
  docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY \
    --env CARGO_BUILD_JOBS=2 \
    --env CARGO_TARGET_DIR=/workspace/target dev \
    cargo build --locked --quiet --release \
      --manifest-path benchmarks/v07/rustdb-runner/Cargo.toml || return $?
  if ! digest=$(docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY dev \
    sha256sum /workspace/target/release/rustdb-v07-runner); then
    return 1
  fi
  digest=${digest%% *}
  if [[ ! $digest =~ ^[0-9a-f]{64}$ ]]; then
    echo "runner build returned an invalid SHA-256: $digest" >&2
    return 1
  fi
  printf '%s\n' "$digest" >"$TEMP_ROOT/runner-build-id" || return $?
  echo "runner build id: $digest"
}

run_external() {
  local name=$1
  local medium=$2
  local memory=$3
  local query=$4
  local dataset=$5
  local track=$6
  local fixture=${7:-}
  local temporary="$TEMP_ROOT/$name"
  local command_json spill_entry
  local runner_args=(
    runner-command --workspace "$ROOT" --temporary "$temporary"
    --build-id "$RUSTDB_BUILD_ID" --memory-limit "$memory"
  )
  mkdir -p "$temporary" || return $?
  if [[ -n $fixture ]]; then
    runner_args+=(--fixture "$fixture")
  fi
  if [[ $medium = minio ]]; then
    runner_args+=(--minio)
  fi
  if ! command_json=$(python3 -B "$INPUT_HELPER" "${runner_args[@]}"); then
    return 1
  fi
  python3 -B "$EXTERNAL_RUNNER" \
    --query "$query" \
    --dataset "$dataset" \
    --storage-track "$track" \
    --storage-medium "$medium" \
    --output "$OUTPUT/reports/$name.json" \
    --threads 4 \
    --memory-limit "$memory" \
    --concurrency 8 \
    --batch-size 8192 \
    --warmup 0 \
    --iterations 1 \
    --query-timeout-seconds 3600 \
    --rustdb-command-json "$command_json" || return $?
  if [[ -d $temporary/spill ]]; then
    if ! spill_entry=$(find "$temporary/spill" -mindepth 1 -print -quit); then
      echo "cannot inspect external Spill directory: $temporary/spill" >&2
      return 1
    fi
    if [[ -n $spill_entry ]]; then
      echo "external run left Spill entries: $spill_entry" >&2
      return 1
    fi
  fi
}

run_clickbench() {
  env \
    CLICKBENCH_DATA_DIR="$RUSTDB_BETA_CLICKBENCH_DATA_DIR" \
    CLICKBENCH_PROFILE="$RUSTDB_BETA_CLICKBENCH_PROFILE" \
    CLICKBENCH_OFFLINE=1 \
    CLICKBENCH_TARGET_DIR="$TEMP_ROOT/clickbench-target" \
    CLICKBENCH_RESULT_ROOT="$OUTPUT/clickbench" \
    CLICKBENCH_RUN_NAME=beta-acceptance \
    CLICKBENCH_CONTAINER_MEMORY=12g \
    CLICKBENCH_ENGINE_MEMORY_BYTES=4294967296 \
    "$ROOT/benchmarks/clickbench/run.sh"
}

run_beta2_lifecycle() {
  python3 -B "$TIMEOUT_HELPER" --seconds 3600 -- \
    docker compose --project-directory "$ROOT" run --rm --no-deps --no-TTY \
      dev ./scripts/ci/check.sh beta2
}

capture_tpch() {
  python3 -B "$TPCH_HELPER" capture \
    --workspace "$ROOT" --output "$OUTPUT" --medium "$1"
}

run_tpch_local() {
  local compare_status=0 capture_status=0
  "$ROOT/tools/tpch/generate.sh" 1 || return $?
  env TPCH_SKIP_BUILD=0 TPCH_REQUIRE_SPILL=0 TPCH_RUSTFLAGS= \
    TPCH_MEMORY_LIMIT_BYTES=4294967296 TPCH_THREADS=4 \
    TPCH_BATCH_SIZE=8192 TPCH_IO_CONCURRENCY=16 \
    "$ROOT/tools/tpch/compare.sh" --report \
      --queries benchmarks/tpch/cases/sf1-local.txt 1 || compare_status=$?
  capture_tpch local || capture_status=$?
  [[ $compare_status -eq 0 ]] || return "$compare_status"
  return "$capture_status"
}

run_tpch_minio() {
  local compare_status=0 capture_status=0 uri uri_log
  uri_log="$TEMP_ROOT/tpch-minio-upload.log"
  "$ROOT/tools/tpch/upload_minio.sh" 1 | tee "$uri_log" || return $?
  uri=$(tail -n 1 "$uri_log")
  [[ $uri = s3://rustdb-tests/tpch-sf1 ]] || {
    echo "unexpected TPC-H MinIO root: $uri" >&2
    return 1
  }
  env TPCH_SKIP_BUILD=1 TPCH_REQUIRE_SPILL=0 \
    TPCH_MEMORY_LIMIT_BYTES=4294967296 TPCH_THREADS=4 \
    TPCH_BATCH_SIZE=8192 TPCH_IO_CONCURRENCY=16 \
    TPCH_S3_ENDPOINT=http://minio:9000 TPCH_S3_REGION=us-east-1 \
    TPCH_S3_PATH_STYLE=1 \
    "$ROOT/tools/tpch/compare.sh" --report \
      --queries benchmarks/tpch/cases/sf1-minio.txt \
      --rustdb-root "$uri" 1 || compare_status=$?
  capture_tpch minio || capture_status=$?
  [[ $compare_status -eq 0 ]] || return "$compare_status"
  return "$capture_status"
}

run_step preflight preflight
CURRENT_STEP=environment-setup
TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/rustdb-beta-acceptance.XXXXXX")

run_step orbstack-all "$ROOT/scripts/ci/orbstack.sh" all
run_step beta2-lifecycle run_beta2_lifecycle
run_step tpch-sf1-local run_tpch_local
run_step tpch-sf1-minio run_tpch_minio
run_step minio-fixture-verify verify_minio_fixture before
run_step runner-build build_runner
RUSTDB_BUILD_ID=$(<"$TEMP_ROOT/runner-build-id")

run_step local-2g run_external local-2g local-nvme 2147483648 \
  "$OUTPUT/local-query.sql" "$OUTPUT/local-fixture-manifest.json" \
  "$RUSTDB_BETA_LOCAL_FORMAT" "$RUSTDB_BETA_LOCAL_FIXTURE"
run_step local-4g run_external local-4g local-nvme 4294967296 \
  "$OUTPUT/local-query.sql" "$OUTPUT/local-fixture-manifest.json" \
  "$RUSTDB_BETA_LOCAL_FORMAT" "$RUSTDB_BETA_LOCAL_FIXTURE"
run_step local-fixture-reverify verify_local_fixture
run_step minio-2g run_external minio-2g minio 2147483648 \
  "$OUTPUT/minio-query.sql" "$OUTPUT/minio-fixture-manifest.json" \
  "$RUSTDB_BETA_MINIO_FORMAT"
run_step minio-4g run_external minio-4g minio 4294967296 \
  "$OUTPUT/minio-query.sql" "$OUTPUT/minio-fixture-manifest.json" \
  "$RUSTDB_BETA_MINIO_FORMAT"
run_step minio-fixture-reverify verify_minio_fixture after
run_step clickbench run_clickbench

CURRENT_STEP=complete
echo "Beta acceptance evidence: $OUTPUT/evidence.json"
