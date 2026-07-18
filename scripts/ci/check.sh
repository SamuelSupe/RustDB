#!/bin/sh
set -eu

workspace=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$workspace"

run() {
  echo "+ $*"
  "$@"
}

lint() {
  run cargo fmt --all -- --check
  run cargo clippy --locked --all-targets -- -D warnings
}

require_minio() {
  if [ "${RUSTDB_REQUIRE_MINIO:-}" != "1" ]; then
    echo "RUSTDB_REQUIRE_MINIO=1 is required for the integration test gate" >&2
    exit 2
  fi
  if [ -z "${RUSTDB_MINIO_ENDPOINT:-}" ]; then
    echo "RUSTDB_MINIO_ENDPOINT is required for the integration test gate" >&2
    exit 2
  fi
}

test_with_minio() {
  require_minio
  test_jobs=${RUSTDB_TEST_JOBS:-1}
  run cargo test --locked --all-targets --jobs "$test_jobs"
}

hosted_test_with_minio() {
  require_minio
  test_jobs=${RUSTDB_TEST_JOBS:-1}
  run cargo test --locked --lib --bins \
    --test csv_validation \
    --test hive_parquet \
    --test parquet_query \
    --test query_engine \
    --test runtime_filter_pruning \
    --test s3_deep_pruning \
    --test s3_query \
    --test v08_remote \
    --jobs "$test_jobs" -- \
    --skip execution::tests::aggregate_spills_and_join_completes_under_small_memory_limit
}

test_portable() {
  tool_tests
  test_jobs=${RUSTDB_TEST_JOBS:-1}
  run cargo test --locked --all-targets --jobs "$test_jobs"
}

v08_acceptance() {
  require_minio
  test_jobs=${RUSTDB_TEST_JOBS:-1}
  run cargo test --locked --lib storage::native:: --jobs "$test_jobs"
  run cargo test --locked --lib catalog::tests --jobs "$test_jobs"
  run cargo test --locked --lib command:: --jobs "$test_jobs"
  run cargo test --locked --lib engine::transaction::tests --jobs "$test_jobs"
  run cargo test --locked --lib engine::tests::native --jobs "$test_jobs"
  run cargo test --locked --lib engine::tests::copy --jobs "$test_jobs"
  run cargo test --locked --lib engine::tests::maintenance --jobs "$test_jobs"
  run cargo test --locked --lib engine::tests::refresh_table_canonicalizes_the_default_schema_only --jobs "$test_jobs"
  run cargo test --locked --lib engine::copy_sink:: --jobs "$test_jobs"
  run cargo test --locked --lib storage::remote_backup::tests --jobs "$test_jobs"
  run cargo test --locked --lib storage::remote_temp::tests --jobs "$test_jobs"
  run cargo test --locked --lib runtime::compute::tests::cancellation_waits_for_protected_async_cleanup --jobs "$test_jobs"
  run cargo test --locked --lib runtime::task_group::tests --jobs "$test_jobs"
  run cargo test --locked --lib sql::correctness_tests --jobs "$test_jobs"
  run cargo test --locked --lib sql::join_tests --jobs "$test_jobs"
  run cargo test --locked --lib sql::timezone_tests --jobs "$test_jobs"
  run cargo test --locked --lib execution::window::tests::executes_bounded_rows_range_and_groups_frames --jobs "$test_jobs"
  run cargo test --locked --lib prepared::tests::binds_v08_time_uuid_and_interval_parameter_values --jobs "$test_jobs"
  run cargo test --locked --test v08_remote --jobs "$test_jobs"
}

check_portable() {
  run cargo check --locked --all-targets
}

tool_tests() {
  run python3 -m unittest discover -s benchmarks/tests -p 'test_*.py'
  run python3 tools/tpch/test_canonicalize.py
  run python3 tools/tpch/test_harness.py
}

release_build() {
  release_jobs=${RUSTDB_RELEASE_BUILD_JOBS:-1}
  run cargo build --locked --release --all-targets --jobs "$release_jobs"
}

release_cli_build() {
  release_jobs=${RUSTDB_RELEASE_BUILD_JOBS:-1}
  run cargo build --locked --release --bin rustdb --jobs "$release_jobs"
}

dist_build() {
  release_jobs=${RUSTDB_RELEASE_BUILD_JOBS:-1}
  run cargo build --locked --release --bin rustdb --jobs "$release_jobs"
  run scripts/dist/package.sh --binary target/release/rustdb --output dist --check
}

usage() {
  cat >&2 <<'EOF'
usage: scripts/ci/check.sh lint|test|minio-test|hosted-test|portable|v08|check|release|release-cli|dist|all

  lint         formatting and strict Clippy
  test         all-target tests; requires a live configured MinIO
  minio-test   alias for test
  hosted-test  representative live-MinIO tests without dedicated Spill suites
  portable     all-target tests without requiring MinIO (S3 tests may skip)
  v08          one focused v0.8 reliability pass with live MinIO
  check        compile every target without running tests
  release      portable release build of every target
  release-cli  release build of the distributed rustdb CLI
  dist         build and validate the native CLI distribution archive
  all          lint, live-MinIO tests, and release build
EOF
  exit 2
}

case "${1:-}" in
  lint)
    lint
    ;;
  test|minio-test)
    tool_tests
    test_with_minio
    ;;
  hosted-test)
    tool_tests
    hosted_test_with_minio
    ;;
  portable)
    test_portable
    ;;
  v08)
    v08_acceptance
    ;;
  check)
    check_portable
    ;;
  release)
    release_build
    ;;
  release-cli)
    release_cli_build
    ;;
  dist)
    dist_build
    ;;
  all)
    lint
    tool_tests
    test_with_minio
    release_build
    ;;
  *)
    usage
    ;;
esac
