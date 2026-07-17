#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
mode=orbstack
if [ "${1:-}" = "--native" ]; then
  mode=native
  shift
elif [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  cat <<'EOF'
usage: scripts/dist/build.sh [--native]
用法： scripts/dist/build.sh [--native]

Without flags, build and validate a Linux package in OrbStack.
默认在 OrbStack 中构建并校验当前架构的 Linux 发行包。

--native builds for the current native host (for example macOS arm64).
--native 在当前原生系统构建（例如 macOS arm64）。
EOF
  exit 0
fi
[ "$#" -eq 0 ] || { echo "unknown option / 未知选项: $1" >&2; exit 2; }

output=${RUSTDB_DIST_DIR:-"$root/dist"}
mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)

if [ "$mode" = native ]; then
  target_dir=${CARGO_TARGET_DIR:-"$root/target"}
  cargo build --locked --release --bin rustdb
  "$root/scripts/dist/package.sh" \
    --binary "$target_dir/release/rustdb" --output "$output" --check
  exit 0
fi

target_dir=${RUSTDB_DIST_TARGET_DIR:-/private/tmp/rustdb-dist-target}
mkdir -p "$target_dir"
target_dir=$(CDPATH= cd -- "$target_dir" && pwd)
docker compose -f "$root/compose.yaml" run --rm --no-deps -T \
  --volume "$target_dir:/rustdb-dist-target" \
  --env CARGO_TARGET_DIR=/rustdb-dist-target \
  --env CARGO_BUILD_JOBS="${RUSTDB_DIST_BUILD_JOBS:-2}" \
  dev cargo build --locked --release --bin rustdb
docker compose -f "$root/compose.yaml" run --rm --no-deps -T \
  --volume "$target_dir:/rustdb-dist-target:ro" \
  --volume "$output:/rustdb-dist-output" \
  dev ./scripts/dist/package.sh \
    --binary /rustdb-dist-target/release/rustdb \
    --output /rustdb-dist-output \
    --check
