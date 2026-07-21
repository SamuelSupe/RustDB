#!/bin/sh
set -eu

stage=${1:-all}
workspace=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$workspace"

# Keep the release gate isolated from an already running developer MinIO.
export RUSTDB_MINIO_HOST_PORT=${RUSTDB_MINIO_HOST_PORT:-19000}
export RUSTDB_MINIO_CONSOLE_PORT=${RUSTDB_MINIO_CONSOLE_PORT:-19001}

if ! command -v docker >/dev/null 2>&1; then
  echo "OrbStack's Docker command is required" >&2
  exit 2
fi
docker compose version >/dev/null

docker compose up -d --wait --wait-timeout 60 minio
docker compose rm -f -s minio-init >/dev/null 2>&1 || true
docker compose up --force-recreate minio-init
docker compose run --rm --no-deps dev ./scripts/ci/check.sh "$stage"
