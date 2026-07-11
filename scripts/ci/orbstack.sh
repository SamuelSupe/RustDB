#!/bin/sh
set -eu

stage=${1:-all}
workspace=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$workspace"

if ! command -v docker >/dev/null 2>&1; then
  echo "OrbStack's Docker command is required" >&2
  exit 2
fi
docker compose version >/dev/null

docker compose up -d --wait --wait-timeout 60 minio
docker compose rm -f -s minio-init >/dev/null 2>&1 || true
docker compose up --force-recreate minio-init
docker compose run --rm --no-deps dev ./scripts/ci/check.sh "$stage"
