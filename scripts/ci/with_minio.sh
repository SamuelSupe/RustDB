#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "usage: scripts/ci/with_minio.sh COMMAND [ARG ...]" >&2
  exit 2
fi

workspace=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$workspace"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker with Compose v2 is required to start the MinIO test service" >&2
  exit 2
fi
docker compose version >/dev/null

# Use an isolated project so cleanup cannot remove a developer's normal
# RustDB Compose services or persistent MinIO volume.
COMPOSE_PROJECT_NAME=${RUSTDB_CI_COMPOSE_PROJECT:-rustdb-ci-$$}
export COMPOSE_PROJECT_NAME

cleanup() {
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

docker compose up -d --wait --wait-timeout 60 minio
docker compose run --rm --no-deps minio-init

AWS_ACCESS_KEY_ID=rustdb-test
AWS_SECRET_ACCESS_KEY=rustdb-test-secret
AWS_REGION=us-east-1
AWS_ENDPOINT_URL=http://127.0.0.1:9000
RUSTDB_MINIO_ENDPOINT=http://127.0.0.1:9000
RUSTDB_REQUIRE_MINIO=1
NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,$NO_PROXY}"
no_proxy="127.0.0.1,localhost${no_proxy:+,$no_proxy}"
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_ENDPOINT_URL
export RUSTDB_MINIO_ENDPOINT RUSTDB_REQUIRE_MINIO NO_PROXY no_proxy

"$@"
