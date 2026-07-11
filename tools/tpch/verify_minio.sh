#!/bin/sh

set -eu

. "$(dirname -- "$0")/common.sh"

if [ "$#" -ne 2 ]; then
  tpch_die "usage: $0 LOCAL_DATASET_ROOT s3://BUCKET/PREFIX"
fi

local_argument=$1
remote_uri=$2
case "$local_argument" in
  /*|../*|*/../*|*/..) tpch_die "local dataset root must be workspace-relative" ;;
esac
case "$remote_uri" in
  s3://*/*) remote_path=${remote_uri#s3://} ;;
  *) tpch_die "remote dataset root must be s3://BUCKET/PREFIX" ;;
esac
case "/$remote_path/" in
  *'/../'*|*'/./'*) tpch_die "remote dataset root contains an unsafe path component" ;;
esac
case "$remote_path" in
  *[!A-Za-z0-9._/-]*) tpch_die "remote dataset root contains an unsupported character" ;;
esac

local_root=$TPCH_ROOT/$local_argument
tpch_verify_dataset "$local_root"
[ -f "$local_root/manifest.json" ] || tpch_die "missing dataset metadata: $local_root/manifest.json"

tpch_require cmp
tpch_require docker
mkdir -p "$TPCH_ROOT/data"
work=$(mktemp -d "$TPCH_ROOT/data/.tpch-minio-verify.XXXXXX")
trap 'rm -rf "$work"' EXIT HUP INT TERM

fetch_object() {
  object=$1
  destination=$2
  docker compose --project-directory "$TPCH_ROOT" run --rm --no-deps --no-TTY \
    --env "TPCH_OBJECT=$remote_path/$object" \
    --entrypoint /bin/sh minio-init -ec '
      mc alias set rustdb http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
      mc cat "rustdb/$TPCH_OBJECT"
    ' > "$destination"
}

fetch_object manifest.sha256 "$work/manifest.sha256"
fetch_object manifest.json "$work/manifest.json"
cmp "$local_root/manifest.sha256" "$work/manifest.sha256" >/dev/null ||
  tpch_die "remote manifest.sha256 differs from the local dataset"
cmp "$local_root/manifest.json" "$work/manifest.json" >/dev/null ||
  tpch_die "remote manifest.json differs from the local dataset"

printf 'MinIO dataset manifest matches %s\n' "$local_argument"
