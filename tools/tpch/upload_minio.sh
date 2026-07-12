#!/bin/sh

set -eu

. "$(dirname -- "$0")/common.sh"

if [ "$#" -gt 1 ]; then
  tpch_die "usage: $0 [SCALE_FACTOR]"
fi

scale=${1:-1}
tpch_validate_scale "$scale"
tpch_require docker
tpch_require python3

relative=$(tpch_dataset_relative "$scale")
dataset="$TPCH_ROOT/$relative"
[ -d "$dataset" ] || tpch_die "dataset not found: $relative; run tools/tpch/generate.sh $scale first"
tpch_verify_dataset "$dataset" || tpch_die "dataset is incomplete: $relative"

destination="tpch-sf$scale"

docker compose --project-directory "$TPCH_ROOT" up -d --wait --wait-timeout 60 minio
docker compose --project-directory "$TPCH_ROOT" run --rm --no-deps minio-init
docker compose --project-directory "$TPCH_ROOT" run --rm --no-deps --no-TTY \
  --volume "$dataset:/dataset:ro" \
  --env "TPCH_DESTINATION=$destination" \
  --entrypoint /bin/sh minio-init -ec '
    mc alias set rustdb http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    remote="rustdb/rustdb-tests/$TPCH_DESTINATION"
    mc rm --recursive --force "$remote" >/dev/null 2>&1 || true
    for table in customer lineitem nation orders part partsupp region supplier; do
      test -f "/dataset/$table/part-00000.parquet"
      mc mirror --quiet --overwrite "/dataset/$table" "$remote/$table" >/dev/null
      mc stat "$remote/$table/part-00000.parquet" >/dev/null
    done
    mc cp --quiet /dataset/manifest.json /dataset/manifest.sha256 "$remote/" >/dev/null
    mc stat "$remote/manifest.sha256" >/dev/null
  '

remote_uri=s3://rustdb-tests/$destination
tpch_validate_s3_uri "$remote_uri"
"$TPCH_TOOLS/verify_minio.sh" "$relative" "$remote_uri"
printf '%s\n' "$remote_uri"
