#!/bin/sh
set -eu

workspace=${RUSTDB_WORKSPACE:-$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)}
wait_seconds=${RUSTDB_SPILL_REPLAY_WAIT_SECONDS:-30}

case "$wait_seconds" in
  ''|*[!0-9]*)
    echo "RUSTDB_SPILL_REPLAY_WAIT_SECONDS must be a non-negative integer" >&2
    exit 2
    ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "OrbStack's Docker command is required" >&2
  exit 2
fi
docker compose version >/dev/null

probe_root=$(mktemp -d "${TMPDIR:-/tmp}/rustdb-spill-fresh-mount.XXXXXX")
keep_probe=${RUSTDB_KEEP_SPILL_REPLAY_PROBE:-0}
cleanup() {
  if [ "$keep_probe" = "1" ]; then
    echo "probe directory preserved at $probe_root" >&2
  else
    rm -rf "$probe_root"
  fi
}
trap cleanup EXIT HUP INT TERM

cd "$workspace"
echo "+ container A: spill and clean $probe_root"
docker compose run --rm --no-deps \
  --env RUSTDB_SPILL_REPLAY_ROOT=/probe \
  --volume "$probe_root:/probe" \
  dev cargo test --locked --test spill_fresh_mount fresh_mount_cleanup_probe -- \
    --ignored --exact --nocapture

echo "+ waiting ${wait_seconds}s for delayed shared-filesystem writeback"
sleep "$wait_seconds"

echo "+ container B: fresh-mount verification"
docker compose run --rm --no-deps \
  --volume "$probe_root:/probe:ro" \
  --entrypoint sh \
  dev -eu -c '
    echo "probe filesystem: $(stat -f -c %T /probe)"
    residue=$(find /probe/spill -mindepth 1 -print -quit 2>/dev/null || true)
    if [ -n "$residue" ]; then
      echo "spill residue reappeared after a fresh mount:" >&2
      find /probe/spill -mindepth 1 -print >&2
      exit 1
    fi
  '

echo "spill fresh-mount cleanup regression passed"
