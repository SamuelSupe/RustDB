#!/bin/sh
set -eu

workspace=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
exec "$workspace/tools/tpch/compare_query.sh" "$@"
