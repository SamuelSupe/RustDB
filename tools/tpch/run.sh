#!/bin/sh

set -eu

tools=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
scale=${1:-0.01}

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [SCALE_FACTOR]" >&2
  exit 2
fi

"$tools/generate.sh" "$scale"
"$tools/compare.sh" "$scale"
