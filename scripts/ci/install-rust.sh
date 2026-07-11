#!/bin/sh
set -eu

toolchain=${RUSTDB_RUST_TOOLCHAIN:-1.97.0}

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup is required to install Rust $toolchain" >&2
  exit 2
fi

rustup set profile minimal
rustup toolchain install "$toolchain" \
  --component clippy \
  --component rustfmt \
  --no-self-update
rustup run "$toolchain" rustc --version --verbose
