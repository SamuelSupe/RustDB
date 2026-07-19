#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
binary="$root/target/release/rustdb"
output="$root/dist"
target=""
check=0

usage() {
  cat <<'EOF'
usage: scripts/dist/package.sh [options]
用法： scripts/dist/package.sh [选项]

  --binary PATH   prebuilt rustdb binary / 已构建的 rustdb 二进制
  --output DIR    output directory / 输出目录（默认 dist）
  --target NAME   archive target label / 产物目标标签
  --check         validate archive and install cycle / 校验产物与安装卸载
  -h, --help      show this help / 显示帮助
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --binary)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      binary=$2
      shift 2
      ;;
    --output)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      output=$2
      shift 2
      ;;
    --target)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      target=$2
      shift 2
      ;;
    --check)
      check=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option / 未知选项: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

version=$(sed -n 's/^version = "\([^"]*\)"$/\1/p' "$root/Cargo.toml" | head -n 1)
[ -n "$version" ] || { echo "cannot read package version / 无法读取版本" >&2; exit 1; }
[ -x "$binary" ] || { echo "binary is not executable / 二进制不可执行: $binary" >&2; exit 1; }

if [ -z "$target" ]; then
  case "$(uname -s)" in
    Linux) os=linux ;;
    Darwin) os=macos ;;
    *) echo "unsupported operating system / 不支持的操作系统: $(uname -s)" >&2; exit 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) arch=x86_64 ;;
    arm64|aarch64) arch=aarch64 ;;
    *) echo "unsupported architecture / 不支持的架构: $(uname -m)" >&2; exit 1 ;;
  esac
  target="$os-$arch"
fi
case "$target" in
  *[!A-Za-z0-9._-]*|'') echo "invalid target label / 非法目标标签: $target" >&2; exit 1 ;;
esac

reported_version=$("$binary" --version)
case "$reported_version" in
  "rustdb $version") ;;
  *)
    echo "binary version mismatch / 二进制版本不匹配: $reported_version" >&2
    exit 1
    ;;
esac

mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
temporary=$(mktemp -d "${TMPDIR:-/tmp}/rustdb-dist.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

name="rustdb-v${version}-${target}"
stage="$temporary/$name"
mkdir -p "$stage/bin" "$stage/docs"
install -m 0755 "$binary" "$stage/bin/rustdb"
install -m 0755 "$root/packaging/dist/install.sh" "$stage/install.sh"
install -m 0755 "$root/packaging/dist/uninstall.sh" "$stage/uninstall.sh"
install -m 0644 "$root/LICENSE" "$stage/LICENSE"
install -m 0644 "$root/packaging/dist/README.md" "$stage/README.md"
install -m 0644 "$root/packaging/dist/README.zh-CN.md" "$stage/README.zh-CN.md"
install -m 0644 "$root/packaging/dist/CLI.md" "$stage/docs/CLI.md"
install -m 0644 "$root/packaging/dist/CLI.zh-CN.md" "$stage/docs/CLI.zh-CN.md"
install -m 0644 "$root/packaging/dist/INSTALL.md" "$stage/docs/INSTALL.md"
install -m 0644 "$root/packaging/dist/INSTALL.zh-CN.md" "$stage/docs/INSTALL.zh-CN.md"
install -m 0644 "$root/docs/http-shell.md" "$stage/docs/HTTP-SHELL.md"
install -m 0644 "$root/docs/http-shell.zh-CN.md" "$stage/docs/HTTP-SHELL.zh-CN.md"
install -m 0644 "$root/docs/openapi-v1.yaml" "$stage/docs/openapi-v1.yaml"
printf '%s\n' "$version" >"$stage/VERSION"

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

: >"$stage/SHA256SUMS"
for file in \
  LICENSE README.md README.zh-CN.md VERSION \
  bin/rustdb docs/CLI.md docs/CLI.zh-CN.md \
  docs/INSTALL.md docs/INSTALL.zh-CN.md \
  docs/HTTP-SHELL.md docs/HTTP-SHELL.zh-CN.md docs/openapi-v1.yaml \
  install.sh uninstall.sh
do
  digest=$(sha256 "$stage/$file")
  printf '%s  %s\n' "$digest" "$file" >>"$stage/SHA256SUMS"
done

epoch=${SOURCE_DATE_EPOCH:-}
if [ -z "$epoch" ]; then
  epoch=$(git -C "$root" log -1 --format=%ct 2>/dev/null || printf '0')
fi
archive="$output/$name.tar.gz"
python3 -B "$root/scripts/dist/archive.py" "$stage" "$archive" --epoch "$epoch"
archive_digest=$(sha256 "$archive")
printf '%s  %s\n' "$archive_digest" "$(basename "$archive")" >"$archive.sha256"

if [ "$check" -eq 1 ]; then
  "$root/scripts/dist/check.sh" "$archive"
fi
printf '%s\n' "$archive"
