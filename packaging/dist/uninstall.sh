#!/bin/sh
set -eu

prefix=${RUSTDB_PREFIX:-/usr/local}
dry_run=0

is_zh() {
  language=${RUSTDB_LANG:-${LC_ALL:-${LC_MESSAGES:-${LANG:-en}}}}
  case "$language" in zh*|ZH*) return 0 ;; *) return 1 ;; esac
}

say() {
  if is_zh; then printf '%s\n' "$2"; else printf '%s\n' "$1"; fi
}

usage() {
  cat <<'EOF'
usage: ./uninstall.sh [--prefix DIR] [--dry-run]
用法： ./uninstall.sh [--prefix 目录] [--dry-run]

Environment / 环境变量: RUSTDB_PREFIX, DESTDIR, RUSTDB_LANG
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prefix)
      [ "$#" -ge 2 ] || { usage >&2; exit 2; }
      prefix=$2
      shift 2
      ;;
    --dry-run)
      dry_run=1
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

case "$prefix" in /*) ;; *) echo "prefix must be absolute / 安装前缀必须是绝对路径" >&2; exit 2 ;; esac
case "$prefix" in *'/../'*|*/..|*'/./'*|*/.) echo "prefix must be normalized / 安装前缀不能包含 . 或 .." >&2; exit 2 ;; esac
destdir=${DESTDIR:-}
case "$destdir" in ''|/*) ;; *) echo "DESTDIR must be absolute / DESTDIR 必须是绝对路径" >&2; exit 2 ;; esac
case "$destdir" in *'/../'*|*/..|*'/./'*|*/.) echo "DESTDIR must be normalized / DESTDIR 不能包含 . 或 .." >&2; exit 2 ;; esac
root="$destdir$prefix"
found=0

remove_file() {
  path="$root/$1"
  [ -e "$path" ] || return 0
  found=1
  if [ "$dry_run" -eq 1 ]; then
    printf '%s\n' "$path"
  else
    rm -f "$path"
  fi
}

for file in \
  bin/rustdb \
  share/doc/rustdb/LICENSE \
  share/doc/rustdb/README.md \
  share/doc/rustdb/README.zh-CN.md \
  share/doc/rustdb/SHA256SUMS \
  share/doc/rustdb/CLI.md \
  share/doc/rustdb/CLI.zh-CN.md \
  share/doc/rustdb/INSTALL.md \
  share/doc/rustdb/INSTALL.zh-CN.md \
  share/doc/rustdb/HTTP-SHELL.md \
  share/doc/rustdb/HTTP-SHELL.zh-CN.md \
  share/doc/rustdb/openapi-v1.yaml \
  share/rustdb/VERSION \
  share/rustdb/uninstall.sh
do
  remove_file "$file"
done

if [ "$dry_run" -eq 0 ]; then
  rmdir "$root/share/doc/rustdb" 2>/dev/null || true
  rmdir "$root/share/rustdb" 2>/dev/null || true
fi
if [ "$found" -eq 1 ]; then
  say "RustDB removed from $prefix." "RustDB 已从 ${prefix} 卸载。"
else
  say "No RustDB distribution files found in $prefix." "在 ${prefix} 中未发现 RustDB 发行文件。"
fi
