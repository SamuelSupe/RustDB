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
  share/doc/rustdb/docs/CLI.md \
  share/doc/rustdb/docs/CLI.zh-CN.md \
  share/doc/rustdb/docs/INSTALL.md \
  share/doc/rustdb/docs/INSTALL.zh-CN.md \
  share/doc/rustdb/docs/http-shell.md \
  share/doc/rustdb/docs/http-shell.zh-CN.md \
  share/doc/rustdb/docs/operator-guide.md \
  share/doc/rustdb/docs/operator-guide.zh-CN.md \
  share/doc/rustdb/docs/diagnostics.md \
  share/doc/rustdb/docs/diagnostics.zh-CN.md \
  share/doc/rustdb/docs/native-import.md \
  share/doc/rustdb/docs/native-repair.md \
  share/doc/rustdb/docs/compatibility.md \
  share/doc/rustdb/docs/troubleshooting.md \
  share/doc/rustdb/docs/migration-v0.5.md \
  share/doc/rustdb/docs/migration-v1-beta.md \
  share/doc/rustdb/docs/parquet-pruning.md \
  share/doc/rustdb/docs/s3.md \
  share/doc/rustdb/docs/openapi-v2.yaml \
  share/doc/rustdb/packaging/config/rustdb.example.toml \
  share/doc/rustdb/RELEASE-NOTES.md \
  share/doc/rustdb/CLI.md \
  share/doc/rustdb/CLI.zh-CN.md \
  share/doc/rustdb/INSTALL.md \
  share/doc/rustdb/INSTALL.zh-CN.md \
  share/doc/rustdb/HTTP-SHELL.md \
  share/doc/rustdb/HTTP-SHELL.zh-CN.md \
  share/doc/rustdb/OPERATOR-GUIDE.md \
  share/doc/rustdb/OPERATOR-GUIDE.zh-CN.md \
  share/doc/rustdb/DIAGNOSTICS.md \
  share/doc/rustdb/DIAGNOSTICS.zh-CN.md \
  share/doc/rustdb/NATIVE-IMPORT.md \
  share/doc/rustdb/NATIVE-REPAIR.md \
  share/doc/rustdb/COMPATIBILITY.md \
  share/doc/rustdb/openapi-v1.yaml \
  share/doc/rustdb/openapi-v2.yaml \
  share/rustdb/VERSION \
  share/rustdb/uninstall.sh
do
  remove_file "$file"
done

if [ "$dry_run" -eq 0 ]; then
  rmdir "$root/share/doc/rustdb/docs" 2>/dev/null || true
  rmdir "$root/share/doc/rustdb/packaging/config" 2>/dev/null || true
  rmdir "$root/share/doc/rustdb/packaging" 2>/dev/null || true
  rmdir "$root/share/doc/rustdb" 2>/dev/null || true
  rmdir "$root/share/rustdb" 2>/dev/null || true
fi
if [ "$found" -eq 1 ]; then
  say "RustDB removed from $prefix." "RustDB 已从 ${prefix} 卸载。"
else
  say "No RustDB distribution files found in $prefix." "在 ${prefix} 中未发现 RustDB 发行文件。"
fi
