#!/bin/sh
set -eu

package=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
prefix=${RUSTDB_PREFIX:-/usr/local}

is_zh() {
  language=${RUSTDB_LANG:-${LC_ALL:-${LC_MESSAGES:-${LANG:-en}}}}
  case "$language" in zh*|ZH*) return 0 ;; *) return 1 ;; esac
}

say() {
  if is_zh; then printf '%s\n' "$2"; else printf '%s\n' "$1"; fi
}

usage() {
  cat <<'EOF'
usage: ./install.sh [--prefix DIR]
用法： ./install.sh [--prefix 目录]

Default prefix / 默认安装前缀: /usr/local
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

for file in \
  LICENSE README.md README.zh-CN.md SHA256SUMS VERSION \
  bin/rustdb docs/CLI.md docs/CLI.zh-CN.md \
  docs/INSTALL.md docs/INSTALL.zh-CN.md \
  docs/http-shell.md docs/http-shell.zh-CN.md \
  docs/operator-guide.md docs/operator-guide.zh-CN.md \
  docs/diagnostics.md docs/diagnostics.zh-CN.md \
  docs/native-import.md docs/native-repair.md docs/compatibility.md \
  docs/troubleshooting.md docs/migration-v0.5.md docs/migration-v1-beta.md \
  docs/parquet-pruning.md docs/s3.md \
  packaging/config/rustdb.example.toml \
  docs/openapi-v2.yaml RELEASE-NOTES.md \
  uninstall.sh
do
  [ -f "$package/$file" ] || { echo "incomplete package / 发行包不完整: $file" >&2; exit 1; }
done
"$package/bin/rustdb" --version >/dev/null

root="$destdir$prefix"
doc="$root/share/doc/rustdb"
state="$root/share/rustdb"
install -d "$root/bin" "$doc/docs" "$doc/packaging/config" "$state"

# Remove files installed by the Beta 1 layout before writing the Beta 2 tree.
for legacy in \
  CLI.md CLI.zh-CN.md INSTALL.md INSTALL.zh-CN.md \
  HTTP-SHELL.md HTTP-SHELL.zh-CN.md OPERATOR-GUIDE.md OPERATOR-GUIDE.zh-CN.md \
  DIAGNOSTICS.md DIAGNOSTICS.zh-CN.md NATIVE-IMPORT.md NATIVE-REPAIR.md \
  COMPATIBILITY.md openapi-v1.yaml openapi-v2.yaml
do
  rm -f "$doc/$legacy"
done
install -m 0755 "$package/bin/rustdb" "$root/bin/rustdb"
install -m 0755 "$package/uninstall.sh" "$state/uninstall.sh"
install -m 0644 "$package/VERSION" "$state/VERSION"
install -m 0644 "$package/LICENSE" "$doc/LICENSE"
install -m 0644 "$package/README.md" "$doc/README.md"
install -m 0644 "$package/README.zh-CN.md" "$doc/README.zh-CN.md"
install -m 0644 "$package/SHA256SUMS" "$doc/SHA256SUMS"
install -m 0644 "$package/docs/CLI.md" "$doc/docs/CLI.md"
install -m 0644 "$package/docs/CLI.zh-CN.md" "$doc/docs/CLI.zh-CN.md"
install -m 0644 "$package/docs/INSTALL.md" "$doc/docs/INSTALL.md"
install -m 0644 "$package/docs/INSTALL.zh-CN.md" "$doc/docs/INSTALL.zh-CN.md"
for file in \
  http-shell.md http-shell.zh-CN.md \
  operator-guide.md operator-guide.zh-CN.md \
  diagnostics.md diagnostics.zh-CN.md \
  native-import.md native-repair.md compatibility.md troubleshooting.md \
  migration-v0.5.md migration-v1-beta.md parquet-pruning.md s3.md \
  openapi-v2.yaml
do
  install -m 0644 "$package/docs/$file" "$doc/docs/$file"
done
install -m 0644 "$package/packaging/config/rustdb.example.toml" \
  "$doc/packaging/config/rustdb.example.toml"
install -m 0644 "$package/RELEASE-NOTES.md" "$doc/RELEASE-NOTES.md"

say \
  "RustDB installed in $prefix. Run: $prefix/bin/rustdb --help" \
  "RustDB 已安装到 ${prefix}。运行：${prefix}/bin/rustdb --help-zh"
