#!/bin/sh
set -eu

[ "$#" -eq 1 ] || {
  echo "usage / 用法: scripts/dist/check.sh ARCHIVE.tar.gz" >&2
  exit 2
}
archive=$1
[ -f "$archive" ] || { echo "archive not found / 找不到产物: $archive" >&2; exit 1; }
[ -f "$archive.sha256" ] || { echo "missing checksum / 缺少校验文件: $archive.sha256" >&2; exit 1; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

expected=$(awk 'NR == 1 {print $1}' "$archive.sha256")
actual=$(sha256 "$archive")
[ "$expected" = "$actual" ] || { echo "archive checksum mismatch / 产物校验失败" >&2; exit 1; }

temporary=$(mktemp -d "${TMPDIR:-/tmp}/rustdb-dist-check.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM
listing="$temporary/archive-entries"
tar -tzf "$archive" >"$listing"
archive_root=""
while IFS= read -r entry; do
  normalized=${entry%/}
  case "$normalized" in
    ''|/*|../*|*/../*|*/..|*'/./'*|*/.)
      echo "unsafe archive path / 不安全的产物路径: $entry" >&2
      exit 1
      ;;
  esac
  first=${normalized%%/*}
  case "$first" in rustdb-v*) ;; *) echo "invalid archive root / 非法产物根目录" >&2; exit 1 ;; esac
  if [ -z "$archive_root" ]; then
    archive_root=$first
  elif [ "$archive_root" != "$first" ]; then
    echo "multiple archive roots / 产物包含多个根目录" >&2
    exit 1
  fi
done <"$listing"
[ -n "$archive_root" ] || { echo "empty archive / 空发行包" >&2; exit 1; }
tar -xzf "$archive" -C "$temporary"
package="$temporary/$archive_root"
[ -d "$package" ] || { echo "invalid archive root / 非法产物根目录" >&2; exit 1; }

for file in \
  LICENSE README.md README.zh-CN.md SHA256SUMS VERSION \
  bin/rustdb docs/CLI.md docs/CLI.zh-CN.md \
  docs/INSTALL.md docs/INSTALL.zh-CN.md install.sh uninstall.sh
do
  [ -f "$package/$file" ] || { echo "missing package file / 缺少文件: $file" >&2; exit 1; }
done
[ -x "$package/bin/rustdb" ]
[ -x "$package/install.sh" ]
[ -x "$package/uninstall.sh" ]

while read -r digest file; do
  [ -n "$digest" ] || continue
  case "$file" in ''|/*|../*|*/../*|*/..) echo "unsafe checksum path / 不安全的校验路径: $file" >&2; exit 1 ;; esac
  [ "$digest" = "$(sha256 "$package/$file")" ] || {
    echo "package checksum mismatch / 包内校验失败: $file" >&2
    exit 1
  }
done <"$package/SHA256SUMS"

if [ "${RUSTDB_DIST_SKIP_EXEC:-0}" != "1" ]; then
  "$package/bin/rustdb" --version >/dev/null
  "$package/bin/rustdb" --help | grep -q 'Usage:'
  "$package/bin/rustdb" --help-zh | grep -q '使用方法'

  install_root="$temporary/install-root"
  DESTDIR="$install_root" RUSTDB_LANG=en "$package/install.sh" --prefix /usr >/dev/null
  "$install_root/usr/bin/rustdb" --version >/dev/null
  [ -f "$install_root/usr/share/doc/rustdb/README.zh-CN.md" ]
  DESTDIR="$install_root" RUSTDB_LANG=zh-CN \
    "$package/uninstall.sh" --prefix /usr >/dev/null
  [ ! -e "$install_root/usr/bin/rustdb" ]
  [ ! -e "$install_root/usr/share/doc/rustdb" ]
fi

echo "dist check passed / 发行包校验通过: $(basename "$archive")"
