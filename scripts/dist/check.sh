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

sidecar_lines=$(awk 'NF { lines += 1 } END { print lines + 0 }' "$archive.sha256")
[ "$sidecar_lines" -eq 1 ] || {
  echo "checksum sidecar must contain exactly one entry / 校验文件必须仅包含一条记录" >&2
  exit 1
}
expected=$(awk 'NF {print $1}' "$archive.sha256")
expected_name=$(awk 'NF {print $2}' "$archive.sha256")
[ "${#expected}" -eq 64 ] || { echo "invalid archive checksum / 产物校验值非法" >&2; exit 1; }
case "$expected" in *[!0-9A-Fa-f]*) echo "invalid archive checksum / 产物校验值非法" >&2; exit 1 ;; esac
[ "$expected_name" = "$(basename "$archive")" ] || {
  echo "checksum filename mismatch / 校验文件名与产物不匹配" >&2
  exit 1
}
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
  docs/INSTALL.md docs/INSTALL.zh-CN.md \
  docs/HTTP-SHELL.md docs/HTTP-SHELL.zh-CN.md \
  docs/OPERATOR-GUIDE.md docs/OPERATOR-GUIDE.zh-CN.md \
  docs/DIAGNOSTICS.md docs/DIAGNOSTICS.zh-CN.md \
  docs/NATIVE-IMPORT.md docs/NATIVE-REPAIR.md docs/COMPATIBILITY.md \
  docs/openapi-v1.yaml RELEASE-NOTES.md \
  install.sh uninstall.sh
do
  [ -f "$package/$file" ] || { echo "missing package file / 缺少文件: $file" >&2; exit 1; }
done
[ -x "$package/bin/rustdb" ]
[ -x "$package/install.sh" ]
[ -x "$package/uninstall.sh" ]

checksum_entries=$(awk 'NF { entries += 1 } END { print entries + 0 }' "$package/SHA256SUMS")
[ "$checksum_entries" -eq 22 ] || {
  echo "incomplete package checksum manifest / 包内校验清单不完整" >&2
  exit 1
}
awk 'NF != 2 { exit 1 }' "$package/SHA256SUMS" || {
  echo "invalid package checksum manifest / 包内校验清单非法" >&2
  exit 1
}
while read -r digest file; do
  [ -n "$digest" ] || continue
  [ "${#digest}" -eq 64 ] || { echo "invalid package checksum / 包内校验值非法: $file" >&2; exit 1; }
  case "$digest" in *[!0-9A-Fa-f]*) echo "invalid package checksum / 包内校验值非法: $file" >&2; exit 1 ;; esac
  case "$file" in ''|/*|../*|*/../*|*/..) echo "unsafe checksum path / 不安全的校验路径: $file" >&2; exit 1 ;; esac
  [ "$digest" = "$(sha256 "$package/$file")" ] || {
    echo "package checksum mismatch / 包内校验失败: $file" >&2
    exit 1
  }
done <"$package/SHA256SUMS"
for file in \
  LICENSE README.md README.zh-CN.md VERSION \
  bin/rustdb docs/CLI.md docs/CLI.zh-CN.md \
  docs/INSTALL.md docs/INSTALL.zh-CN.md \
  docs/HTTP-SHELL.md docs/HTTP-SHELL.zh-CN.md \
  docs/OPERATOR-GUIDE.md docs/OPERATOR-GUIDE.zh-CN.md \
  docs/DIAGNOSTICS.md docs/DIAGNOSTICS.zh-CN.md \
  docs/NATIVE-IMPORT.md docs/NATIVE-REPAIR.md docs/COMPATIBILITY.md \
  docs/openapi-v1.yaml RELEASE-NOTES.md \
  install.sh uninstall.sh
do
  matches=$(awk -v file="$file" '$2 == file { matches += 1 } END { print matches + 0 }' "$package/SHA256SUMS")
  [ "$matches" -eq 1 ] || {
    echo "missing or duplicate package checksum / 包内校验记录缺失或重复: $file" >&2
    exit 1
  }
done

if [ "${RUSTDB_DIST_SKIP_EXEC:-0}" != "1" ]; then
  "$package/bin/rustdb" --version >/dev/null
  "$package/bin/rustdb" --help | grep -q 'Usage:'
  "$package/bin/rustdb" --help-zh | grep -q '使用方法'

  install_root="$temporary/install-root"
  DESTDIR="$install_root" RUSTDB_LANG=en "$package/install.sh" --prefix /usr >/dev/null
  "$install_root/usr/bin/rustdb" --version >/dev/null
  [ -f "$install_root/usr/share/doc/rustdb/README.zh-CN.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/HTTP-SHELL.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/HTTP-SHELL.zh-CN.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/OPERATOR-GUIDE.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/OPERATOR-GUIDE.zh-CN.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/DIAGNOSTICS.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/DIAGNOSTICS.zh-CN.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/NATIVE-IMPORT.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/NATIVE-REPAIR.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/COMPATIBILITY.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/RELEASE-NOTES.md" ]
  [ -f "$install_root/usr/share/doc/rustdb/openapi-v1.yaml" ]
  DESTDIR="$install_root" RUSTDB_LANG=zh-CN \
    "$package/uninstall.sh" --prefix /usr >/dev/null
  [ ! -e "$install_root/usr/bin/rustdb" ]
  [ ! -e "$install_root/usr/share/doc/rustdb" ]
fi

echo "dist check passed / 发行包校验通过: $(basename "$archive")"
