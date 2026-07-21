#!/usr/bin/env python3
"""Fail when a packaged Markdown document links to a missing local path."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


LINK = re.compile(r"\]\(([^)]+)\)")


def target(raw: str) -> str:
    value = raw.strip()
    if value.startswith("<") and ">" in value:
        return value[1 : value.index(">")]
    return value.split(maxsplit=1)[0] if value else ""


def main() -> int:
    root = Path(sys.argv[1]).resolve()
    missing: list[str] = []
    for document in sorted(root.rglob("*.md")):
        for match in LINK.finditer(document.read_text(encoding="utf-8")):
            link = target(match.group(1))
            parsed = urlsplit(link)
            if not link or link.startswith("#") or parsed.scheme or parsed.netloc:
                continue
            path = unquote(parsed.path)
            if not path or path.startswith("/"):
                continue
            resolved = (document.parent / path).resolve()
            try:
                resolved.relative_to(root)
            except ValueError:
                missing.append(f"{document.relative_to(root)} -> {link} (escapes package)")
                continue
            if not resolved.exists():
                missing.append(f"{document.relative_to(root)} -> {link}")
    if missing:
        print("missing relative Markdown links / 缺少相对文档链接:", file=sys.stderr)
        for item in missing:
            print(f"  {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_links.py PACKAGE_ROOT")
    raise SystemExit(main())
