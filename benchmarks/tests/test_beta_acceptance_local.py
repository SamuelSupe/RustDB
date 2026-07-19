from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import beta_acceptance_local as local  # noqa: E402


class BetaAcceptanceLocalTests(unittest.TestCase):
    def test_rejects_a_changed_local_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "fixture"
            root.mkdir()
            (root / "a.parquet").write_bytes(b"abc")
            files, total, digest = local.inventory(root)
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema": "rustdb-beta-local-fixture-v1",
                        "source_root": str(root.resolve()),
                        "files": files,
                        "total_bytes": total,
                        "inventory_sha256": digest,
                    }
                ),
                encoding="utf-8",
            )
            output = Path(temporary) / "verification.json"
            self.assertEqual(local.verify(manifest, root, output)["files"], 1)

            (root / "b.parquet").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "inventory changed"):
                local.verify(manifest, root, output)


if __name__ == "__main__":
    unittest.main()
