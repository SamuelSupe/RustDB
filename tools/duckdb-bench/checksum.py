from __future__ import annotations

import hashlib
import struct


MODE = "multiset-sha256-v2"
_PREFIX = b"rustdb-multiset-sha256-v2"
_MASK = (1 << 256) - 1


class MultisetChecksum:
    def __init__(self) -> None:
        self.count = 0
        self.total = 0
        self.xor = 0

    def update_encoded(self, encoded: bytes | bytearray) -> None:
        self.update_digest(hashlib.sha256(encoded).digest())

    def update_digest(self, digest: bytes) -> None:
        if len(digest) != 32:
            raise ValueError("row checksum must be a SHA-256 digest")
        number = int.from_bytes(digest, "big")
        self.total = (self.total + number) & _MASK
        self.xor ^= number
        self.count += 1

    def finish(self) -> str:
        digest = hashlib.sha256()
        digest.update(_PREFIX)
        digest.update(struct.pack("<Q", self.count))
        digest.update(self.total.to_bytes(32, "big"))
        digest.update(self.xor.to_bytes(32, "big"))
        return digest.hexdigest()
