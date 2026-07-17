from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Callable


def common_storage_root(database: str, temp_directory: str) -> Path:
    database_parent = Path(database).resolve().parent
    temporary = Path(temp_directory).resolve()
    return Path(os.path.commonpath((database_parent, temporary)))


def storage_bytes(root: Path) -> int:
    total = 0
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            entries = list(os.scandir(current))
        except FileNotFoundError:
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
            except FileNotFoundError:
                continue
    return total


class StorageSampler:
    def __init__(
        self,
        root: Path,
        limit: int,
        interval_seconds: float = 0.002,
        on_exceeded: Callable[[], None] | None = None,
    ) -> None:
        self.root = root
        self.limit = limit
        self.interval_seconds = interval_seconds
        self.on_exceeded = on_exceeded
        self.baseline = storage_bytes(root)
        self.peak = self.baseline
        self._error: Exception | None = None
        self._exceeded = self.baseline > limit
        self._callback_called = False
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._sample, daemon=True)

    def start(self) -> None:
        self._worker.start()

    def stop(self) -> tuple[int, int, int]:
        self._stop.set()
        self._worker.join()
        final = self.sample_now()
        self._raise_error()
        return self.baseline, self.peak, final

    def sample_now(self) -> int:
        try:
            current = storage_bytes(self.root)
        except Exception as error:
            with self._lock:
                self._error = error
            raise
        callback = None
        with self._lock:
            self.peak = max(self.peak, current)
            self._exceeded = self._exceeded or current > self.limit
            if current > self.limit and not self._callback_called:
                self._callback_called = True
                callback = self.on_exceeded
        if callback is not None:
            try:
                callback()
            except Exception as error:
                with self._lock:
                    self._error = RuntimeError(
                        f"storage limit interrupt callback failed: {error}"
                    )
        return current

    def check_limit(self) -> None:
        self.sample_now()
        self._raise_error()
        with self._lock:
            exceeded = self._exceeded
            peak = self.peak
        if exceeded:
            raise RuntimeError(
                f"native setup storage exceeded {self.limit} bytes under {self.root} "
                f"(peak {peak} bytes)"
            )

    def _raise_error(self) -> None:
        with self._lock:
            error = self._error
        if error is not None:
            raise RuntimeError(f"failed to sample storage under {self.root}: {error}")

    def _sample(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.sample_now()
            except Exception:
                return
