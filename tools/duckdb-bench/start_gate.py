from __future__ import annotations

import threading
import time


class QueryStartGate:
    def __init__(self, queries: int, timeout_seconds: float) -> None:
        participants = queries + 1
        self._ready = threading.Barrier(participants, timeout=timeout_seconds)
        self._start = threading.Barrier(participants, timeout=timeout_seconds)
        self._origin: float | None = None

    def wait_until_ready(self) -> None:
        self._ready.wait()

    def release(self) -> float:
        if self._origin is not None:
            raise RuntimeError("query start gate was released more than once")
        self._origin = time.perf_counter()
        self._start.wait()
        return self._origin

    def wait_for_start(self) -> tuple[float, float]:
        self._ready.wait()
        self._start.wait()
        origin = self._origin
        if origin is None:
            raise RuntimeError("query start gate has no release timestamp")
        return origin, time.perf_counter()
