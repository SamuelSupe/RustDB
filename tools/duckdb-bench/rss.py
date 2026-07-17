from __future__ import annotations

import os
import threading
import time


def current_rss_bytes() -> int:
    with open("/proc/self/statm", encoding="ascii") as source:
        resident_pages = int(source.read().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE")


class RssSampler:
    def __init__(self) -> None:
        self.baseline = current_rss_bytes()
        self.peak = self.baseline
        self._stop = threading.Event()
        self._worker = threading.Thread(target=self._sample, daemon=True)

    def start(self) -> None:
        self._worker.start()

    def stop(self) -> tuple[int, int]:
        self._stop.set()
        self._worker.join()
        self.peak = max(self.peak, current_rss_bytes())
        return self.baseline, self.peak

    def _sample(self) -> None:
        while not self._stop.is_set():
            self.peak = max(self.peak, current_rss_bytes())
            self._stop.wait(0.002)
