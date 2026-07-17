from __future__ import annotations

import os
import platform
import subprocess


def collect() -> dict[str, object]:
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model(),
        "logical_cpus": os.cpu_count() or 1,
        "total_memory_bytes": total_memory_bytes(),
    }


def cpu_model() -> str:
    if platform.system() == "Darwin":
        value = command("sysctl", "-n", "machdep.cpu.brand_string")
        if value:
            return value
    if platform.system() == "Linux":
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as source:
                for line in source:
                    if line.startswith(("model name", "Hardware")):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return platform.processor() or platform.machine() or "unknown"


def total_memory_bytes() -> int:
    if platform.system() == "Darwin":
        value = command("sysctl", "-n", "hw.memsize")
        if value and value.isdigit():
            return int(value)
    try:
        return int(os.sysconf("SC_PHYS_PAGES")) * int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, TypeError, ValueError):
        return 1


def command(*args: str) -> str:
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""
