from __future__ import annotations

from pathlib import Path
from typing import Any


STATUS_PATH = Path("/proc/self/status")
CPUSET_PATH = Path("/sys/fs/cgroup/cpuset.cpus.effective")
CPU_MAX_PATH = Path("/sys/fs/cgroup/cpu.max")
MEMORY_MAX_PATH = Path("/sys/fs/cgroup/memory.max")
MEMINFO_PATH = Path("/proc/meminfo")


def collect() -> dict[str, Any]:
    return parse_resources(
        read_required(STATUS_PATH),
        read_required(CPUSET_PATH),
        read_required(CPU_MAX_PATH),
        read_required(MEMORY_MAX_PATH),
        read_required(MEMINFO_PATH),
    )


def parse_resources(
    status: str,
    cpuset: str,
    cpu_max: str,
    memory_max: str,
    meminfo: str,
) -> dict[str, Any]:
    affinity = unique_value(status, "Cpus_allowed_list", STATUS_PATH)
    cpuset = nonempty(cpuset, CPUSET_PATH)
    affinity_count = cpu_list_count(affinity, "Cpus_allowed_list")
    cpu_list_count(cpuset, str(CPUSET_PATH))
    quota, period = parse_cpu_max(cpu_max)
    return {
        "cpu_affinity_list": affinity,
        "cpu_affinity_count": affinity_count,
        "cgroup_cpuset_cpus_effective": cpuset,
        "cgroup_cpu_quota_us": quota,
        "cgroup_cpu_period_us": period,
        "cgroup_memory_max_bytes": parse_maximum(memory_max, str(MEMORY_MAX_PATH)),
        "visible_memory_bytes": parse_mem_total(meminfo),
    }


def read_required(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(f"cannot read worker resource fact {path}: {error}") from error


def unique_value(input_value: str, key: str, label: Path) -> str:
    values = []
    for line in input_value.splitlines():
        candidate, separator, value = line.partition(":")
        if separator and candidate.strip() == key:
            values.append(value.strip())
    if len(values) != 1:
        raise ValueError(f"{label} must contain {key} exactly once")
    return nonempty(values[0], key)


def nonempty(input_value: str, label: object) -> str:
    value = input_value.strip()
    if not value:
        raise ValueError(f"{label} must not be empty")
    return value


def cpu_list_count(input_value: str, label: str) -> int:
    count = 0
    previous_end = None
    for segment in input_value.split(","):
        if not segment or segment.strip() != segment:
            raise ValueError(f"{label} contains an invalid CPU segment")
        if "-" in segment:
            if segment.count("-") != 1:
                raise ValueError(f"{label} contains an invalid CPU range")
            start_value, end_value = segment.split("-", 1)
            start = parse_cpu(start_value, label)
            end = parse_cpu(end_value, label)
        else:
            start = end = parse_cpu(segment, label)
        if start > end or (previous_end is not None and start <= previous_end):
            raise ValueError(f"{label} CPU ranges must be ordered and disjoint")
        count += end - start + 1
        previous_end = end
    if count == 0:
        raise ValueError(f"{label} must contain at least one CPU")
    return count


def parse_cpu(input_value: str, label: str) -> int:
    if not input_value.isascii() or not input_value.isdigit():
        raise ValueError(f"{label} contains a non-numeric CPU")
    return int(input_value)


def parse_cpu_max(input_value: str) -> tuple[int | None, int]:
    parts = input_value.split()
    if len(parts) != 2:
        raise ValueError(f"{CPU_MAX_PATH} must contain quota and period")
    return parse_maximum(parts[0], "cpu.max quota"), positive_int(
        parts[1], "cpu.max period"
    )


def parse_maximum(input_value: str, label: str) -> int | None:
    value = nonempty(input_value, label)
    return None if value == "max" else positive_int(value, label)


def parse_mem_total(input_value: str) -> int:
    value = unique_value(input_value, "MemTotal", MEMINFO_PATH)
    parts = value.split()
    if len(parts) != 2 or parts[1] != "kB":
        raise ValueError(f"{MEMINFO_PATH} MemTotal must use kB")
    return positive_int(parts[0], "MemTotal") * 1024


def positive_int(input_value: str, label: str) -> int:
    try:
        value = int(input_value)
    except ValueError as error:
        raise ValueError(f"{label} must be a positive integer") from error
    if value <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return value
