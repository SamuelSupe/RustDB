use std::fs;

use serde::Serialize;

const STATUS_PATH: &str = "/proc/self/status";
const CPUSET_PATH: &str = "/sys/fs/cgroup/cpuset.cpus.effective";
const CPU_MAX_PATH: &str = "/sys/fs/cgroup/cpu.max";
const MEMORY_MAX_PATH: &str = "/sys/fs/cgroup/memory.max";
const MEMINFO_PATH: &str = "/proc/meminfo";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct WorkerResources {
    pub(crate) cpu_affinity_list: String,
    pub(crate) cpu_affinity_count: usize,
    pub(crate) cgroup_cpuset_cpus_effective: String,
    pub(crate) cgroup_cpu_quota_us: Option<u64>,
    pub(crate) cgroup_cpu_period_us: u64,
    pub(crate) cgroup_memory_max_bytes: Option<u64>,
    pub(crate) visible_memory_bytes: u64,
}

impl WorkerResources {
    pub(crate) fn collect() -> Result<Self, String> {
        Self::parse(
            &read_required(STATUS_PATH)?,
            &read_required(CPUSET_PATH)?,
            &read_required(CPU_MAX_PATH)?,
            &read_required(MEMORY_MAX_PATH)?,
            &read_required(MEMINFO_PATH)?,
        )
    }

    fn parse(
        status: &str,
        cpuset: &str,
        cpu_max: &str,
        memory_max: &str,
        meminfo: &str,
    ) -> Result<Self, String> {
        let affinity = unique_value(status, "Cpus_allowed_list", ':', STATUS_PATH)?;
        let cpuset = nonempty(cpuset, CPUSET_PATH)?;
        let affinity_count = cpu_list_count(affinity, "Cpus_allowed_list")?;
        cpu_list_count(cpuset, CPUSET_PATH)?;
        let (quota, period) = parse_cpu_max(cpu_max)?;
        let memory_max = parse_maximum(memory_max, MEMORY_MAX_PATH)?;
        let visible_memory_bytes = parse_mem_total(meminfo)?;
        Ok(Self {
            cpu_affinity_list: affinity.to_owned(),
            cpu_affinity_count: affinity_count,
            cgroup_cpuset_cpus_effective: cpuset.to_owned(),
            cgroup_cpu_quota_us: quota,
            cgroup_cpu_period_us: period,
            cgroup_memory_max_bytes: memory_max,
            visible_memory_bytes,
        })
    }
}

fn read_required(path: &str) -> Result<String, String> {
    fs::read_to_string(path)
        .map_err(|error| format!("cannot read worker resource fact {path}: {error}"))
}

fn unique_value<'a>(
    input: &'a str,
    key: &str,
    separator: char,
    label: &str,
) -> Result<&'a str, String> {
    let mut values = input.lines().filter_map(|line| {
        let (candidate, value) = line.split_once(separator)?;
        (candidate.trim() == key).then_some(value.trim())
    });
    let value = values
        .next()
        .ok_or_else(|| format!("{label} does not contain {key}"))?;
    if values.next().is_some() {
        return Err(format!("{label} contains {key} more than once"));
    }
    nonempty(value, key)
}

fn nonempty<'a>(input: &'a str, label: &str) -> Result<&'a str, String> {
    let value = input.trim();
    if value.is_empty() {
        Err(format!("{label} must not be empty"))
    } else {
        Ok(value)
    }
}

fn cpu_list_count(input: &str, label: &str) -> Result<usize, String> {
    let mut count = 0_usize;
    let mut previous_end = None;
    for segment in input.split(',') {
        if segment.is_empty() || segment.trim() != segment {
            return Err(format!("{label} contains an invalid CPU segment"));
        }
        let (start, end) = match segment.split_once('-') {
            Some((start, end)) if !end.contains('-') => {
                (parse_cpu(start, label)?, parse_cpu(end, label)?)
            }
            Some(_) => return Err(format!("{label} contains an invalid CPU range")),
            None => {
                let cpu = parse_cpu(segment, label)?;
                (cpu, cpu)
            }
        };
        if start > end || previous_end.is_some_and(|previous| start <= previous) {
            return Err(format!("{label} CPU ranges must be ordered and disjoint"));
        }
        let width = end
            .checked_sub(start)
            .and_then(|width| width.checked_add(1))
            .ok_or_else(|| format!("{label} CPU range overflowed"))?;
        count = count
            .checked_add(width)
            .ok_or_else(|| format!("{label} CPU count overflowed"))?;
        previous_end = Some(end);
    }
    if count == 0 {
        return Err(format!("{label} must contain at least one CPU"));
    }
    Ok(count)
}

fn parse_cpu(input: &str, label: &str) -> Result<usize, String> {
    if input.is_empty() || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{label} contains a non-numeric CPU"));
    }
    input
        .parse::<usize>()
        .map_err(|_| format!("{label} contains a non-numeric CPU"))
}

fn parse_cpu_max(input: &str) -> Result<(Option<u64>, u64), String> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 {
        return Err(format!("{CPU_MAX_PATH} must contain quota and period"));
    }
    let quota = parse_maximum(parts[0], "cpu.max quota")?;
    let period = positive_u64(parts[1], "cpu.max period")?;
    Ok((quota, period))
}

fn parse_maximum(input: &str, label: &str) -> Result<Option<u64>, String> {
    let value = nonempty(input, label)?;
    if value == "max" {
        Ok(None)
    } else {
        positive_u64(value, label).map(Some)
    }
}

fn parse_mem_total(input: &str) -> Result<u64, String> {
    let value = unique_value(input, "MemTotal", ':', MEMINFO_PATH)?;
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 || parts[1] != "kB" {
        return Err(format!("{MEMINFO_PATH} MemTotal must use kB"));
    }
    positive_u64(parts[0], "MemTotal")?
        .checked_mul(1024)
        .ok_or_else(|| "MemTotal byte count overflowed".to_owned())
}

fn positive_u64(input: &str, label: &str) -> Result<u64, String> {
    match input.parse::<u64>() {
        Ok(value) if value > 0 => Ok(value),
        _ => Err(format!("{label} must be a positive integer")),
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerResources;

    const STATUS: &str = "Name:\trustdb\nCpus_allowed_list:\t0-3,8-11\n";
    const MEMINFO: &str = "MemTotal:       8388608 kB\nMemFree:         100 kB\n";

    #[test]
    fn parses_finite_and_unlimited_resource_fixtures() {
        let finite =
            WorkerResources::parse(STATUS, "0-7\n", "800000 100000\n", "4294967296\n", MEMINFO)
                .unwrap();
        assert_eq!(finite.cpu_affinity_list, "0-3,8-11");
        assert_eq!(finite.cpu_affinity_count, 8);
        assert_eq!(finite.cgroup_cpuset_cpus_effective, "0-7");
        assert_eq!(finite.cgroup_cpu_quota_us, Some(800_000));
        assert_eq!(finite.cgroup_cpu_period_us, 100_000);
        assert_eq!(finite.cgroup_memory_max_bytes, Some(4_294_967_296));
        assert_eq!(finite.visible_memory_bytes, 8 << 30);

        let unlimited =
            WorkerResources::parse(STATUS, "0-7", "max 100000", "max", MEMINFO).unwrap();
        assert_eq!(unlimited.cgroup_cpu_quota_us, None);
        assert_eq!(unlimited.cgroup_memory_max_bytes, None);
    }

    #[test]
    fn rejects_missing_or_malformed_resource_fixtures() {
        let cases = [
            ("Name:\trustdb\n", "0-7", "max 100000", "max", MEMINFO),
            (STATUS, "0-3,3-7", "max 100000", "max", MEMINFO),
            (STATUS, "0-7", "max 0", "max", MEMINFO),
            (STATUS, "0-7", "max 100000", "0", MEMINFO),
            (STATUS, "0-7", "max 100000", "max", "MemTotal: 1024 bytes\n"),
        ];
        for (status, cpuset, cpu_max, memory_max, meminfo) in cases {
            assert!(WorkerResources::parse(status, cpuset, cpu_max, memory_max, meminfo).is_err());
        }
    }
}
