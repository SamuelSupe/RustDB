const MIN_ADAPTIVE_TARGET_BYTES: usize = 1 << 20;
const MORSELS_PER_TASK: usize = 4;

pub(super) fn effective_target_bytes(
    configured_bytes: usize,
    source_bytes: u64,
    task_count: usize,
) -> usize {
    if configured_bytes <= MIN_ADAPTIVE_TARGET_BYTES {
        return configured_bytes;
    }

    let target_count = task_count.max(1).saturating_mul(MORSELS_PER_TASK);
    let target_count = u64::try_from(target_count).unwrap_or(u64::MAX);
    let candidate = source_bytes.div_ceil(target_count);
    let candidate = usize::try_from(candidate).unwrap_or(usize::MAX);

    candidate
        .max(MIN_ADAPTIVE_TARGET_BYTES)
        .min(configured_bytes)
}

#[cfg(test)]
mod tests {
    use super::effective_target_bytes;

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    #[test]
    fn adapts_medium_files_to_four_morsels_per_task() {
        assert_eq!(effective_target_bytes(8 << 20, 64 * MIB, 4), 4 << 20);
        assert_eq!(
            effective_target_bytes(8 << 20, 64 * MIB + 1, 4),
            (4 << 20) + 1
        );
    }

    #[test]
    fn large_files_keep_the_configured_ceiling() {
        assert_eq!(effective_target_bytes(8 << 20, 10 * GIB, 4), 8 << 20);
        assert_eq!(effective_target_bytes(8 << 20, u64::MAX, 1), 8 << 20);
        assert_eq!(
            effective_target_bytes(8 << 20, u64::MAX, usize::MAX),
            1 << 20
        );
    }

    #[test]
    fn small_files_use_a_one_mib_floor() {
        assert_eq!(effective_target_bytes(8 << 20, 64 << 10, 4), 1 << 20);
    }

    #[test]
    fn sub_mib_user_configuration_is_preserved() {
        assert_eq!(effective_target_bytes(512 << 10, 64 * MIB, 4), 512 << 10);
        assert_eq!(effective_target_bytes(1, 64 * MIB, 4), 1);
    }
}
