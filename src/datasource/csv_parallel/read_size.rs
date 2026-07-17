const MAX_READ_BYTES: usize = 4 << 20;
const OPERATION_BUDGET_DIVISOR: usize = 8;

pub(super) fn effective_read_bytes(target_bytes: usize, operation_limit: usize) -> usize {
    let memory_bound = operation_limit
        .saturating_div(OPERATION_BUDGET_DIVISOR)
        .clamp(1, MAX_READ_BYTES);
    target_bytes.clamp(1, memory_bound)
}

#[cfg(test)]
mod tests {
    use super::effective_read_bytes;

    #[test]
    fn follows_the_morsel_target_up_to_four_mib() {
        assert_eq!(effective_read_bytes(4 << 20, 2 << 30), 4 << 20);
        assert_eq!(effective_read_bytes(8 << 20, 2 << 30), 4 << 20);
        assert_eq!(effective_read_bytes(512 << 10, 2 << 30), 512 << 10);
    }

    #[test]
    fn preserves_three_quarters_of_a_tight_operation_budget() {
        assert_eq!(effective_read_bytes(8 << 20, 16 << 20), 2 << 20);
        assert_eq!(effective_read_bytes(8 << 20, 8), 1);
        assert_eq!(effective_read_bytes(0, 0), 1);
    }
}
