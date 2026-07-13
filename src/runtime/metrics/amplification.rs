const MILLION: u128 = 1_000_000;

pub(super) fn millionths(logical_bytes: u64, write_bytes: u64) -> u64 {
    if logical_bytes == 0 {
        return 0;
    }
    let scaled = u128::from(write_bytes)
        .saturating_mul(MILLION)
        .checked_div(u128::from(logical_bytes))
        .unwrap_or(0);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

#[allow(dead_code)]
pub(super) fn exceeds(logical_bytes: u64, write_bytes: u64, limit: f64) -> bool {
    if write_bytes == 0 {
        return false;
    }
    if logical_bytes == 0 {
        return true;
    }
    write_bytes as f64 > logical_bytes as f64 * limit
}

#[cfg(test)]
mod tests {
    use super::{exceeds, millionths};

    #[test]
    fn fixed_point_ratio_is_stable_and_saturating() {
        assert_eq!(millionths(200, 400), 2_000_000);
        assert_eq!(millionths(3, 1), 333_333);
        assert_eq!(millionths(0, 100), 0);
        assert_eq!(millionths(1, u64::MAX), u64::MAX);
    }

    #[test]
    fn limit_check_handles_boundaries_and_missing_input() {
        assert!(!exceeds(100, 150, 1.5));
        assert!(exceeds(100, 151, 1.5));
        assert!(exceeds(0, 1, 4.0));
        assert!(!exceeds(0, 0, 1.0));
    }
}
