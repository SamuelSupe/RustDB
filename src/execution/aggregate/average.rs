/// Convert an exact integer ratio to the nearest IEEE-754 `f64`, with ties to
/// even. Integer AVG uses this after its parallel partial sums have been
/// merged, so the result does not depend on lane or batch completion order.
pub(super) fn unsigned_ratio(numerator: u128, denominator: u64) -> f64 {
    assert!(denominator > 0, "AVG denominator is non-zero");
    if numerator == 0 {
        return 0.0;
    }

    let denominator = u128::from(denominator);
    let numerator_bits = 128_i32 - numerator.leading_zeros() as i32;
    let denominator_bits = 128_i32 - denominator.leading_zeros() as i32;
    let mut exponent = numerator_bits - denominator_bits;
    let below_power = if exponent >= 0 {
        numerator < (denominator << exponent as u32)
    } else {
        (numerator << (-exponent) as u32) < denominator
    };
    if below_power {
        exponent -= 1;
    }

    let shift = 52 - exponent;
    let (mut significand, remainder, divisor) = if shift >= 0 {
        let scaled = numerator << shift as u32;
        (scaled / denominator, scaled % denominator, denominator)
    } else {
        let divisor = denominator << (-shift) as u32;
        (numerator / divisor, numerator % divisor, divisor)
    };
    let halfway_order = remainder.cmp(&(divisor - remainder));
    if halfway_order.is_gt() || (halfway_order.is_eq() && significand & 1 == 1) {
        significand += 1;
    }
    if significand == 1_u128 << 53 {
        significand >>= 1;
        exponent += 1;
    }

    debug_assert!((1_u128 << 52..1_u128 << 53).contains(&significand));
    let exponent_bits = u64::try_from(exponent + 1023).expect("integer AVG is a normal f64");
    let fraction = u64::try_from(significand - (1_u128 << 52))
        .expect("integer AVG significand has 52 fraction bits");
    f64::from_bits((exponent_bits << 52) | fraction)
}

pub(super) fn signed_ratio(numerator: i128, denominator: u64) -> f64 {
    let magnitude = unsigned_ratio(numerator.unsigned_abs(), denominator);
    if numerator.is_negative() {
        -magnitude
    } else {
        magnitude
    }
}

#[cfg(test)]
mod tests {
    use super::{signed_ratio, unsigned_ratio};

    #[test]
    fn rounds_large_clickbench_average_from_exact_sum() {
        let value = unsigned_ratio(1_948_194_877_894_919_561_610_074, 1_000_000);
        assert_eq!(value.to_bits(), 0x43bb_0960_eb62_2986);
    }

    #[test]
    fn rounds_halfway_ratios_to_even() {
        let even = 1.0_f64;
        let odd = f64::from_bits(even.to_bits() + 1);
        assert_eq!(unsigned_ratio((1_u128 << 53) + 1, 1_u64 << 53), even);
        assert_eq!(
            unsigned_ratio((1_u128 << 53) + 3, 1_u64 << 53),
            f64::from_bits(odd.to_bits() + 1)
        );
    }

    #[test]
    fn preserves_sign_and_small_fraction() {
        assert_eq!(signed_ratio(-3, 2), -1.5);
        assert_eq!(unsigned_ratio(1, u64::MAX), 1.0 / u64::MAX as f64);
    }
}
