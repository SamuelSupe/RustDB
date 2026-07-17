use arrow::datatypes::DataType;

use super::{
    CATALOG_HEADROOM_BYTES, FIXED_METADATA_ALLOWANCE_BYTES, new_snapshot_limit, supported_type,
};

#[test]
fn native_decimal_precision_is_bounded() {
    assert!(supported_type(&DataType::Decimal128(38, 4)));
    assert!(!supported_type(&DataType::Decimal128(39, 4)));
}

#[test]
fn unknown_source_starts_with_only_the_fixed_metadata_budget() {
    assert_eq!(
        new_snapshot_limit(0, 0, 0, 0, 0).unwrap(),
        FIXED_METADATA_ALLOWANCE_BYTES - CATALOG_HEADROOM_BYTES
    );
}

#[test]
fn unknown_source_budget_does_not_precharge_an_existing_snapshot() {
    assert_eq!(
        new_snapshot_limit(0, 0, 0, 512 * 1024 * 1024, 600 * 1024 * 1024).unwrap(),
        FIXED_METADATA_ALLOWANCE_BYTES - CATALOG_HEADROOM_BYTES
    );
}

#[test]
fn append_peak_counts_inherited_storage_once() {
    let mib = 1024 * 1024;
    let limit = new_snapshot_limit(mib, mib, mib + mib / 2, 0, 0).unwrap();
    assert_eq!(
        limit,
        2 * (2 * mib) + FIXED_METADATA_ALLOWANCE_BYTES - (mib + mib / 2) - CATALOG_HEADROOM_BYTES
    );
}
