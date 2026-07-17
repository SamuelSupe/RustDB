use std::{fs::File, io::Write};

use super::{DiskBudget, QuotaFile};

#[test]
fn quota_rejects_before_crossing_the_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("segment.rdbseg");
    let budget = DiskBudget::new(4);
    let mut file = QuotaFile::new(File::create(&path).unwrap(), budget.clone());

    assert!(file.write_all(b"12345").is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    assert_eq!(budget.used(), 0);

    file.write_all(b"1234").unwrap();
    assert!(file.write_all(b"5").is_err());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 4);
    assert_eq!(budget.used(), 4);
}

#[test]
fn metadata_uses_the_same_hard_budget() {
    let directory = tempfile::tempdir().unwrap();
    let budget = DiskBudget::new(10);

    budget
        .reserve_metadata(8, directory.path())
        .expect("first reservation fits");
    assert!(budget.reserve_metadata(3, directory.path()).is_err());
    assert_eq!(budget.used(), 8);
}
