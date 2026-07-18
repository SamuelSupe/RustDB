use std::fmt;

use uuid::Uuid;

use crate::{Error, Result};

/// Stable identity of one physical row version in an immutable Native segment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct NativeRowId {
    segment_id: Uuid,
    offset: u64,
}

impl NativeRowId {
    pub(crate) fn new(segment_id: Uuid, offset: u64, segment_rows: u64) -> Result<Self> {
        if offset >= segment_rows {
            return Err(Error::InvalidArgument(format!(
                "native row offset {offset} is outside segment row count {segment_rows}"
            )));
        }
        Ok(Self { segment_id, offset })
    }

    pub(crate) fn segment_id(self) -> Uuid {
        self.segment_id
    }

    pub(crate) fn offset(self) -> u64 {
        self.offset
    }
}

impl fmt::Display for NativeRowId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.segment_id, self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_the_physical_row_ordinal() {
        let segment = Uuid::new_v4();
        let id = NativeRowId::new(segment, 7, 8).unwrap();
        assert_eq!(id.segment_id(), segment);
        assert_eq!(id.offset(), 7);
        assert_eq!(id.to_string(), format!("{segment}:7"));
        assert!(NativeRowId::new(segment, 8, 8).is_err());
    }
}
