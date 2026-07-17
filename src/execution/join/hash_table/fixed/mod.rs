use std::{collections::HashMap, hash::Hash};

#[cfg(test)]
use std::mem::size_of;

use ahash::RandomState;

mod build;
mod dense;
mod hash;
mod iter;
mod rows;

pub(in crate::execution::join::hash_table) use build::build;
pub(in crate::execution::join::hash_table) use dense::DenseTable;
#[cfg(test)]
pub(in crate::execution::join::hash_table) use dense::{
    DenseSlot, encode_duplicate, encode_unique,
};
pub(in crate::execution::join::hash_table) use iter::FixedKeys;
pub(super) use rows::{DuplicateRows, FixedEntry, try_push_rows};

pub(in crate::execution::join) trait DenseKey:
    Copy + Eq + Hash + Ord
{
    fn distance(min: Self, max: Self) -> Option<usize>;
    fn offset(self, min: Self) -> Option<usize>;
    fn from_offset(min: Self, offset: usize) -> Option<Self>;
}

macro_rules! dense_key {
    ($type:ty, $wide:ty) => {
        impl DenseKey for $type {
            fn distance(min: Self, max: Self) -> Option<usize> {
                usize::try_from((max as $wide).checked_sub(min as $wide)?).ok()
            }

            fn offset(self, min: Self) -> Option<usize> {
                usize::try_from((self as $wide).checked_sub(min as $wide)?).ok()
            }

            fn from_offset(min: Self, offset: usize) -> Option<Self> {
                let value = (min as $wide).checked_add(offset as $wide)?;
                Self::try_from(value).ok()
            }
        }
    };
}

dense_key!(i64, i128);
dense_key!(u64, u128);

enum FixedStorage<T> {
    Dense(DenseTable<T>),
    Hash(HashMap<T, FixedEntry, RandomState>),
}

pub(in crate::execution::join) struct FixedTable<T> {
    storage: FixedStorage<T>,
    duplicates: DuplicateRows,
    nulls: Vec<u32>,
}

pub(in crate::execution::join) type FixedHashTable<T> = FixedTable<T>;

impl<T: DenseKey> FixedTable<T> {
    fn new(storage: FixedStorage<T>) -> Self {
        Self {
            storage,
            duplicates: DuplicateRows::default(),
            nulls: Vec::new(),
        }
    }

    pub(super) fn lookup(&self, key: Option<T>, null_equal_keys: bool) -> Option<&[u32]> {
        match key {
            Some(key) => match &self.storage {
                FixedStorage::Dense(values) => values.rows(key, &self.duplicates),
                FixedStorage::Hash(values) => {
                    values.get(&key).map(|entry| entry.rows(&self.duplicates))
                }
            },
            None if null_equal_keys && !self.nulls.is_empty() => Some(&self.nulls),
            None => None,
        }
    }

    pub(super) fn keys(&self) -> FixedKeys<'_, T> {
        match &self.storage {
            FixedStorage::Dense(values) => FixedKeys::Dense(values.keys()),
            FixedStorage::Hash(values) => FixedKeys::Hash(values.keys()),
        }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        match &self.storage {
            FixedStorage::Dense(values) => values.capacity(),
            FixedStorage::Hash(values) => values.capacity(),
        }
    }

    #[cfg(test)]
    pub(super) fn is_dense(&self) -> bool {
        matches!(self.storage, FixedStorage::Dense(_))
    }

    #[cfg(test)]
    pub(super) fn duplicate_sidecar(&self) -> (usize, usize) {
        (self.duplicates.group_count(), self.duplicates.capacity())
    }

    #[cfg(test)]
    pub(super) fn allocated_bytes(&self) -> usize {
        let storage = match &self.storage {
            FixedStorage::Dense(values) => values.allocated_bytes(),
            FixedStorage::Hash(values) => hash::map_bytes::<T>(values.capacity()),
        };
        storage
            .saturating_add(self.duplicates.allocated_bytes())
            .saturating_add(self.nulls.capacity().saturating_mul(size_of::<u32>()))
    }
}
