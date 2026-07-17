use std::{collections::HashMap, hash::Hash};

use crate::{Error, Result};

use super::GroupKey;

/// Persistent group lookup for ordinary aggregation.
///
/// Encoded keys live in their own map so `&[u8]` can use `Vec<u8>`'s borrowed
/// lookup. Existing groups therefore do not allocate an owned key. Cell keys
/// keep the original equality path used by floating-point groups.
pub(in crate::execution::aggregate) struct GroupIndex {
    storage: Storage,
}

enum Storage {
    Empty,
    Encoded(HashMap<Vec<u8>, usize>),
    Cells(HashMap<Vec<super::super::CellValue>, usize>),
}

impl GroupIndex {
    pub(in crate::execution::aggregate) fn new() -> Self {
        Self {
            storage: Storage::Empty,
        }
    }

    pub(in crate::execution::aggregate) fn get(&self, key: &GroupKey) -> Option<usize> {
        match (&self.storage, key) {
            (Storage::Encoded(index), GroupKey::Encoded(key)) => index.get(key).copied(),
            (Storage::Cells(index), GroupKey::Cells(key)) => index.get(key).copied(),
            (Storage::Empty, _) => None,
            _ => None,
        }
    }

    pub(in crate::execution::aggregate) fn get_encoded(&self, key: &[u8]) -> Option<usize> {
        match &self.storage {
            Storage::Encoded(index) => index.get(key).copied(),
            Storage::Empty | Storage::Cells(_) => None,
        }
    }

    #[cfg(test)]
    pub(in crate::execution::aggregate) fn len(&self) -> usize {
        match &self.storage {
            Storage::Empty => 0,
            Storage::Encoded(index) => index.len(),
            Storage::Cells(index) => index.len(),
        }
    }

    #[cfg(test)]
    pub(in crate::execution::aggregate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(in crate::execution::aggregate) fn insert(
        &mut self,
        key: GroupKey,
        state_id: usize,
    ) -> Result<Option<usize>> {
        match (&mut self.storage, key) {
            (Storage::Empty, GroupKey::Encoded(key)) => {
                let mut index = HashMap::new();
                let previous = index.insert(key, state_id);
                self.storage = Storage::Encoded(index);
                Ok(previous)
            }
            (Storage::Empty, GroupKey::Cells(key)) => {
                let mut index = HashMap::new();
                let previous = index.insert(key, state_id);
                self.storage = Storage::Cells(index);
                Ok(previous)
            }
            (Storage::Encoded(index), GroupKey::Encoded(key)) => Ok(index.insert(key, state_id)),
            (Storage::Cells(index), GroupKey::Cells(key)) => Ok(index.insert(key, state_id)),
            _ => Err(Error::Internal(
                "aggregate group index key representation changed during execution".into(),
            )),
        }
    }

    pub(in crate::execution::aggregate) fn retained_key_bytes(&self) -> usize {
        match &self.storage {
            Storage::Empty => 0,
            Storage::Encoded(index) => index.keys().fold(0usize, |bytes, key| {
                bytes.saturating_add(GroupKey::encoded_memory_size(key.capacity()))
            }),
            Storage::Cells(index) => index.keys().fold(0usize, |bytes, key| {
                bytes.saturating_add(GroupKey::cells_memory_size(key, key.capacity()))
            }),
        }
    }

    pub(in crate::execution::aggregate) fn for_each_entry(
        &self,
        mut visit: impl FnMut(usize, usize),
    ) {
        match &self.storage {
            Storage::Empty => {}
            Storage::Encoded(index) => {
                for (key, state_id) in index {
                    visit(*state_id, GroupKey::encoded_memory_size(key.capacity()));
                }
            }
            Storage::Cells(index) => {
                for (key, state_id) in index {
                    visit(*state_id, GroupKey::cells_memory_size(key, key.capacity()));
                }
            }
        }
    }

    pub(in crate::execution::aggregate) fn remap(&mut self, remap: &[usize]) {
        match &mut self.storage {
            Storage::Empty => {}
            Storage::Encoded(index) => remap_index(index, remap),
            Storage::Cells(index) => remap_index(index, remap),
        }
    }
}

pub(in crate::execution::aggregate) trait ReleaseGroupIndex {
    fn release(&mut self);
}

impl ReleaseGroupIndex for GroupIndex {
    fn release(&mut self) {
        self.storage = Storage::Empty;
    }
}

impl<K> ReleaseGroupIndex for HashMap<K, usize>
where
    K: Eq + Hash,
{
    fn release(&mut self) {
        *self = HashMap::new();
    }
}

fn remap_index<K>(index: &mut HashMap<K, usize>, remap: &[usize])
where
    K: Eq + Hash,
{
    let mut survivors = HashMap::with_capacity(index.len());
    for (key, old_state_id) in std::mem::take(index) {
        let new_state_id = remap.get(old_state_id).copied().unwrap_or(usize::MAX);
        if new_state_id != usize::MAX {
            survivors.insert(key, new_state_id);
        }
    }
    *index = survivors;
}

#[cfg(test)]
mod tests {
    use super::{GroupIndex, ReleaseGroupIndex};
    use crate::execution::aggregate::{CellValue, key::GroupKey};

    #[test]
    fn encoded_keys_support_borrowed_lookup_and_remap() {
        let mut index = GroupIndex::new();
        index
            .insert(GroupKey::Encoded(b"existing".to_vec()), 3)
            .unwrap();

        assert_eq!(index.get_encoded(b"existing"), Some(3));
        assert_eq!(index.get_encoded(b"missing"), None);
        index.remap(&[usize::MAX, usize::MAX, usize::MAX, 1]);
        assert_eq!(index.get_encoded(b"existing"), Some(1));

        index.release();
        assert_eq!(index.get_encoded(b"existing"), None);
    }

    #[test]
    fn cell_keys_keep_their_original_lookup_path() {
        let mut index = GroupIndex::new();
        let key = GroupKey::Cells(vec![CellValue::Float64(-0.0)]);
        index.insert(key.clone(), 7).unwrap();

        assert_eq!(index.get(&key), Some(7));
        assert_eq!(index.get_encoded(&[]), None);
    }
}
