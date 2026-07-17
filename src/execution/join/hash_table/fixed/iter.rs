use std::collections::hash_map::Keys;

use super::{DenseKey, FixedEntry, dense::DenseKeys};

#[derive(Clone)]
pub(in crate::execution::join) enum FixedKeys<'a, T> {
    Dense(DenseKeys<'a, T>),
    Hash(Keys<'a, T, FixedEntry>),
}

impl<T: DenseKey> Iterator for FixedKeys<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Dense(keys) => keys.next(),
            Self::Hash(keys) => keys.next().copied(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<T: DenseKey> ExactSizeIterator for FixedKeys<'_, T> {
    fn len(&self) -> usize {
        match self {
            Self::Dense(keys) => keys.len(),
            Self::Hash(keys) => keys.len(),
        }
    }
}
