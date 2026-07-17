use std::mem::size_of;

use arrow::{array::ArrayRef, datatypes::DataType};

use crate::{Error, Result, runtime::MemoryReservation};

use super::CompositeProbeRows;

mod arena;
mod byte_pair;
mod rows;

pub(in crate::execution::join) struct CompositeMultiplicityTable {
    logical_types: Box<[DataType]>,
    inner: Table,
}

pub(in crate::execution::join) enum CompositeMultiplicityProbe {
    Rows(CompositeProbeRows),
    BytePair(byte_pair::Probe),
}

enum Table {
    Rows(rows::Table),
    BytePair(byte_pair::Table),
}

impl CompositeMultiplicityTable {
    pub(in crate::execution::join) fn supports(types: &[DataType]) -> bool {
        rows::supports(types)
    }

    pub(in crate::execution::join) fn try_new(
        types: &[DataType],
        reservation: &mut MemoryReservation,
    ) -> Result<Option<Self>> {
        if !Self::supports(types) {
            return Ok(None);
        }
        let initial = reservation.size();
        let metadata = size_of::<Self>()
            .saturating_add(types.len().saturating_mul(size_of::<DataType>()))
            .max(1);
        if reservation.try_grow(metadata).is_err() {
            return Ok(None);
        }

        let inner = if byte_pair::supports(types) {
            Table::BytePair(byte_pair::Table::new())
        } else {
            match rows::Table::try_new(types, reservation) {
                Ok(Some(table)) => Table::Rows(table),
                Ok(None) => {
                    reservation.try_resize(initial)?;
                    return Ok(None);
                }
                Err(error) => {
                    reservation.try_resize(initial)?;
                    return Err(error);
                }
            }
        };
        Ok(Some(Self {
            logical_types: types.to_vec().into_boxed_slice(),
            inner,
        }))
    }

    pub(in crate::execution::join) fn try_insert(
        &mut self,
        arrays: &[ArrayRef],
        reservation: &mut MemoryReservation,
    ) -> Result<bool> {
        self.validate_arrays(arrays)?;
        match &mut self.inner {
            Table::Rows(table) => table.try_insert(arrays, reservation),
            Table::BytePair(table) => table.try_insert(arrays, reservation),
        }
    }

    pub(in crate::execution::join) fn probe_workspace_bytes(
        &self,
        arrays: &[ArrayRef],
    ) -> Result<usize> {
        self.validate_arrays(arrays)?;
        match &self.inner {
            Table::Rows(table) => table.probe_workspace_bytes(arrays),
            Table::BytePair(_) => Ok(byte_pair::probe_memory_bytes()),
        }
    }

    pub(in crate::execution::join) fn encode_probe(
        &self,
        arrays: &[ArrayRef],
        workspace: MemoryReservation,
    ) -> Result<CompositeMultiplicityProbe> {
        self.validate_arrays(arrays)?;
        match &self.inner {
            Table::Rows(table) => table
                .encode_probe(arrays, workspace)
                .map(CompositeMultiplicityProbe::Rows),
            Table::BytePair(_) => byte_pair::Probe::try_new(arrays, workspace)
                .map(CompositeMultiplicityProbe::BytePair),
        }
    }

    pub(in crate::execution::join) fn count(
        &self,
        probe: &CompositeMultiplicityProbe,
        arrays: &[ArrayRef],
        row: usize,
    ) -> Result<u64> {
        match (&self.inner, probe) {
            (Table::Rows(table), CompositeMultiplicityProbe::Rows(probe)) => {
                table.count(probe, arrays, row)
            }
            (Table::BytePair(table), CompositeMultiplicityProbe::BytePair(probe)) => {
                table.count(probe, row)
            }
            _ => Err(Error::Internal(
                "composite Join multiplicity probe does not match its table".into(),
            )),
        }
    }

    fn validate_arrays(&self, arrays: &[ArrayRef]) -> Result<()> {
        validate_arrays(&self.logical_types, arrays)
    }

    #[cfg(test)]
    pub(super) fn uses_byte_pair(&self) -> bool {
        matches!(self.inner, Table::BytePair(_))
    }
}

fn validate_arrays(logical_types: &[DataType], arrays: &[ArrayRef]) -> Result<()> {
    if arrays.len() != logical_types.len() {
        return Err(Error::Internal(format!(
            "composite Join multiplicity expected {} key arrays, received {}",
            logical_types.len(),
            arrays.len()
        )));
    }
    for (expected, array) in logical_types.iter().zip(arrays) {
        if expected != array.data_type() {
            return Err(Error::Internal(format!(
                "composite Join multiplicity expected key type {expected}, found {}",
                array.data_type()
            )));
        }
    }
    let rows = arrays.first().map_or(0, |array| array.len());
    if arrays.iter().any(|array| array.len() != rows) {
        return Err(Error::Internal(
            "composite Join multiplicity key arrays have different row counts".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "multiplicity/tests.rs"]
mod tests;
