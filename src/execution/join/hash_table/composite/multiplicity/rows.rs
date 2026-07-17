use arrow::{array::ArrayRef, datatypes::DataType, row::RowConverter};

use crate::{Error, Result, runtime::MemoryReservation};

use super::{super::CompositeProbeRows, arena::MultiplicityEntries};
use crate::execution::join::hash_table::composite::memory;

pub(super) struct Table {
    converter: RowConverter,
    values: MultiplicityEntries,
}

impl Table {
    pub(super) fn try_new(
        types: &[DataType],
        reservation: &mut MemoryReservation,
    ) -> Result<Option<Self>> {
        let estimate = memory::converter_upper_bound(types).max(1);
        if reservation.try_grow(estimate).is_err() {
            return Ok(None);
        }
        let converter = RowConverter::new(
            types
                .iter()
                .cloned()
                .map(arrow::row::SortField::new)
                .collect(),
        )?;
        Ok(Some(Self {
            converter,
            values: MultiplicityEntries::new(),
        }))
    }

    pub(super) fn try_insert(
        &mut self,
        arrays: &[ArrayRef],
        reservation: &mut MemoryReservation,
    ) -> Result<bool> {
        let initial = reservation.size();
        let workspace = memory::encode_peak_bytes(arrays)?;
        if reservation.try_grow(workspace).is_err() {
            return Ok(false);
        }
        let encoded = match self.converter.convert_columns(arrays) {
            Ok(encoded) => encoded,
            Err(error) => {
                reservation.try_resize(initial)?;
                return Err(error.into());
            }
        };
        let actual = encoded.size().max(1);
        if !reconcile_growth(reservation, workspace, actual) {
            drop(encoded);
            return Ok(false);
        }
        for row in 0..encoded.num_rows() {
            if arrays
                .iter()
                .any(|array| arrow::array::Array::is_null(array.as_ref(), row))
            {
                continue;
            }
            if !self
                .values
                .try_increment(encoded.row(row).data(), reservation)?
            {
                drop(encoded);
                return Ok(false);
            }
        }
        drop(encoded);
        reservation.shrink(actual);
        Ok(true)
    }

    pub(super) fn probe_workspace_bytes(&self, arrays: &[ArrayRef]) -> Result<usize> {
        memory::encode_peak_bytes(arrays)
    }

    pub(super) fn encode_probe(
        &self,
        arrays: &[ArrayRef],
        mut workspace: MemoryReservation,
    ) -> Result<CompositeProbeRows> {
        let rows = self.converter.convert_columns(arrays)?;
        let actual = rows.size().max(1);
        if let Err(error) = workspace.try_resize(actual) {
            drop(rows);
            return Err(Error::ResourceExhausted(format!(
                "composite Join multiplicity probe retained {actual} row-encoding bytes: {error}"
            )));
        }
        Ok(CompositeProbeRows {
            rows,
            memory: workspace,
            null_free: arrays.iter().all(|array| array.null_count() == 0),
        })
    }

    pub(super) fn count(
        &self,
        probe: &CompositeProbeRows,
        arrays: &[ArrayRef],
        row: usize,
    ) -> Result<u64> {
        if row >= probe.rows.num_rows() {
            return Err(Error::Internal(
                "composite Join multiplicity probe row is out of bounds".into(),
            ));
        }
        if !probe.null_free
            && arrays
                .iter()
                .any(|array| arrow::array::Array::is_null(array.as_ref(), row))
        {
            return Ok(0);
        }
        Ok(self.values.count(probe.rows.row(row).data()))
    }
}

pub(super) fn supports(types: &[DataType]) -> bool {
    types.len() >= 2 && types.iter().all(supports_type)
}

fn reconcile_growth(reservation: &mut MemoryReservation, estimate: usize, actual: usize) -> bool {
    if actual > estimate && reservation.try_grow(actual - estimate).is_err() {
        return false;
    }
    reservation.shrink(estimate.saturating_sub(actual));
    true
}

fn supports_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
    )
}
