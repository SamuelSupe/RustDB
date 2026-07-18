pub(super) mod cursor;
pub(super) mod frame;

use std::{mem::size_of, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::runtime::{QueryContext, SpillFile, estimate_schema_batch_bytes};
use crate::sql::{BoundExpr, WindowExpr, WindowFrameUnits, WindowFunction};
use crate::{Error, Result};

use super::super::value::{CellValue, values_to_array};
use super::{
    frame_index::{FrameCursor, FrameSidecar},
    memory::{cell_payload_bytes, is_variable},
    sidecar::RangeSidecar,
};
use cursor::{PeerCursor, ValueCursor};
use frame::Target;

pub(super) struct NavigationSidecar {
    pub(super) file: SpillFile,
    pub(super) expression_columns: Vec<Option<usize>>,
}

pub(super) fn build(
    partition: &SpillFile,
    expressions: &[WindowExpr],
    peer_sidecar: Option<&RangeSidecar>,
    frame_sidecar: Option<&FrameSidecar>,
    context: &Arc<QueryContext>,
    batch_size: usize,
    rows: u64,
) -> Result<Option<NavigationSidecar>> {
    let indices = expressions
        .iter()
        .enumerate()
        .filter_map(|(index, expression)| is_navigation(&expression.function).then_some(index))
        .collect::<Vec<_>>();
    if indices.is_empty() {
        return Ok(None);
    }
    let schema = schema(expressions, &indices);
    let capacity = batch_size.max(1);
    let base_bytes = estimate_schema_batch_bytes(schema.as_ref(), capacity)
        .saturating_mul(2)
        .saturating_add(
            indices
                .len()
                .saturating_mul(capacity)
                .saturating_mul(size_of::<CellValue>()),
        )
        .saturating_add(2048);
    let mut memory = context.memory.try_reserve(base_bytes).map_err(|_| {
        Error::ResourceExhausted(format!(
            "window navigation buffer requires {base_bytes} bytes (query limit {}, available {})",
            context.memory.limit(),
            context.memory.available(),
        ))
    })?;
    let mut writer = context
        .spill
        .writer("window-navigation", Arc::clone(&schema))?;
    let mut cursors = indices
        .iter()
        .map(|index| {
            Accessor::new(
                partition,
                &expressions[*index].function,
                Arc::clone(context),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let needs_peers = frame_sidecar.is_none()
        && expressions.iter().any(|expression| {
            expression.frame.units == WindowFrameUnits::Range
                && matches!(
                    expression.function,
                    WindowFunction::FirstValue(_) | WindowFunction::LastValue(_)
                )
        });
    let mut peers = if needs_peers {
        let sidecar = peer_sidecar
            .ok_or_else(|| Error::Internal("RANGE navigation has no peer sidecar".into()))?;
        Some(PeerCursor::new(&sidecar.file, Arc::clone(context))?)
    } else {
        None
    };
    let mut frames = frame_sidecar
        .map(|sidecar| FrameCursor::new(sidecar, Arc::clone(context)))
        .transpose()?;
    let mut buffer = NavigationBuffer::new(indices.len(), capacity);
    for row in 0..rows {
        context.check_cancelled()?;
        let peer = peers
            .as_mut()
            .map(|cursor| cursor.range_at(row))
            .transpose()?;
        let indexed = frames.as_mut().map(FrameCursor::next_range).transpose()?;
        for (column, index) in indices.iter().enumerate() {
            let target = frame::target(&expressions[*index], row, rows, peer, indexed);
            let value = cursors[column].value(target)?;
            let credit = value_credit(&expressions[*index].data_type, &value);
            memory.try_grow(credit)?;
            buffer.push(column, value, credit);
        }
        if buffer.len() >= capacity {
            memory.shrink(buffer.flush(&mut writer, &schema, expressions, &indices)?);
        }
    }
    memory.shrink(buffer.flush(&mut writer, &schema, expressions, &indices)?);
    drop(cursors);
    drop(peers);
    drop(buffer);
    drop(memory);
    let file = writer.finish(1)?;
    let mut expression_columns = vec![None; expressions.len()];
    for (column, index) in indices.iter().enumerate() {
        expression_columns[*index] = Some(column);
    }
    Ok(Some(NavigationSidecar {
        file,
        expression_columns,
    }))
}

struct Accessor {
    value: ValueCursor,
    default: Option<ValueCursor>,
}

impl Accessor {
    fn new(
        file: &SpillFile,
        function: &WindowFunction,
        context: Arc<QueryContext>,
    ) -> Result<Self> {
        let (value, default) = navigation_expressions(function)?;
        Ok(Self {
            value: ValueCursor::new(file, value.clone(), Arc::clone(&context))?,
            default: default
                .cloned()
                .map(|expression| ValueCursor::new(file, expression, context))
                .transpose()?,
        })
    }

    fn value(&mut self, target: Target) -> Result<CellValue> {
        match target {
            Target::Value(row) => self.value.value_at(row),
            Target::Default(row) => self
                .default
                .as_mut()
                .ok_or_else(|| Error::Internal("navigation default cursor is missing".into()))?
                .value_at(row),
            Target::Null => Ok(CellValue::Null),
        }
    }
}

fn navigation_expressions(function: &WindowFunction) -> Result<(&BoundExpr, Option<&BoundExpr>)> {
    match function {
        WindowFunction::Lead { expr, default, .. } | WindowFunction::Lag { expr, default, .. } => {
            Ok((expr, Some(default)))
        }
        WindowFunction::FirstValue(expr) | WindowFunction::LastValue(expr) => Ok((expr, None)),
        _ => Err(Error::Internal(
            "non-navigation function reached navigation sidecar".into(),
        )),
    }
}

fn is_navigation(function: &WindowFunction) -> bool {
    matches!(
        function,
        WindowFunction::Lead { .. }
            | WindowFunction::Lag { .. }
            | WindowFunction::FirstValue(_)
            | WindowFunction::LastValue(_)
    )
}

fn schema(expressions: &[WindowExpr], indices: &[usize]) -> SchemaRef {
    Arc::new(Schema::new(
        indices
            .iter()
            .map(|index| {
                Field::new(
                    format!("navigation_{index}"),
                    expressions[*index].data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn value_credit(data_type: &DataType, value: &CellValue) -> usize {
    if !is_variable(data_type) {
        return 0;
    }
    let copies = if matches!(data_type, DataType::LargeUtf8 | DataType::LargeBinary) {
        3
    } else {
        2
    };
    cell_payload_bytes(value).saturating_mul(copies)
}

struct NavigationBuffer {
    values: Vec<Vec<CellValue>>,
    credits: Vec<Vec<usize>>,
}

impl NavigationBuffer {
    fn new(columns: usize, capacity: usize) -> Self {
        Self {
            values: (0..columns).map(|_| Vec::with_capacity(capacity)).collect(),
            credits: (0..columns).map(|_| Vec::with_capacity(capacity)).collect(),
        }
    }

    fn len(&self) -> usize {
        self.values.first().map(Vec::len).unwrap_or(0)
    }

    fn push(&mut self, column: usize, value: CellValue, credit: usize) {
        self.values[column].push(value);
        self.credits[column].push(credit);
    }

    fn flush(
        &mut self,
        writer: &mut crate::runtime::SpillWriter,
        schema: &SchemaRef,
        expressions: &[WindowExpr],
        indices: &[usize],
    ) -> Result<usize> {
        if self.len() == 0 {
            return Ok(0);
        }
        let released = self.credits.iter_mut().flat_map(std::mem::take).sum();
        let columns = self
            .values
            .iter_mut()
            .zip(indices)
            .map(|(values, index)| {
                let array = values_to_array(values, &expressions[*index].data_type)?;
                values.clear();
                Ok(array)
            })
            .collect::<Result<Vec<ArrayRef>>>()?;
        writer.write_batch(&RecordBatch::try_new(Arc::clone(schema), columns)?)?;
        Ok(released)
    }
}
