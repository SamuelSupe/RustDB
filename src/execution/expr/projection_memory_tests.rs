use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use super::{evaluate, project, projection_workspace_bytes};
use crate::runtime::{BatchEnvelope, MemoryPool};
use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

#[test]
fn column_projection_extra_peak_is_independent_of_input_buffer_size() {
    let small = int_batch(1);
    let large = int_batch(64 * 1024);

    assert!(large.column(0).get_array_memory_size() > small.column(0).get_array_memory_size());
    assert_eq!(extra_projection_peak(small), extra_projection_peak(large));
}

#[test]
fn computed_projection_reserves_its_kernel_output() {
    let batch = int_batch(64 * 1024);
    let expression = BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(0, DataType::Int64, "value")),
            op: BinaryOp::Add,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(1))),
        },
        data_type: DataType::Int64,
        display_name: "value + 1".into(),
    };
    let output = evaluate(&expression, &batch).unwrap();

    assert!(
        projection_workspace_bytes(std::slice::from_ref(&expression), &batch)
            >= output.get_array_memory_size()
    );
}

#[test]
fn cast_projection_reserves_its_new_array() {
    let batch = int_batch(64 * 1024);
    let expression = BoundExpr {
        kind: ExprKind::Cast {
            expr: Box::new(BoundExpr::column(0, DataType::Int64, "value")),
        },
        data_type: DataType::Float64,
        display_name: "CAST(value AS DOUBLE)".into(),
    };
    let output = evaluate(&expression, &batch).unwrap();

    assert!(
        projection_workspace_bytes(std::slice::from_ref(&expression), &batch)
            >= output.get_array_memory_size()
    );
}

fn int_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from_iter_values(
            (0..rows).map(|value| value as i64),
        ))],
    )
    .unwrap()
}

fn extra_projection_peak(batch: RecordBatch) -> usize {
    let pool = MemoryPool::new(128 << 20);
    let input = BatchEnvelope::try_new(batch, &pool, "projection memory test").unwrap();
    let input_peak = pool.peak();
    let expression = BoundExpr::column(0, DataType::Int64, "value");
    let workspace = pool
        .try_reserve(projection_workspace_bytes(
            std::slice::from_ref(&expression),
            input.batch(),
        ))
        .unwrap();
    let projected = project(&[expression], input.schema(), input.batch()).unwrap();
    let output = input
        .replace_with_reservation(projected, workspace, "projection memory test")
        .unwrap();

    assert_eq!(output.memory_size(), output.get_array_memory_size());
    pool.peak() - input_peak
}
