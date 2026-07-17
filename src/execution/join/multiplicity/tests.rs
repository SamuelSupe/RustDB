use std::time::Duration;

use arrow::datatypes::DataType;
use tokio_util::sync::CancellationToken;

use crate::{
    Error,
    runtime::{MemoryPool, QueryContext},
    sql::{AggregateExpr, AggregateFunction, BoundExpr},
};

use super::{eligible, reserve_probe_memory};

#[test]
fn accepts_only_probe_side_supported_aggregates() {
    let keys = vec![
        BoundExpr::column(0, DataType::Utf8, "name"),
        BoundExpr::column(1, DataType::Utf8, "segment"),
    ];
    assert!(eligible(&[count_star(), sum(1, DataType::Int64)], 2, &keys));
    assert!(!eligible(&[sum(2, DataType::Int64)], 2, &keys));
    assert!(!eligible(&[sum(1, DataType::Float64)], 2, &keys));
    assert!(!eligible(&[count_star()], 2, &keys[..1]));
}

#[tokio::test]
async fn parallel_workspace_pressure_fails_without_waiting_for_peer_lanes() {
    let root = tempfile::tempdir().unwrap();
    let pool = MemoryPool::new(1_024);
    let context = QueryContext::shared(pool.clone(), root.path()).unwrap();
    let _held = pool.try_reserve(1_024).unwrap();
    let cancellation = CancellationToken::new();

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        reserve_probe_memory(&context, 1, 1_024, Some(&cancellation)),
    )
    .await
    .expect("parallel workspace admission must be bounded");
    assert!(matches!(result, Err(Error::ResourceExhausted(_))));
}

fn count_star() -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: "count(*)".into(),
    }
}

fn sum(column: usize, input: DataType) -> AggregateExpr {
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(column, input, "value")),
        distinct: false,
        data_type: DataType::Decimal128(38, 0),
        display_name: "sum(value)".into(),
    }
}
