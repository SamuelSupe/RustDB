use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};

use super::match_plan;
use crate::sql::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan,
    PlanSchema, ScalarValue,
};

#[test]
fn remaps_the_current_build_swap_restore_projection() {
    // The benchmark's logical order is orders then lineitem. After choosing
    // orders as the right/build side the physical Join is lineitem then
    // orders, and [2, 0, 1] restores the original logical output order.
    let lineitem = relation(vec![
        field("l_orderkey", DataType::Int64),
        field("l_extendedprice", DataType::Decimal128(10, 2)),
    ]);
    let orders = relation(vec![field("o_orderkey", DataType::Int64)]);
    let join = inner_join(lineitem, orders);
    let input = projection(join, &[2, 0, 1]);
    let aggregates = vec![count_star(), sum(2, DataType::Decimal128(10, 2))];

    let matched = match match_plan(input, &[], &aggregates) {
        Ok(matched) => matched,
        Err(_) => panic!("shape should match"),
    };

    assert_eq!(matched.left.schema().arrow().field(0).name(), "l_orderkey");
    assert_eq!(matched.right.schema().arrow().field(0).name(), "o_orderkey");
    assert_eq!(column(&matched.aggregates[1]), 1);
    assert_eq!(
        matched.aggregates[1].expr.as_ref().unwrap().data_type,
        DataType::Decimal128(10, 2)
    );
    assert_eq!(matched.on.len(), 1);
    assert_eq!(matched.schema.arrow().fields().len(), 3);
}

#[test]
fn composes_nested_column_projections() {
    let join = inner_join(
        relation(vec![
            field("left_key", DataType::Int64),
            field("left_value", DataType::Int64),
        ]),
        relation(vec![field("right_key", DataType::Int64)]),
    );
    let inner = projection(join, &[2, 0, 1]);
    let outer = projection(inner, &[2, 0]);
    let aggregates = vec![sum(0, DataType::Int64), sum(1, DataType::Int64)];

    let matched = match match_plan(outer, &[], &aggregates) {
        Ok(matched) => matched,
        Err(_) => panic!("shape should match"),
    };

    assert_eq!(column(&matched.aggregates[0]), 1);
    assert_eq!(column(&matched.aggregates[1]), 2);
}

#[test]
fn accepts_only_the_fixed_numeric_sum_output_contract() {
    for (input, output) in [
        (DataType::Int8, DataType::Decimal128(38, 0)),
        (DataType::UInt64, DataType::Decimal128(38, 0)),
        (DataType::Decimal128(12, 4), DataType::Decimal128(38, 4)),
    ] {
        let plan = join_with_payload(input.clone());
        let mut aggregate = sum(1, input);
        aggregate.data_type = output;
        assert!(match_plan(plan, &[], &[aggregate]).is_ok());
    }

    let plan = join_with_payload(DataType::Int64);
    let mut wrong_output = sum(1, DataType::Int64);
    wrong_output.data_type = DataType::Int64;
    assert!(match_plan(plan, &[], &[wrong_output]).is_err());

    let plan = join_with_payload(DataType::Utf8);
    let mut text_sum = sum(1, DataType::Utf8);
    text_sum.data_type = DataType::Utf8;
    assert!(match_plan(plan, &[], &[text_sum]).is_err());

    // Lane scheduling changes floating-point reduction order. Keep Float SUM
    // on the ordinary aggregate path until it has a reproducible reducer.
    let plan = join_with_payload(DataType::Float64);
    let mut float_sum = sum(1, DataType::Float64);
    float_sum.data_type = DataType::Float64;
    assert!(match_plan(plan, &[], &[float_sum]).is_err());
}

#[test]
fn rejects_non_global_or_non_direct_aggregate_shapes() {
    let plan = join_with_payload(DataType::Int64);
    assert!(
        match_plan(
            plan,
            &[BoundExpr::column(0, DataType::Int64, "key")],
            &[count_star()]
        )
        .is_err()
    );

    let plan = join_with_payload(DataType::Int64);
    let mut distinct = count_star();
    distinct.distinct = true;
    assert!(match_plan(plan, &[], &[distinct]).is_err());

    let plan = join_with_payload(DataType::Int64);
    let mut count_column = count_star();
    count_column.expr = Some(BoundExpr::column(1, DataType::Int64, "payload"));
    assert!(match_plan(plan, &[], &[count_column]).is_err());

    let plan = join_with_payload(DataType::Int64);
    let mut expression_sum = sum(1, DataType::Int64);
    expression_sum.expr = Some(BoundExpr {
        kind: ExprKind::Binary {
            left: Box::new(BoundExpr::column(1, DataType::Int64, "payload")),
            op: BinaryOp::Add,
            right: Box::new(BoundExpr::literal(ScalarValue::Int64(1))),
        },
        data_type: DataType::Int64,
        display_name: "payload + 1".into(),
    });
    assert!(match_plan(plan, &[], &[expression_sum]).is_err());

    let plan = join_with_payload(DataType::Int64);
    let average = AggregateExpr {
        function: AggregateFunction::Avg,
        expr: Some(BoundExpr::column(1, DataType::Int64, "payload")),
        distinct: false,
        data_type: DataType::Float64,
        display_name: "avg(payload)".into(),
    };
    assert!(match_plan(plan, &[], &[average]).is_err());
}

#[test]
fn rejects_non_column_projection_and_non_simple_joins() {
    let join = join_with_payload(DataType::Int64);
    let schema =
        PlanSchema::unqualified(Arc::new(Schema::new(vec![field("one", DataType::Int64)])));
    let literal = LogicalPlan::Projection {
        input: Box::new(join),
        expressions: vec![BoundExpr::literal(ScalarValue::Int64(1))],
        schema,
    };
    assert!(match_plan(literal, &[], &[count_star()]).is_err());

    for mutation in [
        JoinMutation::Left,
        JoinMutation::NullEqual,
        JoinMutation::Residual,
    ] {
        let mut plan = join_with_payload(DataType::Int64);
        mutate_join(&mut plan, mutation);
        assert!(match_plan(plan, &[], &[count_star()]).is_err());
    }
}

#[test]
fn failed_match_returns_the_original_owned_plan() {
    let input = join_with_payload(DataType::Int64);
    let schema = Arc::clone(input.schema().arrow());
    let returned = match match_plan(input, &[], &[]) {
        Ok(_) => panic!("empty aggregates must not match"),
        Err(returned) => returned,
    };

    assert!(Arc::ptr_eq(returned.schema().arrow(), &schema));
    assert!(matches!(returned, LogicalPlan::Join { .. }));
}

fn join_with_payload(payload: DataType) -> LogicalPlan {
    inner_join(
        relation(vec![
            field("left_key", DataType::Int64),
            field("payload", payload),
        ]),
        relation(vec![field("right_key", DataType::Int64)]),
    )
}

fn inner_join(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    let schema = PlanSchema::join(left.schema(), right.schema());
    LogicalPlan::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: vec![(
            BoundExpr::column(0, DataType::Int64, "left_key"),
            BoundExpr::column(0, DataType::Int64, "right_key"),
        )],
        null_equal_keys: false,
        residual: None,
        null_aware: None,
        join_type: JoinType::Inner,
        schema,
    }
}

fn projection(input: LogicalPlan, columns: &[usize]) -> LogicalPlan {
    let expressions = columns
        .iter()
        .map(|index| {
            let source = input.schema().arrow().field(*index);
            BoundExpr::column(*index, source.data_type().clone(), source.name())
        })
        .collect::<Vec<_>>();
    let fields = columns
        .iter()
        .map(|index| Arc::clone(&input.schema().arrow().fields()[*index]))
        .collect::<Vec<_>>();
    LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema: PlanSchema::unqualified(Arc::new(Schema::new(fields))),
    }
}

fn relation(fields: Vec<Field>) -> LogicalPlan {
    let schema = PlanSchema::unqualified(Arc::new(Schema::new(fields)));
    LogicalPlan::Empty {
        produce_one_row: false,
        schema,
    }
}

fn field(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, true)
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

fn sum(index: usize, input: DataType) -> AggregateExpr {
    let output = match &input {
        DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Decimal128(_, scale) => DataType::Decimal128(38, *scale),
        _ => DataType::Decimal128(38, 0),
    };
    AggregateExpr {
        function: AggregateFunction::Sum,
        expr: Some(BoundExpr::column(index, input, "value")),
        distinct: false,
        data_type: output,
        display_name: "sum(value)".into(),
    }
}

fn column(aggregate: &AggregateExpr) -> usize {
    let ExprKind::Column(index) = &aggregate.expr.as_ref().unwrap().kind else {
        panic!("expected a direct aggregate column")
    };
    *index
}

#[derive(Clone, Copy)]
enum JoinMutation {
    Left,
    NullEqual,
    Residual,
}

fn mutate_join(plan: &mut LogicalPlan, mutation: JoinMutation) {
    let LogicalPlan::Join {
        join_type,
        null_equal_keys,
        residual,
        ..
    } = plan
    else {
        panic!("expected Join")
    };
    match mutation {
        JoinMutation::Left => *join_type = JoinType::Left,
        JoinMutation::NullEqual => *null_equal_keys = true,
        JoinMutation::Residual => *residual = Some(BoundExpr::literal(ScalarValue::Boolean(true))),
    }
}
