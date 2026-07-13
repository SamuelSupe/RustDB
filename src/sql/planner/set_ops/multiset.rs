use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::ast::SetOperator;

use crate::{Error, Result};

use crate::sql::{
    AggregateExpr, AggregateFunction, BinaryOp, BoundExpr, ExprKind, JoinType, LogicalPlan,
    PlanSchema, ScalarFunction, ScalarValue,
    binder::{make_binary, make_case},
};

pub(super) fn plan_multiset_operation(
    left: LogicalPlan,
    right: LogicalPlan,
    schema: PlanSchema,
    operator: SetOperator,
) -> Result<LogicalPlan> {
    if !matches!(operator, SetOperator::Intersect | SetOperator::Except) {
        return Err(Error::Internal(format!(
            "multiset lowering received {operator}"
        )));
    }
    let width = schema.arrow().fields().len();
    let left = count_rows(left, "__rustdb_left_count");
    let right = count_rows(right, "__rustdb_right_count");
    let join_type = if operator == SetOperator::Intersect {
        JoinType::Inner
    } else {
        JoinType::Left
    };
    let join_schema = if join_type == JoinType::Inner {
        PlanSchema::join(left.schema(), right.schema())
    } else {
        PlanSchema::left_join(left.schema(), right.schema())
    };
    let joined = LogicalPlan::Join {
        on: set_keys(&left, &right, width),
        left: Box::new(left),
        right: Box::new(right),
        residual: None,
        null_aware: None,
        null_equal_keys: true,
        join_type,
        schema: join_schema,
    };
    let repeat_count = repeat_count(width, operator)?;
    let projected = project_count(joined, &schema, repeat_count);
    Ok(LogicalPlan::Repeat {
        input: Box::new(projected),
        count: BoundExpr::column(width, DataType::Int64, "__rustdb_repeat_count"),
        schema,
    })
}

fn count_rows(input: LogicalPlan, name: &str) -> LogicalPlan {
    let input_schema = input.schema().clone();
    let group_exprs = input_schema
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| column(index, field))
        .collect();
    let count = AggregateExpr {
        function: AggregateFunction::Count,
        expr: None,
        distinct: false,
        data_type: DataType::Int64,
        display_name: name.into(),
    };
    let mut fields = input_schema
        .arrow()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
    LogicalPlan::Aggregate {
        input: Box::new(input),
        group_exprs,
        aggregate_exprs: vec![count],
        schema: PlanSchema::unqualified(Arc::new(Schema::new(fields))),
    }
}

fn repeat_count(width: usize, operator: SetOperator) -> Result<BoundExpr> {
    let left = BoundExpr::column(width, DataType::Int64, "__rustdb_left_count");
    let right = BoundExpr::column(
        width.saturating_mul(2).saturating_add(1),
        DataType::Int64,
        "__rustdb_right_count",
    );
    if operator == SetOperator::Intersect {
        return make_case(
            None,
            vec![(
                make_binary(left.clone(), BinaryOp::Lt, right.clone())?,
                left,
            )],
            Some(right),
        );
    }
    let zero = BoundExpr::literal(ScalarValue::Int64(0));
    let right = BoundExpr {
        kind: ExprKind::ScalarFunction {
            function: ScalarFunction::Coalesce,
            args: vec![right, zero.clone()],
        },
        data_type: DataType::Int64,
        display_name: "coalesce(__rustdb_right_count, 0)".into(),
    };
    let difference = make_binary(left.clone(), BinaryOp::Subtract, right.clone())?;
    make_case(
        None,
        vec![(make_binary(left, BinaryOp::Gt, right)?, difference)],
        Some(zero),
    )
}

fn project_count(input: LogicalPlan, schema: &PlanSchema, count: BoundExpr) -> LogicalPlan {
    let mut expressions = schema
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| column(index, field))
        .collect::<Vec<_>>();
    expressions.push(count);
    let mut fields = schema.arrow().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "__rustdb_repeat_count",
        DataType::Int64,
        false,
    )));
    LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema: PlanSchema::unqualified(Arc::new(Schema::new(fields))),
    }
}

fn set_keys(left: &LogicalPlan, right: &LogicalPlan, width: usize) -> Vec<(BoundExpr, BoundExpr)> {
    (0..width)
        .map(|index| {
            (
                column(index, left.schema().arrow().field(index)),
                column(index, right.schema().arrow().field(index)),
            )
        })
        .collect()
}

fn column(index: usize, field: &Field) -> BoundExpr {
    BoundExpr::column(index, field.data_type().clone(), field.name())
}
