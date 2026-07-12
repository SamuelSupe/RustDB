use crate::Result;
use crate::sql::{AggregateExpr, BoundExpr, LogicalPlan};

use super::super::{
    pull::{Correlation, Pulled},
    rewrite::remap_columns,
};

pub(super) struct Compacted {
    pub(super) plan: LogicalPlan,
    pub(super) correlations: Vec<Correlation>,
}

/// Narrows the correlated aggregate input before it becomes a hash-join side.
/// The global projection pass keeps logical column positions stable by filling
/// unused fields with NULL arrays; that is correct but needlessly widens every
/// Grace spill record. This local projection owns all downstream expressions,
/// so it can compact their positions without changing the public plan schema.
pub(super) fn apply(
    mut pulled: Pulled,
    groups: &mut [BoundExpr],
    aggregates: &mut [AggregateExpr],
) -> Result<Compacted> {
    for group in groups.iter_mut() {
        remap_columns(group, &pulled.old_to_new)?;
    }
    for aggregate in aggregates.iter_mut() {
        if let Some(expression) = &mut aggregate.expr {
            remap_columns(expression, &pulled.old_to_new)?;
        }
    }

    let mut required = Vec::new();
    for correlation in &pulled.correlations {
        match correlation {
            Correlation::Key { inner, .. } => inner.referenced_columns(&mut required),
            Correlation::Residual(expression) => expression.referenced_columns(&mut required),
        }
    }
    for group in groups.iter() {
        group.referenced_columns(&mut required);
    }
    for aggregate in aggregates.iter() {
        if let Some(expression) = &aggregate.expr {
            expression.referenced_columns(&mut required);
        }
    }
    required.sort_unstable();
    required.dedup();

    let input_width = pulled.plan.schema().arrow().fields().len();
    if required.len() == input_width && required.iter().copied().eq(0..input_width) {
        return Ok(Compacted {
            plan: pulled.plan,
            correlations: pulled.correlations,
        });
    }

    let mut mapping = vec![usize::MAX; input_width];
    let mut expressions = Vec::with_capacity(required.len());
    for (compact_index, input_index) in required.into_iter().enumerate() {
        let field = pulled.plan.schema().arrow().fields().get(input_index).ok_or_else(|| {
            crate::Error::Internal(format!(
                "correlated aggregate requires column {input_index} from a {input_width}-column input"
            ))
        })?;
        mapping[input_index] = compact_index;
        expressions.push(BoundExpr::column(
            input_index,
            field.data_type().clone(),
            field.name(),
        ));
    }

    for correlation in &mut pulled.correlations {
        match correlation {
            Correlation::Key { inner, .. } => remap_columns(inner, &mapping)?,
            Correlation::Residual(expression) => remap_columns(expression, &mapping)?,
        }
    }
    for group in groups.iter_mut() {
        remap_columns(group, &mapping)?;
    }
    for aggregate in aggregates.iter_mut() {
        if let Some(expression) = &mut aggregate.expr {
            remap_columns(expression, &mapping)?;
        }
    }

    let schema = super::expression_schema(&expressions);
    pulled.plan = LogicalPlan::Projection {
        input: Box::new(pulled.plan),
        expressions,
        schema,
    };
    Ok(Compacted {
        plan: pulled.plan,
        correlations: pulled.correlations,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::sql::{AggregateFunction, PlanSchema};

    #[test]
    fn keeps_only_correlation_and_aggregate_inputs_and_remaps_them() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("unused_before", DataType::Int64, false),
            Field::new("quantity", DataType::Float64, false),
            Field::new("unused_between", DataType::Utf8, false),
            Field::new("partkey", DataType::Int64, false),
        ]));
        let pulled = Pulled {
            plan: LogicalPlan::Empty {
                produce_one_row: false,
                schema: PlanSchema::unqualified(schema),
            },
            correlations: vec![Correlation::Key {
                outer: BoundExpr::outer_ref(1, 0, DataType::Int64, "outer_partkey"),
                inner: BoundExpr::column(3, DataType::Int64, "partkey"),
            }],
            // The aggregate was bound against the pre-pull two-column shape.
            old_to_new: vec![3, 1],
            scalar_aggregate: true,
        };
        let mut aggregates = vec![AggregateExpr {
            function: AggregateFunction::Avg,
            expr: Some(BoundExpr::column(1, DataType::Float64, "quantity")),
            distinct: false,
            data_type: DataType::Float64,
            display_name: "avg(quantity)".into(),
        }];

        let compact = apply(pulled, &mut [], &mut aggregates).unwrap();

        let LogicalPlan::Projection {
            expressions,
            schema,
            ..
        } = compact.plan
        else {
            panic!("expected a compacting projection")
        };
        assert_eq!(
            expressions
                .iter()
                .map(|expression| expression.display_name.as_str())
                .collect::<Vec<_>>(),
            ["quantity", "partkey"]
        );
        assert_eq!(schema.arrow().fields().len(), 2);
        assert!(matches!(
            aggregates[0].expr.as_ref().unwrap().kind,
            crate::sql::ExprKind::Column(0)
        ));
        let Correlation::Key { inner, .. } = &compact.correlations[0] else {
            panic!("expected an equality correlation")
        };
        assert!(matches!(inner.kind, crate::sql::ExprKind::Column(1)));
    }
}
