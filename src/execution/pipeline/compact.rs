use crate::{
    Error, Result,
    sql::{BoundExpr, ExprKind},
};

use super::PipelineOperator;

pub(super) struct CompactPlan {
    pub(super) filters: Vec<BoundExpr>,
    pub(super) projection: Option<CompactProjection>,
}

pub(super) struct CompactProjection {
    pub(super) expressions: Vec<BoundExpr>,
    pub(super) schema: arrow::datatypes::SchemaRef,
}

/// Compiles the common projected Scan -> Filter -> terminal Project shape so
/// every expression can run against the compact physical scan columns. More
/// complex projection chains keep using the full-schema fallback.
pub(super) fn plan(
    operators: &[PipelineOperator],
    projection: Option<&[usize]>,
) -> Result<Option<CompactPlan>> {
    let Some(projection) = projection.filter(|projection| !projection.is_empty()) else {
        return Ok(None);
    };
    if operators.is_empty() {
        return Ok(None);
    }

    let (filter_operators, terminal_projection) = match operators.split_last() {
        Some((
            PipelineOperator::Projection {
                expressions,
                schema,
            },
            filters,
        )) if filters
            .iter()
            .all(|operator| matches!(operator, PipelineOperator::Filter(_))) =>
        {
            (filters, Some((expressions, schema)))
        }
        _ if operators
            .iter()
            .all(|operator| matches!(operator, PipelineOperator::Filter(_))) =>
        {
            (operators, None)
        }
        _ => return Ok(None),
    };

    let filters = filter_operators
        .iter()
        .map(|operator| {
            let PipelineOperator::Filter(predicate) = operator else {
                unreachable!("filter prefix was checked above")
            };
            remap(predicate, projection)
        })
        .collect::<Result<Vec<_>>>()?;
    let projection = terminal_projection
        .map(|(expressions, schema)| -> Result<CompactProjection> {
            let expressions = expressions
                .iter()
                .map(|expression| remap(expression, projection))
                .collect::<Result<Vec<_>>>()?;
            Ok(CompactProjection {
                expressions,
                schema: std::sync::Arc::clone(schema),
            })
        })
        .transpose()?;
    Ok(Some(CompactPlan {
        filters,
        projection,
    }))
}

/// Rebinds a filter from the logical scan schema to the compact physical
/// projection returned by the data source.
#[cfg(test)]
pub(super) fn remap_filter(expr: &BoundExpr, projection: &[usize]) -> Result<BoundExpr> {
    remap(expr, projection)
}

fn remap(expr: &BoundExpr, projection: &[usize]) -> Result<BoundExpr> {
    let mut remapped = expr.clone();
    remap_expr(&mut remapped, projection)?;
    Ok(remapped)
}

fn remap_expr(expr: &mut BoundExpr, projection: &[usize]) -> Result<()> {
    match &mut expr.kind {
        ExprKind::Column(index) => {
            *index = projection
                .iter()
                .position(|projected| projected == index)
                .ok_or_else(|| {
                    Error::Internal(format!(
                        "expression column {index} is missing from the scan projection"
                    ))
                })?;
        }
        ExprKind::OuterRef { .. } => {
            return Err(Error::Internal(
                "OuterRef reached compact pipeline remapping after decorrelation".into(),
            ));
        }
        ExprKind::DeferredGroup(_) | ExprKind::DeferredAggregate(_) => {
            return Err(Error::Internal(
                "deferred aggregate result reached compact pipeline remapping".into(),
            ));
        }
        ExprKind::Literal(_) => {}
        ExprKind::Binary { left, right, .. } => {
            remap_expr(left, projection)?;
            remap_expr(right, projection)?;
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } | ExprKind::Cast { expr } => {
            remap_expr(expr, projection)?
        }
        ExprKind::Like { expr, pattern, .. } => {
            remap_expr(expr, projection)?;
            remap_expr(pattern, projection)?;
        }
        ExprKind::Case {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                remap_expr(when, projection)?;
                remap_expr(then, projection)?;
            }
            remap_expr(else_expr, projection)?;
        }
        ExprKind::ScalarFunction { args, .. } => {
            for arg in args {
                remap_expr(arg, projection)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::remap_filter;
    use crate::sql::{BinaryOp, BoundExpr, ExprKind, ScalarValue};

    fn comparison(column: usize, value: i64) -> BoundExpr {
        BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(BoundExpr::column(column, DataType::Int64, "column")),
                op: BinaryOp::Lt,
                right: Box::new(BoundExpr::literal(ScalarValue::Int64(value))),
            },
            data_type: DataType::Boolean,
            display_name: "column < value".into(),
        }
    }

    #[test]
    fn remaps_nested_filter_columns_to_compact_positions() {
        let filter = BoundExpr {
            kind: ExprKind::Binary {
                left: Box::new(comparison(7, 10)),
                op: BinaryOp::And,
                right: Box::new(comparison(2, 20)),
            },
            data_type: DataType::Boolean,
            display_name: "filter".into(),
        };

        let remapped = remap_filter(&filter, &[2, 7]).unwrap();
        let ExprKind::Binary { left, right, .. } = remapped.kind else {
            panic!("expected conjunction")
        };
        let ExprKind::Binary { left, .. } = left.kind else {
            panic!("expected comparison")
        };
        let ExprKind::Binary {
            left: right_column, ..
        } = right.kind
        else {
            panic!("expected comparison")
        };
        assert!(matches!(left.kind, ExprKind::Column(1)));
        assert!(matches!(right_column.kind, ExprKind::Column(0)));
    }

    #[test]
    fn rejects_filter_columns_missing_from_projection() {
        let error = remap_filter(&comparison(3, 10), &[0, 1]).unwrap_err();
        assert!(error.to_string().contains("expression column 3"));
    }
}
