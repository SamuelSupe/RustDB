use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Query,
    SetExpr, Visit, Visitor,
};

use crate::sql::{JoinType, LogicalPlan};

/// Selects one direct IN/EXISTS term from a top-level AND so it can be lowered
/// before later subqueries are attached. The term may appear at any position,
/// and repeated calls stage multiple independent terms in source order.
///
/// OR/CASE and potentially multi-row scalar subqueries deliberately remain on
/// the existing guarded extraction path. The caller still verifies that the
/// extracted term produced a direct Mark/DependentJoin attachment before
/// committing the speculative plan.
pub(in crate::sql) fn stageable_direct_mark_term(predicate: &Expr) -> Option<(Expr, Expr)> {
    let mut terms = Vec::new();
    split_top_level_and(predicate, &mut terms);
    if terms.len() < 2 {
        return None;
    }
    terms.iter().enumerate().find_map(|(index, candidate)| {
        if !direct_mark(candidate) {
            return None;
        }
        let remainder = join_with_and(
            terms
                .iter()
                .enumerate()
                .filter(|(position, _)| *position != index)
                .map(|(_, term)| (*term).clone()),
        )?;
        scalar_cardinality_is_bounded(&remainder).then(|| ((*candidate).clone(), remainder))
    })
}

/// Confirms that speculative extraction produced exactly one direct marker.
/// Correlated markers are converted to filter-form DependentJoin nodes by
/// `apply_where`, so they can be staged without retaining an intermediate
/// marker column while later terms are planned.
pub(in crate::sql) fn is_direct_mark_attachment(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Join {
            on,
            join_type: JoinType::Mark,
            schema,
            ..
        } if on.is_empty()
            && schema
                .arrow()
                .fields()
                .last()
                .is_some_and(|field| field.name().starts_with("__rustdb_scalar_subquery_"))
    ) || matches!(
        plan,
        LogicalPlan::DependentJoin {
            kind: crate::sql::DependentJoinKind::Exists
                | crate::sql::DependentJoinKind::In { .. },
            guard: None,
            schema,
            ..
        } if schema
            .arrow()
            .fields()
            .last()
            .is_some_and(|field| field.name().starts_with("__rustdb_scalar_subquery_"))
    )
}

fn split_top_level_and<'a>(expr: &'a Expr, output: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        split_top_level_and(left, output);
        split_top_level_and(right, output);
    } else {
        output.push(expr);
    }
}

fn join_with_and(expressions: impl IntoIterator<Item = Expr>) -> Option<Expr> {
    expressions
        .into_iter()
        .reduce(|left, right| Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        })
}

fn direct_mark(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { expr, .. } => !contains_scalar_subquery(expr),
        Expr::Exists { .. } => true,
        Expr::Nested(expr) => direct_mark(expr),
        _ => false,
    }
}

/// Protect DuckDB's scalar cardinality rule: a false direct marker must not
/// hide a later scalar subquery that can return multiple rows. Global
/// aggregates and scalar SELECTs without a FROM source are bounded to at most
/// one row and are the only scalar shapes staged in v0.3.
fn scalar_cardinality_is_bounded(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(query) => query_is_at_most_one_row(query),
        // A nested relation can itself contain cardinality-producing scalar
        // expressions. Keep these compound forms on the guarded path.
        Expr::Exists { subquery, .. } => !query_contains_scalar_subquery(subquery),
        Expr::InSubquery { expr, subquery, .. } => {
            !contains_scalar_subquery(expr) && !query_contains_scalar_subquery(subquery)
        }
        Expr::AnyOp { .. } | Expr::AllOp { .. } => false,
        Expr::BinaryOp { left, right, .. }
        | Expr::Like {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::ILike {
            expr: left,
            pattern: right,
            ..
        }
        | Expr::SimilarTo {
            expr: left,
            pattern: right,
            ..
        } => scalar_cardinality_is_bounded(left) && scalar_cardinality_is_bounded(right),
        Expr::Between {
            expr, low, high, ..
        } => {
            scalar_cardinality_is_bounded(expr)
                && scalar_cardinality_is_bounded(low)
                && scalar_cardinality_is_bounded(high)
        }
        Expr::InList { expr, list, .. } => {
            scalar_cardinality_is_bounded(expr) && list.iter().all(scalar_cardinality_is_bounded)
        }
        Expr::UnaryOp { expr, .. }
        | Expr::Nested(expr)
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::IsTrue(expr)
        | Expr::IsNotTrue(expr)
        | Expr::IsFalse(expr)
        | Expr::IsNotFalse(expr)
        | Expr::IsUnknown(expr)
        | Expr::IsNotUnknown(expr)
        | Expr::Cast { expr, .. }
        | Expr::Extract { expr, .. }
        | Expr::Ceil { expr, .. }
        | Expr::Floor { expr, .. } => scalar_cardinality_is_bounded(expr),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            operand.as_deref().is_none_or(scalar_cardinality_is_bounded)
                && conditions.iter().all(|branch| {
                    scalar_cardinality_is_bounded(&branch.condition)
                        && scalar_cardinality_is_bounded(&branch.result)
                })
                && else_result
                    .as_deref()
                    .is_none_or(scalar_cardinality_is_bounded)
        }
        Expr::Function(function) => match &function.args {
            FunctionArguments::List(arguments) => {
                arguments.args.iter().all(|argument| match argument {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(expr),
                        ..
                    } => scalar_cardinality_is_bounded(expr),
                    _ => true,
                })
            }
            FunctionArguments::Subquery(_) => false,
            FunctionArguments::None => true,
        },
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            scalar_cardinality_is_bounded(expr)
                && substring_from
                    .as_deref()
                    .is_none_or(scalar_cardinality_is_bounded)
                && substring_for
                    .as_deref()
                    .is_none_or(scalar_cardinality_is_bounded)
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            scalar_cardinality_is_bounded(expr)
                && trim_what
                    .as_deref()
                    .is_none_or(scalar_cardinality_is_bounded)
                && trim_characters
                    .as_deref()
                    .is_none_or(|values| values.iter().all(scalar_cardinality_is_bounded))
        }
        _ => true,
    }
}

fn query_contains_scalar_subquery(query: &Query) -> bool {
    ast_contains_scalar_subquery(query)
}

fn select_contains_scalar_subquery(select: &sqlparser::ast::Select) -> bool {
    // Visit the complete query block so QUALIFY and inline/named window specs
    // cannot hide a cardinality-producing scalar from speculative staging.
    ast_contains_scalar_subquery(select)
}

fn contains_scalar_subquery(expr: &Expr) -> bool {
    ast_contains_scalar_subquery(expr)
}

fn ast_contains_scalar_subquery(ast: &impl Visit) -> bool {
    struct FindScalar;

    impl Visitor for FindScalar {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if matches!(expr, Expr::Subquery(_)) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    matches!(ast.visit(&mut FindScalar), ControlFlow::Break(()))
}

fn query_is_at_most_one_row(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    if select_contains_scalar_subquery(select) {
        return false;
    }
    let GroupByExpr::Expressions(groups, modifiers) = &select.group_by else {
        return false;
    };
    if !modifiers.is_empty() || !groups.is_empty() {
        return false;
    }
    select.from.is_empty()
        || super::super::aggregate::is_aggregate_query(
            groups,
            &select.projection,
            select.having.as_ref(),
            select.qualify.as_ref(),
            &select.named_window,
        )
}

#[cfg(test)]
mod tests {
    use sqlparser::{
        ast::{SetExpr, Statement},
        dialect::DuckDbDialect,
        parser::Parser,
    };

    use super::stageable_direct_mark_term;

    #[test]
    fn stages_only_direct_marks_before_cardinality_bounded_remainders() {
        let aggregate = selection(
            "SELECT 1 WHERE 1 IN (SELECT key FROM rhs) \
             AND 2 > (SELECT sum(value) FROM detail)",
        );
        assert!(stageable_direct_mark_term(&aggregate).is_some());

        let multi_row = selection(
            "SELECT 1 WHERE 1 IN (SELECT key FROM rhs) \
             AND 2 > (SELECT value FROM detail)",
        );
        assert!(stageable_direct_mark_term(&multi_row).is_none());

        let expression_context = selection(
            "SELECT 1 WHERE (1 IN (SELECT key FROM rhs) OR TRUE) \
             AND 2 > (SELECT sum(value) FROM detail)",
        );
        assert!(stageable_direct_mark_term(&expression_context).is_none());
    }

    #[test]
    fn stages_a_direct_marker_at_any_top_level_and_position() {
        let rightmost = selection(
            "SELECT 1 WHERE active = 1 \
             AND key NOT IN (SELECT key FROM rhs)",
        );
        let (marker, remainder) =
            stageable_direct_mark_term(&rightmost).expect("rightmost marker should stage");
        assert!(matches!(
            marker,
            sqlparser::ast::Expr::InSubquery { negated: true, .. }
        ));
        assert!(stageable_direct_mark_term(&remainder).is_none());

        let two_markers = selection(
            "SELECT 1 WHERE active = 1 \
             AND EXISTS (SELECT 1 FROM first_rhs WHERE first_rhs.key = key) \
             AND NOT EXISTS (SELECT 1 FROM second_rhs WHERE second_rhs.key = key)",
        );
        let (_, remainder) =
            stageable_direct_mark_term(&two_markers).expect("first marker should stage");
        assert!(
            stageable_direct_mark_term(&remainder).is_some(),
            "the second top-level marker should remain independently stageable"
        );
    }

    #[test]
    fn does_not_stage_around_a_potentially_multi_row_scalar() {
        for predicate in [
            "key IN (SELECT key FROM rhs) AND 1 = (SELECT value FROM detail)",
            "1 = (SELECT value FROM detail) AND key IN (SELECT key FROM rhs)",
        ] {
            let predicate = selection(&format!("SELECT 1 WHERE {predicate}"));
            assert!(stageable_direct_mark_term(&predicate).is_none());
        }
    }

    #[test]
    fn does_not_stage_past_scalars_in_qualify_or_window_specs() {
        for scalar in [
            "SELECT count(*) FROM detail \
             QUALIFY row_number() OVER () = (SELECT value FROM multi)",
            "SELECT count(*) FROM detail \
             WINDOW w AS (ORDER BY (SELECT value FROM multi)) \
             QUALIFY row_number() OVER w = 1",
            "SELECT count(*), \
                    row_number() OVER (ORDER BY (SELECT value FROM multi)) \
             FROM detail",
        ] {
            let predicate = selection(&format!(
                "SELECT 1 WHERE key IN (SELECT key FROM rhs) \
                 AND 1 = ({scalar})"
            ));
            assert!(
                stageable_direct_mark_term(&predicate).is_none(),
                "scalar cardinality in `{scalar}` must prevent direct-marker staging"
            );
        }
    }

    fn selection(sql: &str) -> sqlparser::ast::Expr {
        let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql).unwrap();
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT")
        };
        select.selection.clone().expect("WHERE expression")
    }
}
