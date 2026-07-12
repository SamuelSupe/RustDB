use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, NamedWindowDefinition, NamedWindowExpr, Query, SelectItem, Visit, Visitor,
    WindowFrameBound, WindowSpec, WindowType,
};

use super::{contains_aggregate, select_item_has_aggregate};

pub(crate) fn is_aggregate_query(
    group_exprs: &[Expr],
    projection: &[SelectItem],
    having: Option<&Expr>,
    qualify: Option<&Expr>,
    named_windows: &[NamedWindowDefinition],
) -> bool {
    !group_exprs.is_empty()
        || having.is_some()
        || projection.iter().any(select_item_has_aggregate)
        || qualify.is_some_and(contains_aggregate)
        || named_windows.iter().any(|definition| {
            named_window_has_aggregate(definition)
                && named_window_is_used(definition, projection, qualify)
        })
}

pub(super) fn window_type_has_aggregate(window: &WindowType) -> bool {
    match window {
        WindowType::WindowSpec(spec) => window_spec_has_aggregate(spec),
        WindowType::NamedWindow(_) => false,
    }
}

fn named_window_has_aggregate(definition: &NamedWindowDefinition) -> bool {
    let NamedWindowDefinition(_, NamedWindowExpr::WindowSpec(spec)) = definition else {
        return false;
    };
    window_spec_has_aggregate(spec)
}

fn named_window_is_used(
    definition: &NamedWindowDefinition,
    projection: &[SelectItem],
    qualify: Option<&Expr>,
) -> bool {
    let NamedWindowDefinition(name, _) = definition;
    projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            references_named_window(expr, &name.value)
        }
        _ => false,
    }) || qualify.is_some_and(|expr| references_named_window(expr, &name.value))
}

fn references_named_window(expr: &Expr, name: &str) -> bool {
    struct Finder<'a> {
        name: &'a str,
        query_depth: usize,
    }
    impl Visitor for Finder<'_> {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
            self.query_depth = self.query_depth.saturating_sub(1);
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            if self.query_depth == 0
                && matches!(
                    function.over.as_ref(),
                    Some(WindowType::NamedWindow(window))
                        if window.value.eq_ignore_ascii_case(self.name)
                )
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    matches!(
        expr.visit(&mut Finder {
            name,
            query_depth: 0,
        }),
        ControlFlow::Break(())
    )
}

fn window_spec_has_aggregate(spec: &WindowSpec) -> bool {
    spec.partition_by.iter().any(contains_aggregate)
        || spec
            .order_by
            .iter()
            .any(|order| contains_aggregate(&order.expr))
        || spec.window_frame.as_ref().is_some_and(|frame| {
            frame_bound_has_aggregate(&frame.start_bound)
                || frame
                    .end_bound
                    .as_ref()
                    .is_some_and(frame_bound_has_aggregate)
        })
}

fn frame_bound_has_aggregate(bound: &WindowFrameBound) -> bool {
    match bound {
        WindowFrameBound::Preceding(Some(expr)) | WindowFrameBound::Following(Some(expr)) => {
            contains_aggregate(expr)
        }
        WindowFrameBound::CurrentRow
        | WindowFrameBound::Preceding(None)
        | WindowFrameBound::Following(None) => false,
    }
}
