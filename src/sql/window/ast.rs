use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Function, SelectItem, Visit, VisitMut, Visitor, VisitorMut};

use crate::{Error, Result};

#[derive(Clone)]
pub(super) struct WindowCall {
    pub(super) expression: Expr,
    pub(super) function: Function,
}

pub(crate) fn contains_window(expr: &Expr) -> bool {
    struct Finder {
        query_depth: usize,
    }
    impl Visitor for Finder {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth = self.query_depth.saturating_sub(1);
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.query_depth == 0
                && matches!(expr, Expr::Function(function) if function.over.is_some())
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    matches!(
        expr.visit(&mut Finder { query_depth: 0 }),
        ControlFlow::Break(())
    )
}

pub(super) fn collect_window_calls(
    projection: &[SelectItem],
    qualify: Option<&Expr>,
) -> Result<Vec<WindowCall>> {
    struct Collector {
        calls: Vec<WindowCall>,
        window_depth: usize,
        query_depth: usize,
    }
    impl Visitor for Collector {
        type Break = Error;

        fn pre_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth += 1;
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            self.query_depth = self.query_depth.saturating_sub(1);
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.query_depth != 0 {
                return ControlFlow::Continue(());
            }
            let Expr::Function(function) = expr else {
                return ControlFlow::Continue(());
            };
            if function.over.is_none() {
                return ControlFlow::Continue(());
            }
            if self.window_depth != 0 {
                return ControlFlow::Break(Error::Unsupported(
                    "nested window functions are not supported".into(),
                ));
            }
            self.window_depth += 1;
            if !self.calls.iter().any(|call| call.expression == *expr) {
                self.calls.push(WindowCall {
                    expression: expr.clone(),
                    function: function.clone(),
                });
            }
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if self.query_depth == 0
                && matches!(expr, Expr::Function(function) if function.over.is_some())
            {
                self.window_depth = self.window_depth.saturating_sub(1);
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        calls: Vec::new(),
        window_depth: 0,
        query_depth: 0,
    };
    for item in projection {
        let expression = match item {
            SelectItem::UnnamedExpr(expression)
            | SelectItem::ExprWithAlias {
                expr: expression, ..
            } => expression,
            _ => continue,
        };
        if let ControlFlow::Break(error) = expression.visit(&mut collector) {
            return Err(error);
        }
    }
    if let Some(qualify) = qualify
        && let ControlFlow::Break(error) = qualify.visit(&mut collector)
    {
        return Err(error);
    }
    Ok(collector.calls)
}

pub(super) fn collect_functions(expr: &Expr, output: &mut Vec<Function>) {
    struct Collector<'a>(&'a mut Vec<Function>);
    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Expr::Function(function) = expr
                && function.over.is_none()
            {
                self.0.push(function.clone());
            }
            ControlFlow::Continue(())
        }
    }
    let _ = expr.visit(&mut Collector(output));
}

pub(super) fn rewrite_expression(
    expr: &mut Expr,
    groups: &[(Expr, String)],
    aggregates: &[(Expr, String)],
    windows: &[(Expr, String)],
) {
    struct Rewriter<'a> {
        groups: &'a [(Expr, String)],
        aggregates: &'a [(Expr, String)],
        windows: &'a [(Expr, String)],
    }
    impl VisitorMut for Rewriter<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
            let replacement = self
                .windows
                .iter()
                .chain(self.aggregates)
                .chain(self.groups)
                .find_map(|(candidate, name)| (candidate == expr).then_some(name));
            if let Some(name) = replacement {
                *expr = Expr::Identifier(sqlparser::ast::Ident::new(name));
            }
            ControlFlow::Continue(())
        }
    }
    let _ = expr.visit(&mut Rewriter {
        groups,
        aggregates,
        windows,
    });
}
