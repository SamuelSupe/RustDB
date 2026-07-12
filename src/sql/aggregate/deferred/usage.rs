use std::collections::HashSet;

use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem};

pub(super) struct Usage {
    pub outside_aggregate: HashSet<String>,
}

pub(super) fn collect(items: &[SelectItem], having: Option<&Expr>) -> Usage {
    let mut usage = Usage {
        outside_aggregate: HashSet::new(),
    };
    for item in items {
        if let SelectItem::UnnamedExpr(expression)
        | SelectItem::ExprWithAlias {
            expr: expression, ..
        } = item
        {
            visit(expression, false, &mut usage);
        }
    }
    if let Some(expression) = having {
        visit(expression, false, &mut usage);
    }
    usage
}

fn visit(expression: &Expr, inside_aggregate: bool, usage: &mut Usage) {
    match expression {
        Expr::Identifier(ident) if is_generated(&ident.value) => {
            if !inside_aggregate {
                usage.outside_aggregate.insert(ident.value.clone());
            }
        }
        Expr::Function(function) => {
            let nested = inside_aggregate || super::super::aggregate_function(function).is_some();
            let FunctionArguments::List(arguments) = &function.args else {
                return;
            };
            for argument in &arguments.args {
                let expression = match argument {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(expression))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(expression),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(expression),
                        ..
                    } => expression,
                    _ => continue,
                };
                visit(expression, nested, usage);
            }
        }
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
        } => {
            visit(left, inside_aggregate, usage);
            visit(right, inside_aggregate, usage);
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
        | Expr::Floor { expr, .. } => visit(expr, inside_aggregate, usage),
        Expr::Between {
            expr, low, high, ..
        } => {
            visit(expr, inside_aggregate, usage);
            visit(low, inside_aggregate, usage);
            visit(high, inside_aggregate, usage);
        }
        Expr::InList { expr, list, .. } => {
            visit(expr, inside_aggregate, usage);
            for candidate in list {
                visit(candidate, inside_aggregate, usage);
            }
        }
        Expr::InSubquery { expr, .. } => visit(expr, inside_aggregate, usage),
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(operand) = operand {
                visit(operand, inside_aggregate, usage);
            }
            for branch in conditions {
                visit(&branch.condition, inside_aggregate, usage);
                visit(&branch.result, inside_aggregate, usage);
            }
            if let Some(otherwise) = else_result {
                visit(otherwise, inside_aggregate, usage);
            }
        }
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            visit(expr, inside_aggregate, usage);
            if let Some(from) = substring_from {
                visit(from, inside_aggregate, usage);
            }
            if let Some(length) = substring_for {
                visit(length, inside_aggregate, usage);
            }
        }
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            visit(expr, inside_aggregate, usage);
            if let Some(what) = trim_what {
                visit(what, inside_aggregate, usage);
            }
            if let Some(characters) = trim_characters {
                for character in characters {
                    visit(character, inside_aggregate, usage);
                }
            }
        }
        _ => {}
    }
}

fn is_generated(name: &str) -> bool {
    name.starts_with("__rustdb_scalar_subquery_")
}
