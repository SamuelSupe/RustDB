use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::ast::Values;

use crate::{Error, Result};

use super::super::super::{LogicalPlan, PlanSchema};
use crate::sql::{
    binder::bind_expr,
    coercion::{cast_if_needed, common_case_type},
};

pub(super) fn plan_values(values: &Values) -> Result<LogicalPlan> {
    let Some(first) = values.rows.first() else {
        return Err(Error::InvalidArgument(
            "VALUES requires at least one row".to_owned(),
        ));
    };
    let width = first.len();
    if width == 0 {
        return Err(Error::Unsupported(
            "zero-column VALUES rows are not supported".to_owned(),
        ));
    }
    if values.rows.iter().any(|row| row.len() != width) {
        return Err(Error::InvalidArgument(
            "VALUES rows must have the same number of columns".to_owned(),
        ));
    }

    let input_schema = PlanSchema::empty();
    let mut rows = values
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|expression| bind_expr(expression, &input_schema))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut types = vec![DataType::Null; width];
    for row in &rows {
        for (data_type, expression) in types.iter_mut().zip(row) {
            *data_type = common_case_type(data_type, &expression.data_type)?;
        }
    }
    let schema = PlanSchema::unqualified(Arc::new(Schema::new(
        types
            .iter()
            .enumerate()
            .map(|(index, data_type)| {
                Field::new(format!("column{}", index + 1), data_type.clone(), true)
            })
            .collect::<Vec<_>>(),
    )));
    let inputs = rows
        .iter_mut()
        .map(|row| LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Empty {
                produce_one_row: true,
                schema: PlanSchema::empty(),
            }),
            expressions: row
                .drain(..)
                .zip(&types)
                .map(|(expression, data_type)| cast_if_needed(expression, data_type))
                .collect(),
            schema: schema.clone(),
        })
        .collect::<Vec<_>>();
    Ok(if inputs.len() == 1 {
        inputs.into_iter().next().expect("one VALUES row")
    } else {
        LogicalPlan::Append { inputs, schema }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_value_rows_to_one_common_schema() {
        let statement = crate::sql::parse_statements("VALUES (1, NULL), (2, 'x')")
            .unwrap()
            .remove(0);
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query");
        };
        let sqlparser::ast::SetExpr::Values(values) = query.body.as_ref() else {
            panic!("expected VALUES");
        };
        let plan = plan_values(values).unwrap();
        assert_eq!(plan.schema().arrow().field(0).data_type(), &DataType::Int64);
        assert_eq!(plan.schema().arrow().field(1).data_type(), &DataType::Utf8);
    }
}
