use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::Schema;
use sqlparser::ast::TableAliasColumnDef;

use crate::{Error, Result};

use super::{BoundExpr, LogicalPlan, PlanSchema};

pub(super) type CteScope = HashMap<String, LogicalPlan>;

pub(super) fn cte_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

pub(super) fn alias_plan(
    input: LogicalPlan,
    relation: Option<&str>,
    column_aliases: &[TableAliasColumnDef],
) -> Result<LogicalPlan> {
    let width = input.schema().arrow().fields().len();
    if !column_aliases.is_empty() && column_aliases.len() != width {
        return Err(Error::InvalidArgument(format!(
            "relation alias provides {} column names for a {width}-column query",
            column_aliases.len()
        )));
    }
    if column_aliases.iter().any(|alias| alias.data_type.is_some()) {
        return Err(Error::Unsupported(
            "typed relation column aliases are not supported".into(),
        ));
    }
    if relation.is_none() && column_aliases.is_empty() {
        return Ok(input);
    }

    let fields = input
        .schema()
        .arrow()
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = column_aliases
                .get(index)
                .map(|alias| alias.name.value.as_str())
                .unwrap_or_else(|| field.name());
            Arc::new(field.as_ref().clone().with_name(name))
        })
        .collect::<Vec<_>>();
    let arrow = Arc::new(Schema::new_with_metadata(
        fields,
        input.schema().arrow().metadata().clone(),
    ));
    let expressions = arrow
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            BoundExpr::column(index, field.data_type().clone(), field.name().clone())
        })
        .collect();
    let schema = PlanSchema::new(arrow, vec![relation.map(ToOwned::to_owned); width]);
    Ok(LogicalPlan::Projection {
        input: Box::new(input),
        expressions,
        schema,
    })
}
