use std::collections::HashSet;

use sqlparser::ast::{
    AssignmentTarget, Expr, ObjectName, SelectItem, Statement, TableFactor, Update,
    UpdateTableFromKind,
};

use crate::{Error, Result};

pub(crate) struct NativeUpdateCommand {
    pub(crate) name: String,
    pub(crate) target_sql: String,
    pub(crate) qualifier: String,
    pub(crate) from_sql: Option<String>,
    pub(crate) assignments: Vec<(String, Expr)>,
    pub(crate) selection: Option<Box<Expr>>,
    pub(crate) returning: Option<Vec<SelectItem>>,
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeUpdateCommand>> {
    let Statement::Update(update) = statement else {
        return Ok(None);
    };
    parse_update(update).map(Some)
}

fn parse_update(update: &Update) -> Result<NativeUpdateCommand> {
    if !update.optimizer_hints.is_empty()
        || update.output.is_some()
        || update.or.is_some()
        || !update.order_by.is_empty()
        || update.limit.is_some()
        || !update.table.joins.is_empty()
    {
        return Err(unsupported());
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &update.table.relation
    else {
        return Err(unsupported());
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || update.assignments.is_empty()
    {
        return Err(unsupported());
    }
    let name = simple_name(name)?.to_ascii_lowercase();
    let qualifier = match alias {
        Some(alias) if alias.columns.is_empty() => alias.name.value.clone(),
        Some(_) => return Err(unsupported()),
        None => crate::catalog_name::full_qualifier(&name),
    };
    let from_sql = match &update.from {
        Some(UpdateTableFromKind::AfterSet(from)) => {
            let [source] = from.as_slice() else {
                return Err(unsupported());
            };
            if !source.joins.is_empty() {
                return Err(unsupported());
            }
            if update.selection.is_none() {
                return Err(Error::Unsupported(
                    "UPDATE FROM requires an equality join predicate".to_owned(),
                ));
            }
            Some(source.to_string())
        }
        Some(UpdateTableFromKind::BeforeSet(_)) => return Err(unsupported()),
        None => None,
    };
    let mut assigned = HashSet::with_capacity(update.assignments.len());
    let assignments = update
        .assignments
        .iter()
        .map(|assignment| {
            let AssignmentTarget::ColumnName(column) = &assignment.target else {
                return Err(Error::Unsupported(
                    "tuple assignment is not supported by native UPDATE".to_owned(),
                ));
            };
            let column = assignment_column(column, &qualifier)?;
            if !assigned.insert(column.clone()) {
                return Err(Error::InvalidArgument(format!(
                    "UPDATE assigns column '{column}' more than once"
                )));
            }
            Ok((column, assignment.value.clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(NativeUpdateCommand {
        name,
        target_sql: update.table.relation.to_string(),
        qualifier,
        from_sql,
        assignments,
        selection: update.selection.clone().map(Box::new),
        returning: update.returning.clone(),
    })
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
}

fn assignment_column(name: &ObjectName, target_qualifier: &str) -> Result<String> {
    let identifiers = name
        .0
        .iter()
        .map(|part| {
            part.as_ident().ok_or_else(|| {
                Error::Unsupported("UPDATE assignment targets must be identifiers".to_owned())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (column, qualifier) = identifiers.split_last().ok_or_else(unsupported)?;
    if qualifier.len() > 2 {
        return Err(Error::Unsupported(
            "UPDATE assignment targets support at most schema.table.column".to_owned(),
        ));
    }
    if !qualifier.is_empty() {
        let requested = qualifier
            .iter()
            .map(|identifier| identifier.value.as_str())
            .collect::<Vec<_>>()
            .join(".");
        if !crate::catalog_name::qualifier_matches(target_qualifier, &requested) {
            return Err(Error::Catalog(format!(
                "UPDATE assignment target '{name}' does not belong to '{target_qualifier}'"
            )));
        }
    }
    Ok(column.value.to_ascii_lowercase())
}

fn unsupported() -> Error {
    Error::Unsupported(
        "native UPDATE supports one target and optional single FROM source without ordering or limit modifiers"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn accepts_plain_update_and_update_from() {
        for sql in [
            "UPDATE t SET value = value + 1",
            "UPDATE t SET value = 2 WHERE id = 1 RETURNING id, value",
            "UPDATE t SET value = u.value FROM u WHERE t.id = u.id",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement).unwrap().is_some(), "{sql}");
        }
        let sql = "UPDATE t SET value = u.value FROM u";
        let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
        assert!(parse(&statement).is_err());
    }

    #[test]
    fn preserves_full_or_alias_target_qualifiers() {
        let statement = crate::sql::parse_statements(
            "UPDATE analytics.events SET analytics.events.value = analytics.events.value + 1",
        )
        .unwrap()
        .remove(0);
        let command = parse(&statement).unwrap().unwrap();
        assert_eq!(command.name, "analytics.events");
        assert_eq!(command.qualifier, "analytics.events");
        assert_eq!(command.assignments[0].0, "value");

        let statement = crate::sql::parse_statements(
            "UPDATE events AS e SET e.value = e.value + 1 WHERE e.id = 1 RETURNING e.*",
        )
        .unwrap()
        .remove(0);
        let command = parse(&statement).unwrap().unwrap();
        assert_eq!(command.name, "events");
        assert_eq!(command.qualifier, "e");
        assert_eq!(command.assignments[0].0, "value");
    }
}
