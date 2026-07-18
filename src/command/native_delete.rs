use sqlparser::ast::{Delete, Expr, FromTable, ObjectName, SelectItem, Statement, TableFactor};

use crate::{Error, Result};

pub(crate) struct NativeDeleteCommand {
    pub(crate) name: String,
    pub(crate) target_sql: String,
    pub(crate) qualifier: String,
    pub(crate) using_sql: Option<String>,
    pub(crate) selection: Option<Box<Expr>>,
    pub(crate) returning: Option<Vec<SelectItem>>,
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeDeleteCommand>> {
    let Statement::Delete(delete) = statement else {
        return Ok(None);
    };
    parse_delete(delete).map(Some)
}

fn parse_delete(delete: &Delete) -> Result<NativeDeleteCommand> {
    if !delete.optimizer_hints.is_empty()
        || !delete.tables.is_empty()
        || delete.output.is_some()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(unsupported());
    }
    let FromTable::WithFromKeyword(from) = &delete.from else {
        return Err(unsupported());
    };
    let [source] = from.as_slice() else {
        return Err(unsupported());
    };
    if !source.joins.is_empty() {
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
    } = &source.relation
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
    {
        return Err(unsupported());
    }
    let name = simple_name(name)?.to_ascii_lowercase();
    let qualifier = match alias {
        Some(alias) if alias.columns.is_empty() => alias.name.value.clone(),
        Some(_) => return Err(unsupported()),
        None => crate::catalog_name::full_qualifier(&name),
    };
    let using_sql = match &delete.using {
        Some(using) => {
            let [source] = using.as_slice() else {
                return Err(unsupported());
            };
            if !source.joins.is_empty() {
                return Err(unsupported());
            }
            if delete.selection.is_none() {
                return Err(Error::Unsupported(
                    "DELETE USING requires an equality join predicate".to_owned(),
                ));
            }
            Some(source.to_string())
        }
        None => None,
    };
    Ok(NativeDeleteCommand {
        name,
        target_sql: source.relation.to_string(),
        qualifier,
        using_sql,
        selection: delete.selection.clone().map(Box::new),
        returning: delete.returning.clone(),
    })
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
}

fn unsupported() -> Error {
    Error::Unsupported(
        "native DELETE supports one target and optional single USING source without ordering or limit modifiers"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn accepts_plain_delete_and_using() {
        for sql in [
            "DELETE FROM t",
            "DELETE FROM t WHERE id = 2",
            "DELETE FROM t WHERE id = 2 RETURNING id",
            "DELETE FROM t USING u WHERE t.id = u.id",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            let command = parse(&statement).unwrap().unwrap();
            assert_eq!(command.name, "t");
        }
        for sql in ["DELETE FROM t USING u", "DELETE FROM t LIMIT 1"] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement).is_err(), "{sql}");
        }
    }

    #[test]
    fn preserves_full_or_alias_target_qualifiers() {
        let statement = crate::sql::parse_statements(
            "DELETE FROM analytics.events WHERE analytics.events.id = 1 RETURNING analytics.events.*",
        )
        .unwrap()
        .remove(0);
        let command = parse(&statement).unwrap().unwrap();
        assert_eq!(command.name, "analytics.events");
        assert_eq!(command.qualifier, "analytics.events");

        let statement =
            crate::sql::parse_statements("DELETE FROM events AS e WHERE e.id = 1 RETURNING e.*")
                .unwrap()
                .remove(0);
        let command = parse(&statement).unwrap().unwrap();
        assert_eq!(command.name, "events");
        assert_eq!(command.qualifier, "e");
    }
}
