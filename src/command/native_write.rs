use sqlparser::ast::{
    ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableObject,
    helpers::stmt_create_table::CreateTableBuilder,
};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeWriteKind {
    Create,
    Replace,
    Append,
}

pub(crate) struct NativeWriteCommand {
    pub(crate) name: String,
    pub(crate) query: Box<Query>,
    pub(crate) kind: NativeWriteKind,
}

pub(super) fn parse(statement: &Statement, sql: &str) -> Result<Option<NativeWriteCommand>> {
    match statement {
        Statement::CreateTable(create) => {
            let query = create.query.clone().ok_or_else(|| {
                Error::Unsupported("native CREATE TABLE requires AS SELECT".to_owned())
            })?;
            let expected = CreateTableBuilder::new(create.name.clone())
                .or_replace(create.or_replace)
                .query(Some(query.clone()))
                .build();
            if create != &expected || !valid_create_prefix(sql, create.or_replace) {
                return Err(Error::Unsupported(
                    "native CREATE TABLE supports only CREATE [OR REPLACE] TABLE name AS SELECT"
                        .to_owned(),
                ));
            }
            Ok(Some(NativeWriteCommand {
                name: simple_name(&create.name)?.to_ascii_lowercase(),
                query,
                kind: if create.or_replace {
                    NativeWriteKind::Replace
                } else {
                    NativeWriteKind::Create
                },
            }))
        }
        Statement::Insert(insert) => {
            let TableObject::TableName(name) = &insert.table else {
                return Err(Error::Unsupported(
                    "INSERT target must be one native table name".to_owned(),
                ));
            };
            let source = insert.source.clone().ok_or_else(|| {
                Error::Unsupported("native INSERT requires a SELECT query".to_owned())
            })?;
            if matches!(source.body.as_ref(), SetExpr::Values(_)) || !plain_insert(insert) {
                return Err(Error::Unsupported(
                    "native INSERT supports only INSERT INTO name SELECT without modifiers"
                        .to_owned(),
                ));
            }
            Ok(Some(NativeWriteCommand {
                name: simple_name(name)?.to_ascii_lowercase(),
                query: source,
                kind: NativeWriteKind::Append,
            }))
        }
        _ => Ok(None),
    }
}

fn simple_name(name: &ObjectName) -> Result<String> {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return Err(Error::Unsupported(
            "qualified native table names are not supported".to_owned(),
        ));
    };
    Ok(identifier.value.clone())
}

fn valid_create_prefix(sql: &str, replace: bool) -> bool {
    let prefix = if replace {
        "CREATE OR REPLACE TABLE"
    } else {
        "CREATE TABLE"
    };
    sql.trim_start()
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn plain_insert(insert: &sqlparser::ast::Insert) -> bool {
    insert.optimizer_hints.is_empty()
        && insert.or.is_none()
        && !insert.ignore
        && insert.into
        && insert.table_alias.is_none()
        && insert.columns.is_empty()
        && !insert.overwrite
        && insert.assignments.is_empty()
        && insert.partitioned.is_none()
        && insert.after_columns.is_empty()
        && !insert.has_table_keyword
        && insert.on.is_none()
        && insert.returning.is_none()
        && insert.output.is_none()
        && !insert.replace_into
        && insert.priority.is_none()
        && insert.insert_alias.is_none()
        && insert.settings.is_none()
        && insert.format_clause.is_none()
        && insert.multi_table_insert_type.is_none()
        && insert.multi_table_into_clauses.is_empty()
        && insert.multi_table_when_clauses.is_empty()
        && insert.multi_table_else_clause.is_none()
}

#[cfg(test)]
mod tests {
    use super::{NativeWriteKind, parse};

    #[test]
    fn accepts_the_three_native_write_forms() {
        for (sql, kind) in [
            ("CREATE TABLE t AS SELECT 1 AS id", NativeWriteKind::Create),
            (
                "CREATE OR REPLACE TABLE t AS SELECT 2 AS id",
                NativeWriteKind::Replace,
            ),
            ("INSERT INTO t SELECT 3 AS id", NativeWriteKind::Append),
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            let command = parse(&statement, sql).unwrap().unwrap();
            assert_eq!(command.name, "t");
            assert_eq!(command.kind, kind);
        }
    }

    #[test]
    fn rejects_modifiers_and_insert_column_lists() {
        for sql in [
            "CREATE TEMP TABLE t AS SELECT 1",
            "CREATE TABLE IF NOT EXISTS t AS SELECT 1",
            "INSERT INTO t(id) SELECT 1",
            "INSERT OVERWRITE TABLE t SELECT 1",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement, sql).is_err(), "{sql}");
        }
    }
}
