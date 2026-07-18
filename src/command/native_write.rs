use sqlparser::ast::{
    ColumnOption, DataType, ObjectName, Query, SelectItem, Statement, TableObject,
    helpers::stmt_create_table::CreateTableBuilder,
};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeWriteKind {
    Create,
    Replace,
    Append,
    CopyFrom,
    Compact,
    Alter,
}

pub(crate) struct NativeWriteCommand {
    pub(crate) name: String,
    pub(crate) qualifier: String,
    pub(crate) query: Box<Query>,
    pub(crate) kind: NativeWriteKind,
    pub(crate) returning: Option<Vec<SelectItem>>,
}

pub(super) fn parse(statement: &Statement, sql: &str) -> Result<Option<NativeWriteCommand>> {
    match statement {
        Statement::CreateTable(create) => {
            let name = simple_name(&create.name)?.to_ascii_lowercase();
            let query = match &create.query {
                Some(query) => query.clone(),
                None => empty_schema_query(&create.columns)?,
            };
            let mut expected =
                CreateTableBuilder::new(create.name.clone()).or_replace(create.or_replace);
            if create.query.is_some() {
                expected = expected.query(Some(query.clone()));
            } else {
                expected = expected.columns(create.columns.clone());
            }
            if create != &expected.build() || !valid_create_prefix(sql, create.or_replace) {
                return Err(Error::Unsupported(
                    "native CREATE TABLE supports a plain column list or CREATE [OR REPLACE] TABLE name AS SELECT"
                        .to_owned(),
                ));
            }
            Ok(Some(NativeWriteCommand {
                qualifier: crate::catalog_name::full_qualifier(&name),
                name,
                query,
                kind: if create.or_replace {
                    NativeWriteKind::Replace
                } else {
                    NativeWriteKind::Create
                },
                returning: None,
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
            if !plain_insert(insert) {
                return Err(Error::Unsupported(
                    "native INSERT supports INSERT INTO name SELECT or VALUES without modifiers"
                        .to_owned(),
                ));
            }
            let name = simple_name(name)?.to_ascii_lowercase();
            let qualifier = insert
                .table_alias
                .as_ref()
                .map(|alias| alias.alias.value.clone())
                .unwrap_or_else(|| crate::catalog_name::full_qualifier(&name));
            Ok(Some(NativeWriteCommand {
                name,
                qualifier,
                query: source,
                kind: NativeWriteKind::Append,
                returning: insert.returning.clone(),
            }))
        }
        _ => Ok(None),
    }
}

fn empty_schema_query(columns: &[sqlparser::ast::ColumnDef]) -> Result<Box<Query>> {
    if columns.is_empty() {
        return Err(Error::Unsupported(
            "native CREATE TABLE requires at least one column or AS SELECT".to_owned(),
        ));
    }
    let mut names = std::collections::HashSet::new();
    let mut projections = Vec::with_capacity(columns.len());
    for column in columns {
        let name = column.name.value.to_ascii_lowercase();
        if !names.insert(name.clone()) {
            return Err(Error::Catalog(format!("duplicate column '{name}'")));
        }
        if column.data_type == DataType::Unspecified
            || column.options.iter().any(|option| {
                option.name.is_some() || !matches!(&option.option, ColumnOption::Null)
            })
        {
            return Err(Error::Unsupported(
                "native CREATE TABLE column definitions support nullable typed columns without defaults or constraints"
                    .to_owned(),
            ));
        }
        projections.push(format!(
            "CAST(NULL AS {}) AS {}",
            column.data_type,
            quote_identifier(&column.name.value)
        ));
    }
    let sql = format!("SELECT {} WHERE FALSE", projections.join(", "));
    let mut statements = crate::sql::parse_statements(&sql)?;
    let Statement::Query(query) = statements.remove(0) else {
        return Err(Error::Internal(
            "generated CREATE TABLE schema query did not parse".to_owned(),
        ));
    };
    Ok(query)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
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
        && insert.columns.is_empty()
        && !insert.overwrite
        && insert.assignments.is_empty()
        && insert.partitioned.is_none()
        && insert.after_columns.is_empty()
        && !insert.has_table_keyword
        && insert.on.is_none()
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
            ("INSERT INTO t VALUES (4)", NativeWriteKind::Append),
            (
                "INSERT INTO t VALUES (5) RETURNING *",
                NativeWriteKind::Append,
            ),
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
            "CREATE TABLE t (id BIGINT NOT NULL)",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement, sql).is_err(), "{sql}");
        }
    }

    #[test]
    fn accepts_a_plain_nullable_schema() {
        let statement = crate::sql::parse_statements("CREATE TABLE t (id BIGINT, name VARCHAR)")
            .unwrap()
            .remove(0);
        let command = parse(&statement, "CREATE TABLE t (id BIGINT, name VARCHAR)")
            .unwrap()
            .unwrap();
        assert_eq!(command.kind, NativeWriteKind::Create);
    }

    #[test]
    fn preserves_insert_returning_target_qualifier() {
        for (sql, expected) in [
            (
                "INSERT INTO analytics.events VALUES (1) RETURNING analytics.events.*",
                "analytics.events",
            ),
            (
                "INSERT INTO sales.events VALUES (1) RETURNING sales.events.*",
                "sales.events",
            ),
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            let command = parse(&statement, sql).unwrap().unwrap();
            assert_eq!(command.qualifier, expected, "{sql}");
        }
    }
}
