use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectName, ObjectNamePart, Statement, TableFactor, Visit, Visitor};

use crate::{Error, Result, command::ParsedStatement};

/// Validation boundary for the public, read-only HTTP query service.
pub struct HttpReadOnlyPolicy;

impl HttpReadOnlyPolicy {
    /// Rejects every operation outside the HTTP service allowlist before it
    /// can reach binding or execution.
    pub fn validate(sql: &str) -> Result<()> {
        let parsed = crate::command::parse(sql)?;
        match &parsed {
            ParsedStatement::Command(command) if allowed_command(command) => Ok(()),
            ParsedStatement::Query(statement) if allowed_query(statement) => {
                reject_file_functions(statement)
            }
            _ => Err(rejected()),
        }
    }
}

fn allowed_command(command: &crate::command::SessionCommand) -> bool {
    matches!(
        command,
        crate::command::SessionCommand::ShowTables
            | crate::command::SessionCommand::ShowSchemas
            | crate::command::SessionCommand::Describe { .. }
    )
}

fn allowed_query(statement: &Statement) -> bool {
    match statement {
        Statement::Query(_) => true,
        Statement::Explain { statement, .. } => matches!(statement.as_ref(), Statement::Query(_)),
        _ => false,
    }
}

fn reject_file_functions(statement: &Statement) -> Result<()> {
    struct FileFunctionVisitor;

    impl Visitor for FileFunctionVisitor {
        type Break = ();

        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
            let name = match factor {
                TableFactor::Table {
                    name,
                    args: Some(_),
                    ..
                }
                | TableFactor::Function { name, .. } => name,
                _ => return ControlFlow::Continue(()),
            };
            if is_file_function(name) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            if matches!(expression, Expr::Function(function) if is_file_function(&function.name)) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    match statement.visit(&mut FileFunctionVisitor) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(()) => Err(Error::Unsupported(
            "HTTP queries cannot access read_csv/read_parquet; use a server-registered source"
                .to_owned(),
        )),
    }
}

fn is_file_function(name: &ObjectName) -> bool {
    let Some(ObjectNamePart::Identifier(identifier)) = name.0.last() else {
        return false;
    };
    matches!(
        identifier.value.to_ascii_lowercase().as_str(),
        "read_csv" | "read_csv_auto" | "read_parquet"
    )
}

fn rejected() -> Error {
    Error::Unsupported(
        "HTTP queries allow SELECT, WITH, VALUES, SHOW, DESCRIBE, EXPLAIN, and EXPLAIN ANALYZE only"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::HttpReadOnlyPolicy;

    #[test]
    fn accepts_the_read_only_statement_allowlist() {
        for sql in [
            "SELECT 1",
            "WITH values_cte AS (VALUES (1)) SELECT * FROM values_cte",
            "VALUES (1), (2)",
            "SHOW TABLES",
            "SHOW SCHEMAS",
            "DESCRIBE events",
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE SELECT 1",
        ] {
            HttpReadOnlyPolicy::validate(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }

    #[test]
    fn rejects_commands_and_nested_file_function_bypasses() {
        for sql in [
            "CREATE TABLE events AS SELECT 1",
            "INSERT INTO events VALUES (1)",
            "DELETE FROM events",
            "UPDATE events SET value = 1",
            "COPY events TO 'events.csv'",
            "CREATE TEMP VIEW exposed AS SELECT 1",
            "REFRESH TABLE events",
            "BEGIN READ ONLY",
            "WITH leaked AS (SELECT * FROM read_csv('secret.csv')) SELECT * FROM leaked",
            "SELECT * FROM main.read_parquet('secret.parquet')",
            "EXPLAIN INSERT INTO events VALUES (1)",
            "SELECT 1; DROP TABLE events",
        ] {
            assert!(
                HttpReadOnlyPolicy::validate(sql).is_err(),
                "policy unexpectedly allowed {sql}"
            );
        }
    }
}
