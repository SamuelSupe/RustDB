use sqlparser::ast::Statement;

use crate::{Error, Result};

use super::{NativeWriteCommand, NativeWriteKind, SessionCommand};

pub(crate) enum MaintenanceCommand {
    Checkpoint,
    Vacuum { table: Option<String> },
    Analyze { table: Option<String> },
}

pub(super) fn parse_custom(sql: &str) -> Result<Option<SessionCommand>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.eq_ignore_ascii_case("checkpoint") {
        return Ok(Some(SessionCommand::Maintenance(
            MaintenanceCommand::Checkpoint,
        )));
    }
    let Some((first, rest)) = super::take_word(trimmed) else {
        return Ok(None);
    };
    if !first.eq_ignore_ascii_case("compact") {
        return Ok(None);
    }
    let Some((second, rest)) = super::take_word(rest) else {
        return Err(Error::InvalidArgument(
            "COMPACT requires a native table name".to_owned(),
        ));
    };
    let table = if second.eq_ignore_ascii_case("table") {
        rest.trim()
    } else {
        trimmed[first.len()..].trim()
    };
    if table.is_empty() {
        return Err(Error::InvalidArgument(
            "COMPACT requires a native table name".to_owned(),
        ));
    }
    let table = parse_name(table)?;
    let query = parse_query(&format!(
        "SELECT * FROM {}",
        crate::catalog_name::quote(&table)
    ))?;
    Ok(Some(SessionCommand::NativeWrite(NativeWriteCommand {
        qualifier: crate::catalog_name::full_qualifier(&table),
        name: table.to_ascii_lowercase(),
        query,
        kind: NativeWriteKind::Compact,
        returning: None,
    })))
}

pub(super) fn parse_statement(statement: &Statement) -> Result<Option<MaintenanceCommand>> {
    match statement {
        Statement::Analyze(analyze) => {
            if analyze.partitions.is_some()
                || analyze.for_columns
                || !analyze.columns.is_empty()
                || analyze.cache_metadata
                || analyze.noscan
                || analyze.compute_statistics
            {
                return Err(Error::Unsupported(
                    "ANALYZE supports an optional table name without modifiers".to_owned(),
                ));
            }
            Ok(Some(MaintenanceCommand::Analyze {
                table: analyze
                    .table_name
                    .as_ref()
                    .map(|name| super::simple_name(name, "table"))
                    .transpose()?,
            }))
        }
        Statement::Vacuum(vacuum) => {
            if vacuum.full
                || vacuum.sort_only
                || vacuum.delete_only
                || vacuum.reindex
                || vacuum.recluster
                || vacuum.threshold.is_some()
                || vacuum.boost
            {
                return Err(Error::Unsupported(
                    "VACUUM supports an optional table name without modifiers".to_owned(),
                ));
            }
            Ok(Some(MaintenanceCommand::Vacuum {
                table: vacuum
                    .table_name
                    .as_ref()
                    .map(|name| super::simple_name(name, "table"))
                    .transpose()?,
            }))
        }
        _ => Ok(None),
    }
}

fn parse_name(value: &str) -> Result<String> {
    let mut statements = crate::sql::parse_statements(&format!("DESCRIBE {value}"))?;
    let Statement::ExplainTable { table_name, .. } = statements.remove(0) else {
        return Err(Error::InvalidArgument(
            "COMPACT accepts one table name".to_owned(),
        ));
    };
    super::simple_name(&table_name, "table")
}

fn parse_query(sql: &str) -> Result<Box<sqlparser::ast::Query>> {
    let mut statements = crate::sql::parse_statements(sql)?;
    let Statement::Query(query) = statements.remove(0) else {
        return Err(Error::Internal(
            "generated COMPACT query did not parse".to_owned(),
        ));
    };
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::{MaintenanceCommand, parse_custom, parse_statement};
    use crate::command::SessionCommand;

    #[test]
    fn parses_checkpoint_compact_analyze_and_vacuum() {
        assert!(matches!(
            parse_custom("CHECKPOINT;").unwrap(),
            Some(SessionCommand::Maintenance(MaintenanceCommand::Checkpoint))
        ));
        assert!(matches!(
            parse_custom("COMPACT TABLE events").unwrap(),
            Some(SessionCommand::NativeWrite(_))
        ));
        for sql in ["ANALYZE events", "VACUUM events"] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse_statement(&statement).unwrap().is_some(), "{sql}");
        }
    }
}
