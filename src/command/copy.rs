use sqlparser::ast::{CopyOption, CopySource, CopyTarget, Query, Statement};

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyFormat {
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
pub(crate) struct CopyCsvOptions {
    pub(crate) delimiter: u8,
    pub(crate) header: Option<bool>,
    pub(crate) quote: u8,
    pub(crate) escape: Option<u8>,
    pub(crate) null: Option<String>,
}

impl Default for CopyCsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            header: None,
            quote: b'"',
            escape: None,
            null: None,
        }
    }
}

pub(crate) struct CopyToCommand {
    pub(crate) query: Box<Query>,
    pub(crate) location: String,
    pub(crate) format: CopyFormat,
    pub(crate) csv: CopyCsvOptions,
}

pub(crate) enum CopyCommand {
    From(super::NativeWriteCommand),
    To(CopyToCommand),
}

pub(super) fn parse(statement: &Statement) -> Result<Option<CopyCommand>> {
    let Statement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        values,
    } = statement
    else {
        return Ok(None);
    };
    if !legacy_options.is_empty() || !values.is_empty() {
        return Err(Error::Unsupported(
            "COPY legacy options and inline STDIN data are not supported".to_owned(),
        ));
    }
    let CopyTarget::File { filename } = target else {
        return Err(Error::Unsupported(
            "COPY supports a local, file://, or s3:// file target only".to_owned(),
        ));
    };
    let (format, csv) = copy_options(filename, options)?;
    if *to {
        let query = copy_to_query(source)?;
        return Ok(Some(CopyCommand::To(CopyToCommand {
            query,
            location: filename.clone(),
            format,
            csv,
        })));
    }

    let CopySource::Table {
        table_name,
        columns,
    } = source
    else {
        return Err(Error::Unsupported(
            "COPY FROM requires one native table target".to_owned(),
        ));
    };
    if !columns.is_empty() {
        return Err(Error::Unsupported(
            "COPY FROM column lists are not supported in v0.8 alpha.3".to_owned(),
        ));
    }
    if csv.null.is_some() {
        return Err(Error::Unsupported(
            "COPY FROM NULL strings are not supported; normalize the source or use INSERT SELECT"
                .to_owned(),
        ));
    }
    let name = super::simple_name(table_name, "table")?.to_ascii_lowercase();
    let query = copy_from_query(filename, format, &csv)?;
    Ok(Some(CopyCommand::From(super::NativeWriteCommand {
        qualifier: crate::catalog_name::full_qualifier(&name),
        name,
        query,
        kind: super::NativeWriteKind::CopyFrom,
        returning: None,
        import: None,
    })))
}

fn copy_options(location: &str, options: &[CopyOption]) -> Result<(CopyFormat, CopyCsvOptions)> {
    let mut format = None;
    let mut csv = CopyCsvOptions::default();
    let mut seen = std::collections::HashSet::new();
    for option in options {
        let key = match option {
            CopyOption::Format(_) => "format",
            CopyOption::Delimiter(_) => "delimiter",
            CopyOption::Header(_) => "header",
            CopyOption::Quote(_) => "quote",
            CopyOption::Escape(_) => "escape",
            CopyOption::Null(_) => "null",
            CopyOption::Encoding(_) => "encoding",
            _ => {
                return Err(Error::Unsupported(format!(
                    "COPY option {option} is not supported"
                )));
            }
        };
        if !seen.insert(key) {
            return Err(Error::InvalidArgument(format!(
                "COPY option {key} was specified more than once"
            )));
        }
        match option {
            CopyOption::Format(value) => format = Some(parse_format(&value.value)?),
            CopyOption::Delimiter(value) => csv.delimiter = ascii_byte(*value, "DELIMITER")?,
            CopyOption::Header(value) => csv.header = Some(*value),
            CopyOption::Quote(value) => csv.quote = ascii_byte(*value, "QUOTE")?,
            CopyOption::Escape(value) => csv.escape = Some(ascii_byte(*value, "ESCAPE")?),
            CopyOption::Null(value) => csv.null = Some(value.clone()),
            CopyOption::Encoding(value)
                if value.eq_ignore_ascii_case("utf8") || value.eq_ignore_ascii_case("utf-8") => {}
            CopyOption::Encoding(_) => {
                return Err(Error::Unsupported(
                    "COPY supports UTF-8 encoding only".to_owned(),
                ));
            }
            _ => unreachable!("unsupported options returned above"),
        }
    }
    let format = format.unwrap_or_else(|| infer_format(location));
    if format == CopyFormat::Parquet
        && (csv.delimiter != b','
            || csv.header.is_some()
            || csv.quote != b'"'
            || csv.escape.is_some()
            || csv.null.is_some())
    {
        return Err(Error::InvalidArgument(
            "CSV-specific COPY options cannot be used with FORMAT PARQUET".to_owned(),
        ));
    }
    Ok((format, csv))
}

fn parse_format(value: &str) -> Result<CopyFormat> {
    match value.to_ascii_lowercase().as_str() {
        "csv" => Ok(CopyFormat::Csv),
        "parquet" => Ok(CopyFormat::Parquet),
        _ => Err(Error::Unsupported(format!(
            "COPY format '{value}' is not supported"
        ))),
    }
}

fn infer_format(location: &str) -> CopyFormat {
    let lower = location.to_ascii_lowercase();
    if lower.ends_with(".parquet") || lower.ends_with(".pq") {
        CopyFormat::Parquet
    } else {
        CopyFormat::Csv
    }
}

fn ascii_byte(value: char, option: &str) -> Result<u8> {
    u8::try_from(u32::from(value))
        .map_err(|_| Error::InvalidArgument(format!("COPY {option} must be one ASCII byte")))
}

fn copy_to_query(source: &CopySource) -> Result<Box<Query>> {
    match source {
        CopySource::Query(query) => Ok(query.clone()),
        CopySource::Table {
            table_name,
            columns,
        } => {
            let name = super::simple_name(table_name, "table")?;
            let projection = if columns.is_empty() {
                "*".to_owned()
            } else {
                columns
                    .iter()
                    .map(|column| quote_identifier(&column.value))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            parse_query(&format!(
                "SELECT {projection} FROM {}",
                crate::catalog_name::quote(&name)
            ))
        }
    }
}

fn copy_from_query(location: &str, format: CopyFormat, csv: &CopyCsvOptions) -> Result<Box<Query>> {
    let location = quote_string(location);
    let sql = match format {
        CopyFormat::Parquet => format!("SELECT * FROM read_parquet({location})"),
        CopyFormat::Csv => {
            let mut options = vec![format!("delimiter = {}", quote_byte(csv.delimiter))];
            if let Some(header) = csv.header {
                options.push(format!("header = {header}"));
            }
            if csv.quote != b'"' {
                options.push(format!("quote = {}", quote_byte(csv.quote)));
            }
            if let Some(escape) = csv.escape {
                options.push(format!("escape = {}", quote_byte(escape)));
            }
            format!("SELECT * FROM read_csv({location}, {})", options.join(", "))
        }
    };
    parse_query(&sql)
}

fn parse_query(sql: &str) -> Result<Box<Query>> {
    let mut statements = crate::sql::parse_statements(sql)?;
    let Statement::Query(query) = statements.remove(0) else {
        return Err(Error::Internal(
            "generated COPY query did not parse as a query".to_owned(),
        ));
    };
    Ok(query)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_byte(value: u8) -> String {
    quote_string(&char::from(value).to_string())
}

#[cfg(test)]
mod tests {
    use super::{CopyCommand, CopyFormat, parse};

    fn command(sql: &str) -> CopyCommand {
        let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
        parse(&statement).unwrap().unwrap()
    }

    #[test]
    fn parses_copy_from_csv_and_parquet() {
        let CopyCommand::From(csv) =
            command("COPY events FROM '/tmp/events.csv' (FORMAT CSV, HEADER TRUE, DELIMITER '|')")
        else {
            panic!("COPY FROM CSV did not produce a native write");
        };
        assert_eq!(csv.name, "events");
        assert!(csv.query.to_string().contains("read_csv"));

        let CopyCommand::From(parquet) = command("COPY events FROM '/tmp/events.parquet'") else {
            panic!("COPY FROM Parquet did not produce a native write");
        };
        assert!(parquet.query.to_string().contains("read_parquet"));
    }

    #[test]
    fn parses_copy_query_to_csv() {
        let CopyCommand::To(copy) =
            command("COPY (SELECT id FROM events) TO '/tmp/events.csv' (FORMAT CSV, HEADER FALSE)")
        else {
            panic!("COPY TO did not produce an export command");
        };
        assert_eq!(copy.format, CopyFormat::Csv);
        assert_eq!(copy.csv.header, Some(false));
    }

    #[test]
    fn rejects_column_list_import_and_parquet_csv_options() {
        for sql in [
            "COPY events(id) FROM '/tmp/events.csv'",
            "COPY events TO '/tmp/events.parquet' (HEADER TRUE)",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement).is_err(), "{sql}");
        }
    }
}
