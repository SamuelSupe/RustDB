use std::{collections::HashSet, sync::Arc};

mod walk;

use sqlparser::{
    ast::{
        Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, ObjectNamePart, Statement,
        TableFactor, Value,
    },
    dialect::DuckDbDialect,
    parser::Parser,
};
use uuid::Uuid;

use crate::{
    Catalog, CsvHeader, CsvOptions, EngineConfig, Error, ParquetOptions, Result, TableEntry,
    datasource::{CsvTable, MetadataCache, ParquetTable, TableProvider},
    runtime::QueryContext,
};

/// SQL rewritten to use query-local catalog providers.
///
/// `generated_tables` is the exact ownership list for this preparation.  The
/// caller must remove only these entries after binding the rewritten SQL.
pub(crate) struct PreparedSql {
    pub(crate) sql: String,
    pub(crate) generated_tables: Vec<String>,
}

#[cfg(test)]
pub(crate) async fn prepare(
    catalog: &Catalog,
    config: &EngineConfig,
    sql: &str,
) -> Result<PreparedSql> {
    let cache = MetadataCache::new(config.metadata_cache_bytes);
    prepare_with_cache(catalog, config, &cache, sql).await
}

#[cfg(test)]
pub(crate) async fn prepare_with_cache(
    catalog: &Catalog,
    config: &EngineConfig,
    metadata_cache: &MetadataCache,
    sql: &str,
) -> Result<PreparedSql> {
    prepare_with_cache_for_query(catalog, config, metadata_cache, sql, None).await
}

pub(crate) async fn prepare_with_cache_for_query(
    catalog: &Catalog,
    config: &EngineConfig,
    metadata_cache: &MetadataCache,
    sql: &str,
    context: Option<Arc<QueryContext>>,
) -> Result<PreparedSql> {
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "exactly one SQL statement is required".to_owned(),
        ));
    }
    let mut statement = statements.remove(0);
    let specs = collect_specs(&statement)?;
    if specs.is_empty() {
        return Ok(PreparedSql {
            sql: sql.to_owned(),
            generated_tables: Vec::new(),
        });
    }

    // Build every provider before mutating the session catalog. A failed file
    // leaves no partially registered generated tables behind.
    let mut prepared = Vec::with_capacity(specs.len());
    let mut names = HashSet::with_capacity(specs.len());
    for spec in specs {
        let provider = build_provider(spec, config, metadata_cache, context.clone()).await?;
        let name = generated_name(catalog, &mut names);
        prepared.push((name, provider));
    }

    let rewritten = rewrite_statement(
        &mut statement,
        &mut prepared.iter().map(|(name, _)| name.clone()),
    )?;
    if rewritten != prepared.len() {
        return Err(Error::Internal(
            "file table-function rewrite count changed during preparation".to_owned(),
        ));
    }
    let generated_tables = prepared
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    let mut registered: Vec<String> = Vec::with_capacity(generated_tables.len());
    for (name, provider) in prepared {
        if let Err(error) = catalog.register(TableEntry::new(name.clone(), provider)) {
            for registered_name in registered {
                catalog.unregister(&registered_name);
            }
            return Err(error);
        }
        registered.push(name);
    }
    Ok(PreparedSql {
        sql: statement.to_string(),
        generated_tables,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileKind {
    Csv,
    Parquet,
}

#[derive(Clone, Debug)]
enum FileSpec {
    Csv {
        location: String,
        options: CsvOptions,
    },
    Parquet {
        location: String,
        options: ParquetOptions,
    },
}

fn collect_specs(statement: &Statement) -> Result<Vec<FileSpec>> {
    let mut specs = Vec::new();
    walk::visit(statement, &mut |factor| {
        if let Some(spec) = parse_factor(factor)? {
            specs.push(spec);
        }
        Ok(())
    })?;
    Ok(specs)
}

fn parse_factor(factor: &TableFactor) -> Result<Option<FileSpec>> {
    let TableFactor::Table {
        name,
        args: Some(arguments),
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = factor
    else {
        return Ok(None);
    };
    let Some(kind) = file_kind(name) else {
        return Ok(None);
    };
    if !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || arguments.settings.is_some()
    {
        return Err(Error::Unsupported(format!(
            "modifiers on {} are not supported",
            function_name(kind)
        )));
    }

    let mut location = None;
    let mut named = Vec::new();
    let mut seen = HashSet::new();
    for argument in &arguments.args {
        match argument {
            FunctionArg::Unnamed(argument) => {
                if location.is_some() {
                    return Err(Error::InvalidArgument(format!(
                        "{} accepts exactly one positional location",
                        function_name(kind)
                    )));
                }
                location = Some(literal_string(argument, "location")?);
            }
            FunctionArg::Named { name, arg, .. } => {
                push_named(&mut named, &mut seen, &name.value, arg)?;
            }
            FunctionArg::ExprNamed { name, arg, .. } => {
                let Expr::Identifier(name) = name else {
                    return Err(Error::InvalidArgument(format!(
                        "{} argument names must be identifiers",
                        function_name(kind)
                    )));
                };
                push_named(&mut named, &mut seen, &name.value, arg)?;
            }
        }
    }
    let location = location.ok_or_else(|| {
        Error::InvalidArgument(format!(
            "{} requires one positional location",
            function_name(kind)
        ))
    })?;

    match kind {
        FileKind::Csv => {
            let mut options = CsvOptions::default();
            for (name, value) in named {
                match name.as_str() {
                    "header" => options.header = csv_header(value)?,
                    "delimiter" => options.delimiter = delimiter(value)?,
                    "sample_size" => options.sample_size = positive_usize(value, "sample_size")?,
                    _ => return Err(unknown_argument(kind, &name)),
                }
            }
            Ok(Some(FileSpec::Csv { location, options }))
        }
        FileKind::Parquet => {
            let mut options = ParquetOptions::default();
            for (name, value) in named {
                match name.as_str() {
                    "union_by_name" => options.union_by_name = boolean(value, &name)?,
                    "hive_partitioning" => {
                        options.hive_partitioning = hive_partitioning(value)?;
                    }
                    _ => return Err(unknown_argument(kind, &name)),
                }
            }
            Ok(Some(FileSpec::Parquet { location, options }))
        }
    }
}

fn push_named<'a>(
    named: &mut Vec<(String, &'a FunctionArgExpr)>,
    seen: &mut HashSet<String>,
    name: &str,
    value: &'a FunctionArgExpr,
) -> Result<()> {
    let name = name.to_ascii_lowercase();
    if is_secret_argument(&name) {
        return Err(Error::InvalidArgument(format!(
            "credential argument '{name}' is not allowed in SQL; use the configured credential provider"
        )));
    }
    if !seen.insert(name.clone()) {
        return Err(Error::InvalidArgument(format!(
            "duplicate table-function argument '{name}'"
        )));
    }
    named.push((name, value));
    Ok(())
}

async fn build_provider(
    spec: FileSpec,
    config: &EngineConfig,
    metadata_cache: &MetadataCache,
    context: Option<Arc<QueryContext>>,
) -> Result<Arc<dyn TableProvider>> {
    match spec {
        FileSpec::Csv { location, options } => Ok(Arc::new(
            CsvTable::try_new_for_query(vec![location], options, config, context).await?,
        )),
        FileSpec::Parquet { location, options } => Ok(Arc::new(
            ParquetTable::try_new_with_cache_for_query(
                vec![location],
                options,
                config,
                metadata_cache.clone(),
                context,
            )
            .await?,
        )),
    }
}

fn rewrite_statement(
    statement: &mut Statement,
    replacements: &mut impl Iterator<Item = String>,
) -> Result<usize> {
    let mut rewritten = 0;
    walk::visit_mut(statement, &mut |factor| {
        rewritten += usize::from(rewrite_factor(factor, replacements)?);
        Ok(())
    })?;
    Ok(rewritten)
}

fn rewrite_factor(
    factor: &mut TableFactor,
    replacements: &mut impl Iterator<Item = String>,
) -> Result<bool> {
    let TableFactor::Table { name, args, .. } = factor else {
        return Ok(false);
    };
    if args.is_none() || file_kind(name).is_none() {
        return Ok(false);
    }
    let replacement = replacements.next().ok_or_else(|| {
        Error::Internal("missing generated table name during file-function rewrite".to_owned())
    })?;
    *name = Ident::new(replacement).into();
    *args = None;
    Ok(true)
}

fn file_kind(name: &ObjectName) -> Option<FileKind> {
    let [ObjectNamePart::Identifier(name)] = name.0.as_slice() else {
        return None;
    };
    if name.value.eq_ignore_ascii_case("read_csv") {
        Some(FileKind::Csv)
    } else if name.value.eq_ignore_ascii_case("read_parquet") {
        Some(FileKind::Parquet)
    } else {
        None
    }
}

fn function_name(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Csv => "read_csv",
        FileKind::Parquet => "read_parquet",
    }
}

fn literal_value<'a>(argument: &'a FunctionArgExpr, name: &str) -> Result<&'a Value> {
    let FunctionArgExpr::Expr(Expr::Value(value)) = argument else {
        return Err(Error::InvalidArgument(format!(
            "table-function argument '{name}' must be a literal"
        )));
    };
    Ok(&value.value)
}

fn literal_string(argument: &FunctionArgExpr, name: &str) -> Result<String> {
    literal_value(argument, name)?
        .clone()
        .into_string()
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "table-function argument '{name}' must be a string literal"
            ))
        })
}

fn boolean(argument: &FunctionArgExpr, name: &str) -> Result<bool> {
    let value = literal_value(argument, name)?;
    if let Value::Boolean(value) = value {
        return Ok(*value);
    }
    match value.clone().into_string().as_deref() {
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(Error::InvalidArgument(format!(
            "table-function argument '{name}' must be TRUE or FALSE"
        ))),
    }
}

fn csv_header(argument: &FunctionArgExpr) -> Result<CsvHeader> {
    let value = literal_value(argument, "header")?;
    if let Value::Boolean(value) = value {
        return Ok(if *value {
            CsvHeader::Present
        } else {
            CsvHeader::Absent
        });
    }
    match value.clone().into_string().as_deref() {
        Some(value) if value.eq_ignore_ascii_case("auto") => Ok(CsvHeader::Auto),
        Some(value)
            if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("present") =>
        {
            Ok(CsvHeader::Present)
        }
        Some(value)
            if value.eq_ignore_ascii_case("false") || value.eq_ignore_ascii_case("absent") =>
        {
            Ok(CsvHeader::Absent)
        }
        _ => Err(Error::InvalidArgument(
            "CSV header must be AUTO, TRUE/PRESENT, or FALSE/ABSENT".to_owned(),
        )),
    }
}

fn delimiter(argument: &FunctionArgExpr) -> Result<u8> {
    let delimiter = literal_string(argument, "delimiter")?;
    let bytes = delimiter.as_bytes();
    if bytes.len() != 1 || !bytes[0].is_ascii() {
        return Err(Error::InvalidArgument(
            "CSV delimiter must be exactly one ASCII byte".to_owned(),
        ));
    }
    Ok(bytes[0])
}

fn positive_usize(argument: &FunctionArgExpr, name: &str) -> Result<usize> {
    let Value::Number(value, _) = literal_value(argument, name)? else {
        return Err(Error::InvalidArgument(format!(
            "table-function argument '{name}' must be a positive integer"
        )));
    };
    let value: usize = value.parse().map_err(|_| {
        Error::InvalidArgument(format!("table-function argument '{name}' is out of range"))
    })?;
    if value == 0 {
        return Err(Error::InvalidArgument(format!(
            "table-function argument '{name}' must be greater than zero"
        )));
    }
    Ok(value)
}

fn hive_partitioning(argument: &FunctionArgExpr) -> Result<bool> {
    let value = literal_value(argument, "hive_partitioning")?;
    if let Value::Boolean(value) = value {
        return Ok(*value);
    }
    match value.clone().into_string().as_deref() {
        Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        Some(value) if value.eq_ignore_ascii_case("auto") => Ok(true),
        _ => Err(Error::InvalidArgument(
            "Parquet hive_partitioning must be AUTO, TRUE, or FALSE".to_owned(),
        )),
    }
}

fn unknown_argument(kind: FileKind, name: &str) -> Error {
    if is_secret_argument(name) {
        Error::InvalidArgument(format!(
            "credential argument '{name}' is not allowed in SQL; use the configured credential provider"
        ))
    } else {
        Error::InvalidArgument(format!("unknown {} argument '{name}'", function_name(kind)))
    }
}

fn is_secret_argument(name: &str) -> bool {
    let compact = name.replace('_', "");
    compact.contains("secret")
        || compact.contains("credential")
        || compact.contains("password")
        || compact.contains("token")
        || compact.contains("accesskey")
        || compact == "key"
}

fn generated_name(catalog: &Catalog, generated: &mut HashSet<String>) -> String {
    loop {
        let name = format!("__rustdb_file_{}", Uuid::new_v4().simple());
        if catalog.table(&name).is_none() && generated.insert(name.clone()) {
            return name;
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};

    use super::{FileSpec, collect_specs, parse_factor, prepare};
    use crate::{Catalog, CsvHeader, EngineConfig, Error};

    fn factor(sql: &str) -> sqlparser::ast::TableFactor {
        let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql).expect("parse SQL");
        let Statement::Query(query) = statements.remove(0) else {
            panic!("query expected");
        };
        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select expected");
        };
        select.from[0].relation.clone()
    }

    #[test]
    fn parses_supported_csv_and_parquet_options() {
        let csv = factor(
            "SELECT * FROM read_csv('data.csv', header = 'present', delimiter = '|', sample_size = 42)",
        );
        let Some(FileSpec::Csv { options, .. }) = parse_factor(&csv).expect("CSV spec") else {
            panic!("CSV spec expected");
        };
        assert_eq!(options.header, CsvHeader::Present);
        assert_eq!(options.delimiter, b'|');
        assert_eq!(options.sample_size, 42);

        let parquet = factor(
            "SELECT * FROM read_parquet('data.parquet', union_by_name = true, hive_partitioning = 'auto')",
        );
        let Some(FileSpec::Parquet { options, .. }) = parse_factor(&parquet).expect("Parquet spec")
        else {
            panic!("Parquet spec expected");
        };
        assert!(options.union_by_name);
        assert!(options.hive_partitioning);
    }

    #[test]
    fn rejects_unknown_and_credential_arguments_before_io() {
        let unknown = factor("SELECT * FROM read_csv('missing.csv', typo = true)");
        assert!(matches!(
            parse_factor(&unknown),
            Err(Error::InvalidArgument(message)) if message.contains("unknown read_csv argument")
        ));

        let secret = factor(
            "SELECT * FROM read_parquet('s3://bucket/data.parquet', secret_access_key = 'x')",
        );
        assert!(matches!(
            parse_factor(&secret),
            Err(Error::InvalidArgument(message)) if message.contains("credential argument")
        ));
    }

    #[test]
    fn discovers_file_functions_inside_scalar_subqueries() {
        let mut statements = Parser::parse_sql(
            &DuckDbDialect {},
            "SELECT (SELECT count(*) FROM read_parquet('inner.parquet')) \
             FROM read_csv('outer.csv')",
        )
        .unwrap();
        let specs = collect_specs(&statements.remove(0)).unwrap();
        assert_eq!(specs.len(), 2);
    }

    #[tokio::test]
    async fn prepares_csv_registers_generated_table_and_preserves_alias() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("data.csv");
        std::fs::write(&path, "id,name\n1,alice\n2,bob\n").expect("write CSV");
        let sql = format!(
            "SELECT source.id FROM read_csv('{}', header = true) AS source",
            path.display()
        );
        let catalog = Catalog::default();

        let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
            .await
            .expect("prepare file function");
        let names = catalog.table_names();
        assert_eq!(names.len(), 1);
        assert!(names[0].starts_with("__rustdb_file_"));
        assert_eq!(prepared.generated_tables, names);
        assert!(prepared.sql.contains(&names[0]));
        assert!(prepared.sql.contains("source"));
        assert!(!prepared.sql.to_ascii_lowercase().contains("read_csv"));
    }

    #[tokio::test]
    async fn rewrites_from_and_join_inside_explain_query() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("join.csv");
        std::fs::write(&path, "id\n1\n2\n").expect("write CSV");
        let sql = format!(
            "EXPLAIN SELECT * FROM read_csv('{}', header = true) AS left_file \
             INNER JOIN read_csv('{}', header = true) AS right_file \
             ON left_file.id = right_file.id",
            path.display(),
            path.display()
        );
        let catalog = Catalog::default();

        let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
            .await
            .expect("prepare join functions");
        assert_eq!(catalog.table_names().len(), 2);
        assert_eq!(prepared.generated_tables.len(), 2);
        assert!(prepared.sql.starts_with("EXPLAIN"));
        assert!(prepared.sql.contains("left_file"));
        assert!(prepared.sql.contains("right_file"));
        assert!(!prepared.sql.to_ascii_lowercase().contains("read_csv"));
    }

    #[tokio::test]
    async fn rewrites_file_functions_inside_ctes_and_derived_tables() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("nested.csv");
        std::fs::write(&path, "id\n1\n2\n").expect("write CSV");
        let sql = format!(
            "WITH base AS (SELECT * FROM read_csv('{}', header = true)) \
             SELECT nested.id FROM base \
             JOIN (SELECT * FROM read_csv('{}', header = true)) nested \
             ON base.id = nested.id",
            path.display(),
            path.display(),
        );
        let catalog = Catalog::default();

        let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
            .await
            .expect("prepare nested file functions");
        assert_eq!(prepared.generated_tables.len(), 2);
        assert!(!prepared.sql.to_ascii_lowercase().contains("read_csv"));
        crate::sql::plan_sql(&catalog, &prepared.sql).expect("plan rewritten nested query");
    }

    #[tokio::test]
    async fn requires_exactly_one_statement() {
        let result = prepare(
            &Catalog::default(),
            &EngineConfig::default(),
            "SELECT 1; SELECT 2",
        )
        .await;
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }
}
