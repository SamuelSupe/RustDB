use std::{collections::HashSet, ops::ControlFlow, sync::Arc};

mod walk;

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, ObjectNamePart, Statement, TableFactor,
    Value, Visit, Visitor,
};
use uuid::Uuid;

use crate::{
    Catalog, CsvCompression, CsvHeader, CsvOptions, EngineConfig, Error, ParquetOptions,
    ParquetSchemaMode, Result, TableEntry,
    datasource::{CsvTable, MetadataCache, ParquetTable, TableProvider},
    runtime::QueryContext,
};

/// Statement rewritten to use query-local catalog providers.
///
/// `generated_tables` is the exact ownership list for this preparation.  The
/// caller must remove only these entries after binding the rewritten
/// statement. Keeping the parsed AST avoids losing source spans by formatting
/// and reparsing the SQL after table-function replacement.
pub(crate) struct PreparedSql {
    pub(crate) statement: Statement,
    pub(crate) generated_tables: Vec<String>,
}

pub(crate) struct GeneratedTablesGuard {
    catalog: Catalog,
    names: Vec<String>,
}

impl GeneratedTablesGuard {
    pub(crate) fn new(catalog: &Catalog, names: Vec<String>) -> Self {
        Self {
            catalog: catalog.clone(),
            names,
        }
    }
}

impl Drop for GeneratedTablesGuard {
    fn drop(&mut self) {
        for name in &self.names {
            self.catalog.unregister(name);
        }
    }
}

pub(crate) fn reject_parameterized_file_functions(statement: &Statement) -> Result<()> {
    walk::visit(statement, &mut |factor| {
        let TableFactor::Table {
            name,
            args: Some(arguments),
            ..
        } = factor
        else {
            return Ok(());
        };
        if file_kind(name).is_none() {
            return Ok(());
        }
        if arguments.args.iter().any(argument_has_placeholder) {
            return Err(Error::InvalidArgument(
                "prepared parameters are not allowed in read_csv/read_parquet arguments".into(),
            ));
        }
        Ok(())
    })
}

fn argument_has_placeholder(argument: &FunctionArg) -> bool {
    let expression = match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression))
        | FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        }
        | FunctionArg::ExprNamed {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,
        _ => return false,
    };
    struct Finder;
    impl Visitor for Finder {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            if matches!(expression, Expr::Value(value) if matches!(value.value, Value::Placeholder(_)))
            {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }
    matches!(expression.visit(&mut Finder), ControlFlow::Break(()))
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
    let mut statements = crate::sql::parse_statements(sql)?;
    if statements.len() != 1 {
        return Err(Error::InvalidArgument(
            "exactly one SQL statement is required".to_owned(),
        ));
    }
    let statement = statements.remove(0);
    prepare_statement_with_cache_for_query(catalog, config, metadata_cache, statement, context)
        .await
}

pub(crate) async fn prepare_statement_with_cache_for_query(
    catalog: &Catalog,
    config: &EngineConfig,
    metadata_cache: &MetadataCache,
    mut statement: Statement,
    context: Option<Arc<QueryContext>>,
) -> Result<PreparedSql> {
    let specs = collect_specs(&statement)?;
    if specs.is_empty() {
        return Ok(PreparedSql {
            statement,
            generated_tables: Vec::new(),
        });
    }

    // Build every provider before mutating the session catalog. A failed file
    // leaves no partially registered generated tables behind.
    let mut prepared = Vec::with_capacity(specs.len());
    let mut shared = Vec::<(FileSpec, Arc<dyn TableProvider>)>::new();
    let mut names = HashSet::with_capacity(specs.len());
    for spec in specs {
        let provider = if let Some((_, provider)) = shared.iter().find(|(seen, _)| seen == &spec) {
            Arc::clone(provider)
        } else {
            let provider =
                build_provider(spec.clone(), config, metadata_cache, context.clone()).await?;
            shared.push((spec, Arc::clone(&provider)));
            provider
        };
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
        statement,
        generated_tables,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileKind {
    Csv,
    Parquet,
}

#[derive(Clone, Debug, PartialEq)]
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
                    "quote" => options.quote = csv_byte(value, "quote")?,
                    "escape" => options.escape = Some(csv_byte(value, "escape")?),
                    "sample_size" => options.sample_size = positive_usize(value, "sample_size")?,
                    "compression" => options.compression = csv_compression(value)?,
                    _ => return Err(unknown_argument(kind, &name)),
                }
            }
            Ok(Some(FileSpec::Csv { location, options }))
        }
        FileKind::Parquet => {
            let mut options = ParquetOptions::default();
            let mut union_by_name_set = false;
            let mut schema_mode_set = false;
            for (name, value) in named {
                match name.as_str() {
                    "union_by_name" => {
                        union_by_name_set = true;
                        options.union_by_name = boolean(value, &name)?;
                    }
                    "schema_mode" => {
                        schema_mode_set = true;
                        options.schema_mode = parquet_schema_mode(value)?;
                    }
                    "hive_partitioning" => {
                        options.hive_partitioning = hive_partitioning(value)?;
                    }
                    _ => return Err(unknown_argument(kind, &name)),
                }
            }
            if union_by_name_set && schema_mode_set {
                return Err(Error::InvalidArgument(
                    "read_parquet arguments union_by_name and schema_mode conflict".to_owned(),
                ));
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
    csv_byte(argument, "delimiter")
}

fn csv_byte(argument: &FunctionArgExpr, name: &str) -> Result<u8> {
    let value = literal_string(argument, name)?;
    let bytes = value.as_bytes();
    if bytes.len() != 1 || !bytes[0].is_ascii() {
        return Err(Error::InvalidArgument(format!(
            "CSV {name} must be exactly one ASCII byte"
        )));
    }
    Ok(bytes[0])
}

fn csv_compression(argument: &FunctionArgExpr) -> Result<CsvCompression> {
    match literal_string(argument, "compression")?
        .to_ascii_lowercase()
        .as_str()
    {
        "auto" => Ok(CsvCompression::Auto),
        "none" | "uncompressed" => Ok(CsvCompression::None),
        "gzip" | "gz" => Ok(CsvCompression::Gzip),
        "zstd" | "zst" => Ok(CsvCompression::Zstd),
        value => Err(Error::InvalidArgument(format!(
            "CSV compression must be AUTO, NONE, GZIP, or ZSTD; found '{value}'"
        ))),
    }
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

fn parquet_schema_mode(argument: &FunctionArgExpr) -> Result<ParquetSchemaMode> {
    let value = literal_string(argument, "schema_mode")?;
    match value.to_ascii_lowercase().as_str() {
        "strict" => Ok(ParquetSchemaMode::Strict),
        "union" | "union_by_name" => Ok(ParquetSchemaMode::UnionByName),
        "safe_widening" => Ok(ParquetSchemaMode::SafeWidening),
        _ => Err(Error::InvalidArgument(
            "Parquet schema_mode must be 'strict', 'union', or 'safe_widening'".to_owned(),
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
#[path = "table_function/tests.rs"]
mod tests;
