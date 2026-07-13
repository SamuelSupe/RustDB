use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use sqlparser::{ast::Statement, dialect::DuckDbDialect, parser::Parser};

use super::{FileSpec, GeneratedTablesGuard, collect_specs, parse_factor, prepare};
use crate::{Catalog, CsvCompression, CsvHeader, EngineConfig, Error, ParquetSchemaMode};

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
        "SELECT * FROM read_csv('data.csv', header = 'present', delimiter = '|', sample_size = 42, compression = 'zstd')",
    );
    let Some(FileSpec::Csv { options, .. }) = parse_factor(&csv).expect("CSV spec") else {
        panic!("CSV spec expected");
    };
    assert_eq!(options.header, CsvHeader::Present);
    assert_eq!(options.delimiter, b'|');
    assert_eq!(options.sample_size, 42);
    assert_eq!(options.compression, CsvCompression::Zstd);

    let parquet = factor(
        "SELECT * FROM read_parquet('data.parquet', union_by_name = true, hive_partitioning = 'auto')",
    );
    let Some(FileSpec::Parquet { options, .. }) = parse_factor(&parquet).expect("Parquet spec")
    else {
        panic!("Parquet spec expected");
    };
    assert!(options.union_by_name);
    assert!(options.hive_partitioning);

    let widening =
        factor("SELECT * FROM read_parquet('data.parquet', schema_mode = 'safe_widening')");
    let Some(FileSpec::Parquet { options, .. }) = parse_factor(&widening).unwrap() else {
        panic!("Parquet spec expected");
    };
    assert_eq!(options.schema_mode, ParquetSchemaMode::SafeWidening);
}

#[test]
fn rejects_conflicting_parquet_schema_options() {
    let factor = factor(
        "SELECT * FROM read_parquet('data.parquet', union_by_name = true, schema_mode = 'union')",
    );
    assert!(matches!(
        parse_factor(&factor),
        Err(Error::InvalidArgument(message)) if message.contains("conflict")
    ));
}

#[test]
fn rejects_unknown_and_credential_arguments_before_io() {
    let unknown = factor("SELECT * FROM read_csv('missing.csv', typo = true)");
    assert!(matches!(
        parse_factor(&unknown),
        Err(Error::InvalidArgument(message)) if message.contains("unknown read_csv argument")
    ));

    let secret =
        factor("SELECT * FROM read_parquet('s3://bucket/data.parquet', secret_access_key = 'x')");
    assert!(matches!(
        parse_factor(&secret),
        Err(Error::InvalidArgument(message)) if message.contains("credential argument")
    ));

    let compression = factor("SELECT * FROM read_csv('missing.csv', compression = 'zip')");
    assert!(matches!(
        parse_factor(&compression),
        Err(Error::InvalidArgument(message)) if message.contains("CSV compression")
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
    let rewritten = prepared.statement.to_string();
    assert!(rewritten.contains(&names[0]));
    assert!(rewritten.contains("source"));
    assert!(!rewritten.to_ascii_lowercase().contains("read_csv"));
    let guard = GeneratedTablesGuard::new(&catalog, prepared.generated_tables);
    drop(guard);
    assert!(catalog.table_names().is_empty());
}

#[tokio::test]
async fn reuses_providers_only_for_identical_file_specs() {
    let temp = tempfile::tempdir().expect("tempdir");
    let csv_path = temp.path().join("shared.csv");
    std::fs::write(&csv_path, "id\n1\n2\n").expect("write CSV");

    let catalog = Catalog::default();
    let sql = format!(
        "SELECT * FROM read_csv('{}', header = true) a \
         JOIN read_csv('{}', header = true) b ON a.id = b.id",
        csv_path.display(),
        csv_path.display(),
    );
    let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
        .await
        .expect("prepare identical CSV specs");
    let first = catalog.table(&prepared.generated_tables[0]).unwrap();
    let second = catalog.table(&prepared.generated_tables[1]).unwrap();
    assert!(Arc::ptr_eq(first.provider(), second.provider()));

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let parquet_path = temp.path().join("shared.parquet");
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&parquet_path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let catalog = Catalog::default();
    let sql = format!(
        "SELECT * FROM read_parquet('{}') a \
         JOIN read_parquet('{}') b ON a.id = b.id",
        parquet_path.display(),
        parquet_path.display(),
    );
    let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
        .await
        .expect("prepare identical Parquet specs");
    let first = catalog.table(&prepared.generated_tables[0]).unwrap();
    let second = catalog.table(&prepared.generated_tables[1]).unwrap();
    assert!(Arc::ptr_eq(first.provider(), second.provider()));

    let catalog = Catalog::default();
    let sql = format!(
        "SELECT * FROM read_csv('{}', header = true) a \
         JOIN read_csv('{}', header = false) b ON true",
        csv_path.display(),
        csv_path.display(),
    );
    let prepared = prepare(&catalog, &EngineConfig::default(), &sql)
        .await
        .expect("prepare distinct CSV specs");
    let first = catalog.table(&prepared.generated_tables[0]).unwrap();
    let second = catalog.table(&prepared.generated_tables[1]).unwrap();
    assert!(!Arc::ptr_eq(first.provider(), second.provider()));
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
    let rewritten = prepared.statement.to_string();
    assert!(rewritten.starts_with("EXPLAIN"));
    assert!(rewritten.contains("left_file"));
    assert!(rewritten.contains("right_file"));
    assert!(!rewritten.to_ascii_lowercase().contains("read_csv"));
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
    let rewritten = prepared.statement.to_string();
    assert!(!rewritten.to_ascii_lowercase().contains("read_csv"));
    crate::sql::plan_sql(&catalog, &rewritten).expect("plan rewritten nested query");
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
