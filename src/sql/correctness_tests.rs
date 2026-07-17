use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::{
    Catalog, Result, TableEntry,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    execution::execute,
    runtime::{MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream},
};

use super::plan_sql;

#[tokio::test]
async fn truth_predicates_are_never_null() {
    let batches = run(
        &Catalog::default(),
        "SELECT NULL IS TRUE, NULL IS NOT TRUE, NULL IS FALSE, \
         NULL IS NOT FALSE, NULL IS UNKNOWN, NULL IS NOT UNKNOWN, \
         TRUE IS TRUE, TRUE IS NOT TRUE, FALSE IS FALSE, \
         FALSE IS NOT FALSE, 1 IS TRUE, 0 IS FALSE",
    )
    .await;
    let expected = [
        false, true, false, true, true, false, true, false, true, false, true, true,
    ];
    for (column, expected) in batches[0].columns().iter().zip(expected) {
        let column = column.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!column.is_null(0));
        assert_eq!(column.value(0), expected);
    }
}

#[tokio::test]
async fn resolves_group_by_ordinals_and_aliases() {
    let catalog = grouped_catalog();
    for group in ["1", "total"] {
        let sql = format!(
            "SELECT a + b AS total, count(*) AS n FROM grouped \
             GROUP BY {group} ORDER BY 1"
        );
        let batches = run(&catalog, &sql).await;
        assert_eq!(int64_pairs(&batches), vec![(11, 1), (12, 1), (21, 1)]);
    }
}

#[tokio::test]
async fn group_by_ordinal_resolves_after_wildcard_expansion() {
    let catalog = table_catalog("single", vec![("a", vec![2, 1, 1])]);
    let batches = run(
        &catalog,
        "SELECT *, count(*) FROM single GROUP BY 1 ORDER BY 1",
    )
    .await;
    assert_eq!(int64_pairs(&batches), vec![(1, 2), (2, 1)]);

    let error = plan_sql(
        &grouped_catalog(),
        "SELECT *, count(*) FROM grouped GROUP BY 1",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("must appear in GROUP BY"), "{error}");
}

#[test]
fn group_by_reports_invalid_references_and_uses_the_last_alias() {
    let catalog = grouped_catalog();
    for sql in [
        "SELECT a, count(*) FROM grouped GROUP BY 0",
        "SELECT a, count(*) FROM grouped GROUP BY 3",
    ] {
        let error = plan_sql(&catalog, sql).unwrap_err().to_string();
        assert!(error.contains("GROUP BY position"), "{error}");
    }

    let last_alias = plan_sql(
        &catalog,
        "SELECT a AS z, b AS z, count(*) FROM grouped GROUP BY z",
    )
    .unwrap_err()
    .to_string();
    assert!(
        last_alias.contains("must appear in GROUP BY"),
        "{last_alias}"
    );
}

#[test]
fn name_resolution_errors_include_source_position() {
    let error = plan_sql(
        &grouped_catalog(),
        "SELECT a, count(*)\nFROM grouped\nGROUP BY 0",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("at line 3, column 10"), "{error}");

    let error = plan_sql(&grouped_catalog(), "SELECT a, b\nFROM grouped\nORDER BY 3")
        .unwrap_err()
        .to_string();
    assert!(error.contains("at line 3, column 10"), "{error}");
}

#[test]
fn group_by_input_column_takes_precedence_over_alias() {
    let error = plan_sql(
        &grouped_catalog(),
        "SELECT a AS x, count(*) FROM grouped GROUP BY x",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("must appear in GROUP BY"), "{error}");
}

#[tokio::test]
async fn resolves_having_aliases() {
    let batches = run(
        &grouped_catalog(),
        "SELECT a, count(*) AS n FROM grouped GROUP BY a HAVING n > 1 ORDER BY a",
    )
    .await;
    assert_eq!(int64_pairs(&batches), vec![(1, 2)]);
}

#[tokio::test]
async fn having_uses_the_last_duplicate_alias() {
    let batches = run(
        &grouped_catalog(),
        "SELECT count(*) AS z, sum(b) AS z FROM grouped \
         GROUP BY a HAVING z > 5 ORDER BY 2",
    )
    .await;
    assert_eq!(
        int64_column(&batches, 0)
            .into_iter()
            .zip(decimal_column(&batches, 1))
            .collect::<Vec<_>>(),
        vec![(1, 10), (2, 30)]
    );
}

#[tokio::test]
async fn orders_by_hidden_input_and_alias_expressions() {
    let catalog = ordering_catalog();
    let hidden = run(&catalog, "SELECT a FROM ordering ORDER BY b").await;
    assert_eq!(int64_column(&hidden, 0), vec![2, 1, 3]);

    let alias = run(
        &catalog,
        "SELECT a AS output_value FROM ordering ORDER BY output_value + 1 DESC",
    )
    .await;
    assert_eq!(int64_column(&alias, 0), vec![3, 2, 1]);
}

#[tokio::test]
async fn orders_aggregates_by_hidden_aggregate_expression() {
    let batches = run(
        &grouped_catalog(),
        "SELECT a FROM grouped GROUP BY a ORDER BY sum(b) DESC",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1, 2]);
}

#[tokio::test]
async fn distinct_accepts_output_expressions_but_rejects_hidden_columns() {
    let catalog = grouped_catalog();
    let batches = run(
        &catalog,
        "SELECT DISTINCT a + b AS total FROM grouped ORDER BY a + b",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![11, 12, 21]);

    let error = plan_sql(&catalog, "SELECT DISTINCT a FROM grouped ORDER BY b")
        .unwrap_err()
        .to_string();
    assert!(error.contains("SELECT DISTINCT output"), "{error}");
    assert!(error.contains("at line 1, column 41"), "{error}");

    let duplicate = run(
        &catalog,
        "SELECT DISTINCT a AS z, b AS z FROM grouped ORDER BY z, a",
    )
    .await;
    assert_eq!(int64_pairs(&duplicate), vec![(1, 10), (2, 10), (1, 20)]);
}

#[test]
fn order_by_reports_invalid_ordinals() {
    let catalog = grouped_catalog();
    for sql in [
        "SELECT a FROM grouped ORDER BY 0",
        "SELECT a FROM grouped ORDER BY -1",
        "SELECT a FROM grouped ORDER BY 2",
    ] {
        let error = plan_sql(&catalog, sql).unwrap_err().to_string();
        assert!(error.contains("ORDER BY position"), "{error}");
        assert!(error.contains("at line 1, column"), "{error}");
    }
}

#[tokio::test]
async fn order_by_uses_the_last_duplicate_alias() {
    let batches = run(
        &grouped_catalog(),
        "SELECT a AS z, b AS z FROM grouped ORDER BY z, a",
    )
    .await;
    assert_eq!(int64_pairs(&batches), vec![(1, 10), (2, 10), (1, 20)]);
}

#[tokio::test]
async fn order_by_defaults_to_nulls_last_for_asc_and_desc() {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
    )
    .unwrap();
    let catalog = Catalog::default();
    catalog
        .register(TableEntry::new("nullable", Arc::new(MemoryTable { batch })))
        .unwrap();

    let asc = run(&catalog, "SELECT a FROM nullable ORDER BY a ASC").await;
    assert_eq!(optional_int64_column(&asc, 0), vec![Some(1), Some(2), None]);
    let desc = run(&catalog, "SELECT a FROM nullable ORDER BY a DESC").await;
    assert_eq!(
        optional_int64_column(&desc, 0),
        vec![Some(2), Some(1), None]
    );
    let explicit = run(
        &catalog,
        "SELECT a FROM nullable ORDER BY a DESC NULLS FIRST",
    )
    .await;
    assert_eq!(
        optional_int64_column(&explicit, 0),
        vec![None, Some(2), Some(1)]
    );
}

fn grouped_catalog() -> Catalog {
    table_catalog(
        "grouped",
        vec![
            ("a", vec![1, 1, 2]),
            ("b", vec![10, 20, 10]),
            ("x", vec![100, 200, 100]),
        ],
    )
}

fn ordering_catalog() -> Catalog {
    table_catalog(
        "ordering",
        vec![("a", vec![1, 2, 3]), ("b", vec![20, 10, 30])],
    )
}

fn table_catalog(name: &str, columns: Vec<(&str, Vec<i64>)>) -> Catalog {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .into_iter()
        .map(|(_, values)| Arc::new(Int64Array::from(values)) as _)
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    let catalog = Catalog::default();
    catalog
        .register(TableEntry::new(name, Arc::new(MemoryTable { batch })))
        .unwrap();
    catalog
}

async fn run(catalog: &Catalog, sql: &str) -> Vec<RecordBatch> {
    let plan = plan_sql(catalog, sql).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn int64_column(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn optional_int64_column(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn decimal_column(batches: &[RecordBatch], column: usize) -> Vec<i128> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn int64_pairs(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    int64_column(batches, 0)
        .into_iter()
        .zip(int64_column(batches, 1))
        .collect()
}

#[derive(Clone)]
struct MemoryTable {
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics {
            row_count: Some(self.batch.num_rows() as u64),
            total_byte_size: Some(self.batch.get_array_memory_size() as u64),
            file_count: 1,
        }
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        context.check_cancelled()?;
        let mut batch = match request.projection {
            Some(projection) => self.batch.project(&projection)?,
            None => self.batch.clone(),
        };
        if let Some(limit) = request.limit {
            batch = batch.slice(0, limit.min(batch.num_rows()));
        }
        Ok(boxed_record_batch_stream(stream::once(
            async move { Ok(batch) },
        )))
    }
}
