use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray},
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
async fn aggregate_filter_reuses_null_aware_aggregate_execution() {
    let batches = run(
        &grouped_catalog(),
        "SELECT a, \
         count(*) FILTER (WHERE b > 10), \
         sum(b) FILTER (WHERE b > 10), \
         count(DISTINCT x) FILTER (WHERE b >= 10) \
         FROM grouped GROUP BY a ORDER BY a",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![1, 2]);
    assert_eq!(int64_column(&batches, 1), vec![1, 0]);
    assert_eq!(optional_decimal_column(&batches, 2), vec![Some(20), None]);
    assert_eq!(int64_column(&batches, 3), vec![2, 1]);

    let error = plan_sql(
        &grouped_catalog(),
        "SELECT count(*) FILTER (WHERE b) FROM grouped",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("FILTER requires BOOLEAN"), "{error}");
}

#[tokio::test]
async fn order_insensitive_aggregates_accept_and_validate_ordering_clauses() {
    let batches = run(
        &grouped_catalog(),
        "SELECT sum(a ORDER BY b DESC), min(b) WITHIN GROUP (ORDER BY a) FROM grouped",
    )
    .await;
    assert_eq!(decimal_column(&batches, 0), vec![4]);
    assert_eq!(int64_column(&batches, 1), vec![10]);

    let error = plan_sql(
        &grouped_catalog(),
        "SELECT sum(a ORDER BY missing) FROM grouped",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("missing"), "{error}");
}

#[tokio::test]
async fn count_distinct_accepts_multiple_expressions() {
    let catalog = nullable_tuple_catalog();
    let batches = run(
        &catalog,
        "SELECT count(DISTINCT a, b), \
         count(DISTINCT b, a), \
         count(DISTINCT a, b) FILTER (WHERE b >= 10) \
         FROM tuples",
    )
    .await;
    assert_eq!(int64_column(&batches, 0), vec![3]);
    assert_eq!(int64_column(&batches, 1), vec![3]);
    assert_eq!(int64_column(&batches, 2), vec![3]);

    let error = plan_sql(&catalog, "SELECT sum(DISTINCT a, b) FROM tuples")
        .unwrap_err()
        .to_string();
    assert!(error.contains("only for COUNT"), "{error}");
}

#[tokio::test]
async fn grouping_sets_rollup_and_cube_expand_to_aggregate_branches() {
    let catalog = grouped_catalog();
    for grouping in ["GROUPING SETS ((a, b), (a), ())", "ROLLUP (a, b)"] {
        let sql = format!(
            "SELECT a, b, count(*) FROM grouped GROUP BY {grouping} \
             ORDER BY a NULLS LAST, b NULLS LAST"
        );
        let batches = run(&catalog, &sql).await;
        assert_eq!(
            optional_int64_column(&batches, 0),
            vec![Some(1), Some(1), Some(1), Some(2), Some(2), None],
            "{grouping}"
        );
        assert_eq!(
            optional_int64_column(&batches, 1),
            vec![Some(10), Some(20), None, Some(10), None, None],
            "{grouping}"
        );
        assert_eq!(int64_column(&batches, 2), vec![1, 1, 2, 1, 1, 3]);
    }

    let cube = run(
        &catalog,
        "SELECT a, b, count(*) FROM grouped GROUP BY CUBE (a, b) \
         ORDER BY a NULLS LAST, b NULLS LAST",
    )
    .await;
    assert_eq!(cube.iter().map(RecordBatch::num_rows).sum::<usize>(), 8);
    assert_eq!(
        optional_int64_column(&cube, 0),
        vec![
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            None,
            None,
            None
        ]
    );
    assert_eq!(
        optional_int64_column(&cube, 1),
        vec![
            Some(10),
            Some(20),
            None,
            Some(10),
            None,
            Some(10),
            Some(20),
            None
        ]
    );
    assert_eq!(int64_column(&cube, 2), vec![1, 1, 2, 1, 1, 2, 1, 3]);

    let masks = run(
        &catalog,
        "SELECT a, b, grouping(a, b) AS gid, count(*) FROM grouped \
         GROUP BY GROUPING SETS ((a, b), (a), ()) \
         ORDER BY gid, a NULLS LAST, b NULLS LAST",
    )
    .await;
    assert_eq!(int64_column(&masks, 2), vec![0, 0, 0, 1, 1, 3]);
    assert_eq!(int64_column(&masks, 3), vec![1, 1, 1, 2, 1, 3]);
}

#[tokio::test]
async fn grouping_sets_normalize_equivalent_ordinals_and_aliases() {
    let catalog = grouped_catalog();
    for grouping in ["GROUPING SETS ((1), (a))", "GROUPING SETS ((1), (k))"] {
        let batches = run(
            &catalog,
            &format!("SELECT a AS k, count(*) FROM grouped GROUP BY {grouping} ORDER BY k"),
        )
        .await;
        assert_eq!(int64_column(&batches, 0), vec![1, 1, 2, 2], "{grouping}");
        assert_eq!(int64_column(&batches, 1), vec![2, 2, 1, 1], "{grouping}");
    }
}

#[tokio::test]
async fn interval_comparison_keys_use_one_sql_equality_domain() {
    let scalar = run(
        &Catalog::default(),
        "SELECT \
         INTERVAL '1' DAY = INTERVAL '24 hours', \
         INTERVAL '1' MONTH = INTERVAL '30' DAY, \
         INTERVAL '1' DAY < INTERVAL '25 hours'",
    )
    .await;
    for column in scalar[0].columns() {
        assert!(
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );
    }

    let grouped = run(
        &Catalog::default(),
        "SELECT count(*), count(DISTINCT i) \
         FROM (VALUES (INTERVAL '1' DAY), (INTERVAL '24 hours')) AS v(i) \
         GROUP BY i",
    )
    .await;
    assert_eq!(int64_column(&grouped, 0), vec![2]);
    assert_eq!(int64_column(&grouped, 1), vec![1]);

    let joined = run(
        &Catalog::default(),
        "SELECT count(*) \
         FROM (VALUES (INTERVAL '1' DAY)) AS l(i) \
         JOIN (VALUES (INTERVAL '24 hours')) AS r(i) ON l.i = r.i",
    )
    .await;
    assert_eq!(int64_column(&joined, 0), vec![1]);
}

#[tokio::test]
async fn time_timestamp_precision_and_uuid_literals_cast_strictly() {
    let batches = run(
        &Catalog::default(),
        "SELECT \
         CAST(TIME '12:34:56.123456' AS VARCHAR), \
         CAST(CAST('01:02:03.125' AS TIME(3)) AS VARCHAR), \
         CAST(TIMESTAMP(3) '2024-02-29 12:34:56.1234' AS VARCHAR), \
         CAST(TIMESTAMP(9) '2024-02-29 12:34:56.123456789' AS VARCHAR), \
         CAST(UUID '550e8400-e29b-41d4-a716-446655440000' AS VARCHAR), \
         CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) = \
           UUID '550e8400-e29b-41d4-a716-446655440000'",
    )
    .await;
    assert_eq!(string_column(&batches, 0), vec!["12:34:56.123456"]);
    assert_eq!(string_column(&batches, 1), vec!["01:02:03.125"]);
    assert_eq!(string_column(&batches, 2), vec!["2024-02-29 12:34:56.123"]);
    assert_eq!(
        string_column(&batches, 3),
        vec!["2024-02-29 12:34:56.123456789"]
    );
    assert_eq!(
        string_column(&batches, 4),
        vec!["550e8400-e29b-41d4-a716-446655440000"]
    );
    let equal = batches[0]
        .column(5)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(equal.value(0));

    let error = plan_sql(&Catalog::default(), "SELECT UUID 'not-a-uuid'")
        .unwrap_err()
        .to_string();
    assert!(error.contains("UUID literal"), "{error}");
}

#[tokio::test]
async fn compound_intervals_cover_standard_ranges_and_mixed_units() {
    let batches = run(
        &Catalog::default(),
        "SELECT \
         TIMESTAMP '2024-01-01 00:00:00' \
           + INTERVAL '1 02:03:04.5' DAY TO SECOND \
           = TIMESTAMP '2024-01-02 02:03:04.5', \
         DATE '2024-01-31' + INTERVAL '1-1' YEAR TO MONTH \
           = DATE '2025-02-28', \
         TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 day 2 hours' \
           = TIMESTAMP '2024-01-02 02:00:00', \
         TIMESTAMP '2024-01-01 00:00:00' \
           + INTERVAL '2:03.5' MINUTE TO SECOND \
           = TIMESTAMP '2024-01-01 00:02:03.5', \
         CAST('2:03.5' AS INTERVAL) = INTERVAL '2:03.5' MINUTE TO SECOND",
    )
    .await;
    for column in batches[0].columns() {
        let values = column.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(values.value(0));
    }

    for sql in [
        "SELECT INTERVAL '1 24:00' DAY TO MINUTE",
        "SELECT INTERVAL '1-12' YEAR TO MONTH",
        "SELECT INTERVAL '1:60' MINUTE TO SECOND",
    ] {
        assert!(plan_sql(&Catalog::default(), sql).is_err(), "{sql}");
    }
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

fn nullable_tuple_catalog() -> Catalog {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                Some(1),
                Some(1),
                Some(1),
                Some(2),
                None,
                Some(2),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                Some(20),
                Some(10),
                Some(10),
                None,
            ])),
        ],
    )
    .unwrap();
    let catalog = Catalog::default();
    catalog
        .register(TableEntry::new("tuples", Arc::new(MemoryTable { batch })))
        .unwrap();
    catalog
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

fn optional_decimal_column(batches: &[RecordBatch], column: usize) -> Vec<Option<i128>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn string_column(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_owned())
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
