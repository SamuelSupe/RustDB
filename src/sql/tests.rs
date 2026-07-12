use std::sync::Arc;

use arrow::{
    array::{
        Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::datasource::{ScanRequest, TableProvider, TableStatistics};
use crate::execution::execute;
use crate::runtime::{MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::{Catalog, Result, TableEntry};

use super::{StatementPlan, plan_sql};

#[test]
fn plans_distinct_as_grouping() {
    let StatementPlan::Query(plan) = plan_sql(&Catalog::default(), "SELECT DISTINCT 1").unwrap()
    else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.starts_with("Aggregate groups=[\"1\"]"));
    assert!(explain.contains("distinct=group_key_dedup"));
    assert!(explain.contains("Projection [\"1\"]"));
}

#[test]
fn explain_reports_decorrelation_and_spill_strategies() {
    let StatementPlan::Query(plan) = plan_sql(
        &Catalog::default(),
        "SELECT count(DISTINCT 1) ORDER BY count(DISTINCT 1)",
    )
    .unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("dedup:tagged_full_key"), "{explain}");
    assert!(explain.contains("Aggregate") && explain.contains("spill=recursive_hash"));
    assert!(explain.contains("Sort") && explain.contains("spill=external_ipc_lz4"));

    let catalog = Catalog::default();
    register_ids(&catalog, "left_ids", vec![Some(1)]);
    register_ids(&catalog, "right_ids", vec![Some(1)]);
    let StatementPlan::Query(plan) = plan_sql(
        &catalog,
        "SELECT l.id FROM left_ids l WHERE EXISTS (\
             SELECT 1 FROM right_ids r WHERE r.id = l.id\
         )",
    )
    .unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("SemiJoin"), "{explain}");
    assert!(explain.contains("decorrelation=complete"), "{explain}");
    assert!(explain.contains("spill=grace_hash"), "{explain}");
}

#[test]
fn binds_supported_distinct_aggregates_and_rejects_unsupported_shapes() {
    let StatementPlan::Query(plan) = plan_sql(
        &Catalog::default(),
        "SELECT count(DISTINCT 1), sum(DISTINCT 2), avg(DISTINCT 3), \
         min(DISTINCT 4), max(DISTINCT 5)",
    )
    .unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("distinct=count:3,dedup:tagged_full_key"));

    for sql in [
        "SELECT count(DISTINCT *)",
        "SELECT count(DISTINCT 1, 2)",
        "SELECT sum(DISTINCT *)",
        "SELECT sum(1 ORDER BY 1)",
        "SELECT count(1) FILTER (WHERE TRUE)",
    ] {
        assert!(plan_sql(&Catalog::default(), sql).is_err(), "{sql}");
    }
}

#[test]
fn binds_having_to_aggregate_output_and_deduplicates_aggregates() {
    let StatementPlan::Query(plan) = plan_sql(
        &Catalog::default(),
        "SELECT count(*) AS n HAVING count(*) > 0",
    )
    .unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("Filter count(*) > 0"));
    assert!(explain.contains("Aggregate groups=[] aggregates=[\"count(*)\"]"));
}

#[test]
fn keeps_windows_explicitly_unsupported() {
    let window = plan_sql(&Catalog::default(), "SELECT count(*) OVER ()")
        .unwrap_err()
        .to_string();
    assert!(window.contains("window") || window.contains("OVER"));
}

#[tokio::test]
async fn executes_scalar_subqueries_with_sql_cardinality_rules() {
    assert_eq!(int64_value(&run("SELECT (SELECT 7)").await), 7);

    let empty = run("SELECT (SELECT 7 WHERE FALSE)").await;
    let value = empty[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert!(value.is_null(0));

    let catalog = Catalog::default();
    register_ids(&catalog, "ids", vec![Some(1), Some(2)]);
    let plan = plan_sql(&catalog, "SELECT (SELECT id FROM ids)").unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let error = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("scalar subquery returned more than one row"));
}

#[tokio::test]
async fn executes_year_month_and_day_date_intervals() {
    let batches = run("SELECT \
         DATE '1995-03-31' - INTERVAL '1' MONTH = DATE '1995-02-28', \
         DATE '1995-03-31' - INTERVAL '1' YEAR = DATE '1994-03-31', \
         DATE '1995-03-31' - INTERVAL '1' DAY = DATE '1995-03-30'")
    .await;
    for column in batches[0].columns() {
        let value = column.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(value.value(0));
    }
}

#[tokio::test]
async fn timestamp_typed_literals_honor_microsecond_precision() {
    let batches = run("SELECT TIMESTAMP(0) '2024-02-29 12:34:56.999999', \
                TIMESTAMP(3) '2024-02-29 12:34:56.123999', \
                TIMESTAMP(6) '1969-12-31 23:59:59.123456', \
                TIMESTAMP(3) '1969-12-31 23:59:59.123456', \
                TIMESTAMP(3) '1969-12-31 23:59:59.8765'")
    .await;
    let batch = &batches[0];
    let values = (0..5)
        .map(|column| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();
    assert_eq!(values[0] % 1_000_000, 0);
    assert_eq!(values[1] % 1_000_000, 124_000);
    assert_eq!(values[2], -876_544);
    assert_eq!(values[3], -877_000);
    assert_eq!(values[4], -124_000);

    let error = plan_sql(
        &Catalog::default(),
        "SELECT TIMESTAMP(7) '2024-02-29 12:34:56.1234567'",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("precision above 6"), "{error}");

    let error = plan_sql(
        &Catalog::default(),
        "SELECT CAST('2024-02-29 12:34:56.123456' AS TIMESTAMP(3))",
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("precision-qualified TIMESTAMP casts"),
        "{error}"
    );
}

#[tokio::test]
async fn explain_analyze_executes_and_reports_global_metrics() {
    let batches = run("EXPLAIN ANALYZE SELECT 1").await;
    let output = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert!(output.contains("Projection"));
    assert!(output.contains("Global Metrics"));
    assert!(output.contains("returned_rows=1"));
}

#[tokio::test]
async fn folds_constants_and_short_circuits_inactive_rows() {
    let StatementPlan::Query(plan) =
        plan_sql(&Catalog::default(), "SELECT 1 + 2 * 3 WHERE TRUE AND 2 > 1").unwrap()
    else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("Projection [\"7\"]"));
    assert!(explain.contains("Filter true"));
    assert_eq!(int64_value(&run("SELECT 1 + 2 * 3").await), 7);

    for sql in [
        "SELECT 1 / 0",
        "SELECT CAST(NULL AS BOOLEAN) AND (9223372036854775807 + 1 > 0)",
        "SELECT CAST(NULL AS BOOLEAN) OR (9223372036854775807 + 1 > 0)",
        "SELECT CAST(1000 AS DECIMAL(3, 2))",
    ] {
        let plan = plan_sql(&Catalog::default(), sql).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
        let error = execute(plan, context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("division by zero")
                || error.contains("overflow")
                || error.contains("strict CAST"),
            "unexpected error for {sql}: {error}"
        );
    }

    let short = run("SELECT CASE WHEN FALSE THEN 1 / 0 ELSE 0 END, \
                CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 / 0 ELSE 1 END, \
                FALSE AND (1 / 0 = 0), TRUE OR (1 / 0 = 0), \
                coalesce(7, 1 / 0), nullif(CAST(NULL AS BIGINT), 1 / 0)")
    .await;
    let batch = &short[0];
    for (column, expected) in [(0, 0), (1, 1), (4, 7)] {
        assert_eq!(
            batch
                .column(column)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(0),
            f64::from(expected)
        );
    }
    assert!(
        !batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert!(batch.column(5).is_null(0));
}

#[tokio::test]
async fn short_circuit_masks_are_applied_per_row() {
    let catalog = Catalog::default();
    register_ids(&catalog, "ids", vec![Some(0), Some(2)]);
    let batches = run_with_catalog(
        &catalog,
        "SELECT CASE WHEN id = 0 THEN 0 ELSE 10 / id END, \
                id <> 0 AND 10 / id > 0, \
                id = 0 OR 10 / id > 0, \
                coalesce(id, 9223372036854775807 + 1) \
         FROM ids ORDER BY id",
    )
    .await;
    let batch = &batches[0];
    let case = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((case.value(0), case.value(1)), (0.0, 5.0));
    let and = batch
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let or = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert_eq!((and.value(0), and.value(1)), (false, true));
    assert_eq!((or.value(0), or.value(1)), (true, true));
    let coalesce = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((coalesce.value(0), coalesce.value(1)), (0, 2));
}

#[tokio::test]
async fn eager_boolean_fast_path_preserves_kleene_null_semantics() {
    let batches = run("SELECT FALSE AND CAST(NULL AS BOOLEAN), \
                TRUE AND CAST(NULL AS BOOLEAN), \
                CAST(NULL AS BOOLEAN) AND FALSE, \
                CAST(NULL AS BOOLEAN) AND TRUE, \
                TRUE OR CAST(NULL AS BOOLEAN), \
                FALSE OR CAST(NULL AS BOOLEAN), \
                CAST(NULL AS BOOLEAN) OR TRUE, \
                CAST(NULL AS BOOLEAN) OR FALSE")
    .await;
    let batch = &batches[0];
    for (column, expected) in [
        (0, Some(false)),
        (1, None),
        (2, Some(false)),
        (3, None),
        (4, Some(true)),
        (5, None),
        (6, Some(true)),
        (7, None),
    ] {
        let actual = batch
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .next()
            .unwrap();
        assert_eq!(actual, expected, "column {column}");
    }
}

#[tokio::test]
async fn nullif_preserves_its_left_type_and_decimal_branches_widen_losslessly() {
    let batches = run("SELECT nullif(1, 2.5), \
                coalesce(CAST(NULL AS DECIMAL(3, 2)), 1000), \
                CASE WHEN false THEN CAST(0 AS DECIMAL(3, 2)) ELSE 1000 END")
    .await;
    let batch = &batches[0];
    let nullif = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(nullif.value(0), 1);
    for column in 1..=2 {
        let value = batch
            .column(column)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(value.value(0), 100_000);
        assert_eq!(value.data_type(), &DataType::Decimal128(21, 2));
    }
}

#[tokio::test]
async fn decimal_rounding_adjusts_output_scale_without_invalid_payloads() {
    let batches = run("SELECT ceil(CAST(99.99 AS DECIMAL(4, 2))), \
                floor(CAST(-99.99 AS DECIMAL(4, 2))), \
                round(CAST(99.99 AS DECIMAL(4, 2)), 0), \
                round(CAST(12.345 AS DECIMAL(8, 3)), 2), \
                round(CAST(12.345 AS DECIMAL(8, 3)), CAST(2 AS INTEGER)), \
                round(CAST(12.345 AS DECIMAL(8, 3)), 1 + 1)")
    .await;
    let batch = &batches[0];
    for (column, value) in [(0, 100), (1, -100), (2, 100)] {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), value);
        assert_eq!(array.data_type(), &DataType::Decimal128(4, 0));
    }
    for column in 3..=5 {
        let rounded = batch
            .column(column)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(rounded.value(0), 1_235);
        assert_eq!(rounded.data_type(), &DataType::Decimal128(8, 2));
    }
}

#[tokio::test]
async fn integer_ceil_and_floor_follow_duckdb_double_semantics() {
    let batches = run("SELECT ceil(CAST(9007199254740993 AS BIGINT)), \
                floor(CAST(9007199254740993 AS BIGINT)), \
                round(CAST(9007199254740993 AS BIGINT))")
    .await;
    let batch = &batches[0];
    for column in 0..=1 {
        let value = batch
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(value.value(0), 9_007_199_254_740_992.0);
    }
    let rounded = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(rounded.value(0), 9_007_199_254_740_993);
}

#[tokio::test]
async fn executes_chained_ctes_and_derived_tables() {
    let cte = run(
        "WITH base(v) AS (SELECT 2), next AS (SELECT v + 3 AS value FROM base) \
         SELECT n.value FROM next AS n",
    )
    .await;
    assert_eq!(int64_value(&cte), 5);

    let derived = run("SELECT d.value FROM (SELECT 7 AS value) AS d").await;
    assert_eq!(int64_value(&derived), 7);
}

#[tokio::test]
async fn rewrites_in_and_exists_to_semi_and_anti_joins() {
    let included = run("SELECT 7 WHERE 7 IN (SELECT 7)").await;
    assert_eq!(int64_value(&included), 7);
    assert!(run("SELECT 7 WHERE 8 IN (SELECT 7)").await.is_empty());

    assert_eq!(
        int64_value(&run("SELECT 9 WHERE EXISTS (SELECT 1)").await),
        9
    );
    assert_eq!(
        int64_value(&run("SELECT 9 WHERE NOT EXISTS (SELECT 1 WHERE FALSE)").await),
        9
    );
    assert!(
        run("SELECT 9 WHERE EXISTS (SELECT 1 WHERE FALSE)")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn semi_join_preserves_left_multiplicity_only() {
    let catalog = Catalog::default();
    register_ids(
        &catalog,
        "left_ids",
        vec![Some(1), Some(2), Some(2), Some(3), None],
    );
    register_ids(&catalog, "right_ids", vec![Some(2), Some(2), Some(4), None]);
    let batches = run_with_catalog(
        &catalog,
        "SELECT id FROM left_ids WHERE id IN (SELECT id FROM right_ids)",
    )
    .await;
    let values = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![2, 2]);
}

#[tokio::test]
async fn correlated_scalar_subquery_enforces_per_outer_row_cardinality() {
    let catalog = Catalog::default();
    register_ids(&catalog, "left_ids", vec![Some(2)]);
    register_ids(&catalog, "right_ids", vec![Some(2), Some(2)]);
    let plan = plan_sql(
        &catalog,
        "SELECT l.id, (SELECT r.id FROM right_ids AS r WHERE r.id = l.id) \
         FROM left_ids AS l",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let error = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("scalar subquery returned more than one row"));
}

#[test]
fn rejects_correlation_without_an_inner_equality_key() {
    let error = plan_sql(
        &Catalog::default(),
        "SELECT d.value FROM (SELECT 1 AS value) d \
         WHERE EXISTS (SELECT 1 WHERE d.value = 1)",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("at least one outer-to-inner equality key"));
}

async fn run(sql: &str) -> Vec<RecordBatch> {
    let catalog = Catalog::default();
    run_with_catalog(&catalog, sql).await
}

async fn run_with_catalog(catalog: &Catalog, sql: &str) -> Vec<RecordBatch> {
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

fn register_ids(catalog: &Catalog, name: &str, values: Vec<Option<i64>>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap();
    catalog
        .register(TableEntry::new(name, Arc::new(MemoryTable { batch })))
        .unwrap();
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

fn int64_value(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
