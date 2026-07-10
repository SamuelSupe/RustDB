use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, Int64Array, StringArray},
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
    assert!(explain.contains("Projection [\"1\"]"));
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
async fn folds_constants_without_hiding_execution_errors() {
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
        "SELECT FALSE AND (9223372036854775807 + 1 > 0)",
        "SELECT TRUE OR (9223372036854775807 + 1 > 0)",
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
            error.contains("division by zero") || error.contains("overflow"),
            "unexpected error for {sql}: {error}"
        );
    }
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

#[test]
fn rejects_correlated_subqueries_explicitly() {
    let error = plan_sql(
        &Catalog::default(),
        "SELECT d.value FROM (SELECT 1 AS value) d \
         WHERE EXISTS (SELECT 1 WHERE d.value = 1)",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("correlated subqueries are not supported"));
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
