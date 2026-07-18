use std::sync::Arc;

use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use crate::{
    Catalog,
    execution::execute,
    runtime::{MemoryPool, QueryContext},
    sql::StatementPlan,
};

use super::super::plan_sql;
use super::validate_distinct_schema;

#[test]
fn plans_set_operations_with_expected_physical_shapes() {
    let catalog = Catalog::default();
    let StatementPlan::Query(union_all) =
        plan_sql(&catalog, "SELECT 1 AS n UNION ALL SELECT 2").unwrap()
    else {
        panic!("expected query plan");
    };
    assert!(union_all.explain().starts_with("Append"));

    let StatementPlan::Query(union) = plan_sql(&catalog, "SELECT 1 AS n UNION SELECT 1").unwrap()
    else {
        panic!("expected query plan");
    };
    let explain = union.explain();
    assert!(explain.starts_with("Aggregate"), "{explain}");
    assert!(explain.contains("Append"), "{explain}");

    for sql in [
        "SELECT 1 AS n INTERSECT ALL SELECT 1",
        "SELECT 1 AS n EXCEPT ALL SELECT 1",
    ] {
        let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
            panic!("expected query plan");
        };
        assert!(
            plan.explain().contains("Repeat"),
            "{sql}: {}",
            plan.explain()
        );
    }

    for (sql, join) in [
        ("SELECT 1 AS n INTERSECT SELECT 1", "SemiJoin"),
        ("SELECT 1 AS n EXCEPT SELECT 1", "AntiJoin"),
    ] {
        let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
            panic!("expected query plan");
        };
        let explain = plan.explain();
        assert!(explain.contains(join), "{sql}: {explain}");
        assert!(explain.contains("null_equal_keys=true"), "{sql}: {explain}");
    }
}

#[test]
fn validates_set_shapes_and_output_only_ordering() {
    let catalog = Catalog::default();
    for sql in [
        "SELECT 1 UNION BY NAME SELECT 1",
        "SELECT 1 UNION ALL SELECT 1, 2",
        "SELECT 1 UNION ALL SELECT CAST(1 AS DOUBLE)",
        "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n + 1",
        "SELECT 1 AS n UNION ALL SELECT 2 AS other ORDER BY other",
    ] {
        assert!(plan_sql(&catalog, sql).is_err(), "{sql}");
    }
}

#[test]
fn uses_left_names_and_lossless_decimal_alignment() {
    let StatementPlan::Query(plan) = plan_sql(
        &Catalog::default(),
        "SELECT CAST(1 AS DECIMAL(10, 2)) AS left_name \
         UNION ALL SELECT CAST(2 AS DECIMAL(12, 4)) AS right_name",
    )
    .unwrap() else {
        panic!("expected query plan");
    };
    let field = plan.schema().arrow().field(0);
    assert_eq!(field.name(), "left_name");
    assert_eq!(field.data_type(), &DataType::Decimal128(12, 4));
    assert!(plan.schema().qualifier(0).is_none());
}

#[test]
fn rejects_nested_distinct_keys() {
    let nested = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let schema = crate::sql::PlanSchema::unqualified(Arc::new(Schema::new(vec![Field::new(
        "nested", nested, true,
    )])));
    let error = validate_distinct_schema(&schema).unwrap_err().to_string();
    assert!(error.contains("nested column"), "{error}");
}

#[tokio::test]
async fn executes_distinct_null_semantics_and_query_level_limit() {
    assert_eq!(rows("SELECT NULL AS n UNION SELECT NULL").await, 1);
    assert_eq!(rows("SELECT NULL AS n INTERSECT SELECT NULL").await, 1);
    assert_eq!(rows("SELECT NULL AS n EXCEPT SELECT NULL").await, 0);
    assert_eq!(
        rows("(SELECT 1 AS n UNION ALL SELECT 1) INTERSECT ALL (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1)").await,
        2
    );
    assert_eq!(
        rows("(SELECT 1 AS n UNION ALL SELECT 1) EXCEPT ALL SELECT 1").await,
        1
    );
    assert_eq!(
        rows("(SELECT NULL AS n UNION ALL SELECT NULL) EXCEPT ALL SELECT NULL").await,
        1
    );
    assert_eq!(
        rows("(SELECT 1 UNION ALL SELECT 1) INTERSECT SELECT 1").await,
        1
    );
    assert_eq!(
        rows("WITH set_rows AS (SELECT 1 AS n UNION ALL SELECT 2) SELECT n FROM set_rows").await,
        2
    );
    assert_eq!(
        rows("SELECT TIME(3) '12:00:00.125' UNION SELECT TIME(9) '12:00:00.125'").await,
        1
    );
    assert_eq!(
        rows("SELECT UUID '550e8400-e29b-41d4-a716-446655440000' INTERSECT SELECT UUID '550e8400-e29b-41d4-a716-446655440000'").await,
        1
    );
    assert_eq!(
        rows("SELECT INTERVAL '1' DAY EXCEPT SELECT INTERVAL '2' DAY").await,
        1
    );
    assert_eq!(
        rows("SELECT INTERVAL '1' DAY UNION SELECT INTERVAL '1' MONTH").await,
        2
    );
    assert_eq!(
        rows("SELECT INTERVAL '1' DAY UNION SELECT INTERVAL '24 hours'").await,
        1
    );
    assert_eq!(
        rows(
            "SELECT TIMESTAMP '2024-01-01 00:00:00' AT TIME ZONE 'America/New_York' \
             UNION SELECT TIMESTAMP '2024-01-01 05:00:00' AT TIME ZONE 'UTC'"
        )
        .await,
        1
    );

    let batches =
        run("SELECT 3 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 ORDER BY n LIMIT 1 OFFSET 1")
            .await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.value(0), 2);
}

async fn rows(sql: &str) -> usize {
    run(sql).await.iter().map(RecordBatch::num_rows).sum()
}

async fn run(sql: &str) -> Vec<RecordBatch> {
    let plan = plan_sql(&Catalog::default(), sql).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
}
