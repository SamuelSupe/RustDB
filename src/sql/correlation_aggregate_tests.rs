use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::datasource::{ScanRequest, TableProvider, TableSourceIdentity, TableStatistics};
use crate::execution::execute;
use crate::runtime::{MemoryPool, QueryContext, RecordBatchStream, boxed_record_batch_stream};
use crate::{Catalog, Error, Result, TableEntry};

use super::{StatementPlan, plan_sql};

#[test]
fn aggregate_projection_and_having_require_grouped_outer_dependencies() {
    let catalog = Catalog::default();
    for sql in [
        "SELECT CASE WHEN EXISTS (\
             SELECT 1 FROM (SELECT 1 AS key) AS i WHERE i.key = p.key\
         ) THEN count(*) ELSE 0 END \
         FROM (SELECT 1 AS key) AS p",
        "SELECT count(*) FROM (SELECT 1 AS key) AS p \
         HAVING EXISTS (\
             SELECT 1 FROM (SELECT 1 AS key) AS i WHERE i.key = p.key\
         )",
    ] {
        let error = plan_sql(&catalog, sql).unwrap_err();
        assert!(
            matches!(error, Error::InvalidArgument(_))
                && error
                    .to_string()
                    .contains("must appear directly in GROUP BY"),
            "{error}"
        );
    }

    plan_sql(
        &catalog,
        "SELECT p.key, CASE WHEN EXISTS (\
             SELECT 1 FROM (SELECT 1 AS key) AS i WHERE i.key = p.key\
         ) THEN count(*) ELSE 0 END \
         FROM (SELECT 1 AS key) AS p GROUP BY p.key",
    )
    .unwrap();
}

#[tokio::test]
async fn uncorrelated_subqueries_run_after_an_empty_global_aggregate() {
    let batches = run(
        &Catalog::default(),
        "SELECT count(*) AS n, \
                (SELECT count(*) FROM (SELECT 1 AS value) AS s) AS scalar_value, \
                EXISTS (SELECT 1) AS exists_value, \
                1 IN (SELECT 1) AS in_value, \
                CASE WHEN EXISTS (SELECT 1) THEN count(*) ELSE 99 END AS case_value \
         FROM (SELECT 1 AS key) AS p WHERE p.key < 0 \
         HAVING (SELECT count(*) FROM (SELECT 1 AS value) AS h) = 1",
    )
    .await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(int64(&batches[0], 0).value(0), 0);
    assert_eq!(int64(&batches[0], 1).value(0), 1);
    assert_eq!(int64(&batches[0], 4).value(0), 0);
    for column in [2, 3] {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
}

#[tokio::test]
async fn subqueries_inside_aggregate_arguments_run_before_aggregation() {
    let batches = run(
        &Catalog::default(),
        "SELECT sum((SELECT 2)) AS scalar_sum, \
                count(CASE WHEN EXISTS (SELECT 1) THEN 1 END) AS exists_count \
         FROM (SELECT 1 AS key) AS rows",
    )
    .await;
    assert_eq!(decimal(&batches[0], 0).value(0), 2);
    assert_eq!(int64(&batches[0], 1).value(0), 1);
}

#[tokio::test]
async fn deferred_scalar_can_cross_an_independent_correlated_aggregate_argument() {
    let batches = run(
        &Catalog::default(),
        "SELECT (SELECT 9) AS scalar_value, \
                count(CASE WHEN EXISTS (\
                    SELECT 1 FROM (SELECT 1 AS key) AS i WHERE i.key = o.key\
                ) THEN 1 END) AS matched \
         FROM (SELECT 1 AS key) AS o GROUP BY o.key",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).value(0), 9);
    assert_eq!(int64(&batches[0], 1).value(0), 1);
}

#[tokio::test]
async fn aggregate_argument_attachment_dependencies_remain_before_aggregation() {
    let batches = run(
        &Catalog::default(),
        "SELECT count(CASE WHEN (SELECT 1) IN (\
                    SELECT i.key FROM (SELECT 1 AS key) AS i WHERE i.key = o.key\
                ) THEN 1 END) AS matched \
         FROM (SELECT 1 AS key) AS o GROUP BY o.key",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).value(0), 1);
}

#[test]
fn deferred_having_still_requires_boolean() {
    let error = plan_sql(
        &Catalog::default(),
        "SELECT count(*) FROM (SELECT 1 AS key) AS rows HAVING (SELECT 1)",
    )
    .unwrap_err();
    assert!(
        matches!(error, Error::InvalidArgument(_))
            && error.to_string().contains("HAVING requires BOOLEAN"),
        "{error}"
    );
}

#[tokio::test]
async fn aggregate_results_can_drive_uncorrelated_in_in_select_and_having() {
    let sql = "SELECT count(*) IN (SELECT 1) AS selected, \
                (sum(o.value) + o.grp) IN (SELECT 2) AS grouped_expression \
         FROM (SELECT 1 AS grp, 1 AS value) AS o GROUP BY o.grp \
         HAVING count(*) IN (SELECT 1)";
    let batches = run(&Catalog::default(), sql).await;
    for column in 0..2 {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
}

#[tokio::test]
async fn aggregate_result_not_in_preserves_rhs_null_semantics() {
    let batches = run(
        &Catalog::default(),
        "SELECT count(*) NOT IN (SELECT CAST(NULL AS BIGINT)) AS result \
         FROM (SELECT 1 AS value) AS rows",
    )
    .await;
    let result = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(result.is_null(0));
}

#[tokio::test]
async fn aggregate_subquery_arguments_execute_over_multiple_input_rows() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "many_rows",
        &["grp", "value"],
        vec![
            vec![1, 1],
            vec![1, 2],
            vec![1, 3],
            vec![2, 4],
            vec![2, 5],
            vec![2, 6],
        ],
    );
    let sql = "SELECT sum((SELECT 2)) AS scalar_sum, \
                      count(CASE WHEN EXISTS (SELECT 1) THEN 1 END) AS exists_count \
               FROM many_rows";
    let plan = plan_sql(&catalog, sql).unwrap();
    let explain = format!("{plan:?}");
    assert!(
        explain.contains("Scan table=many_rows projection=Some([0])"),
        "an attachment with no data dependency needs only one schema anchor:\n{explain}"
    );
    let batches = run(&catalog, sql).await;
    assert_eq!(decimal(&batches[0], 0).value(0), 12);
    assert_eq!(int64(&batches[0], 1).value(0), 6);
}

#[tokio::test]
async fn aggregate_results_can_drive_correlated_in_in_select_and_having() {
    let batches = run(
        &Catalog::default(),
        "SELECT o.grp, \
                count(*) IN (\
                    SELECT i.value FROM (SELECT 1 AS grp, 1 AS value) AS i \
                    WHERE i.grp = o.grp\
                ) AS selected \
         FROM (SELECT 1 AS grp, 7 AS value) AS o GROUP BY o.grp \
         HAVING count(*) IN (\
             SELECT i.value FROM (SELECT 1 AS grp, 1 AS value) AS i \
             WHERE i.grp = o.grp\
         )",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).value(0), 1);
    let selected = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(selected.value(0));
}

#[tokio::test]
async fn correlated_exists_in_having_does_not_evaluate_its_projection() {
    let batches = run(
        &Catalog::default(),
        "SELECT o.key, count(*) AS n \
         FROM (SELECT 1 AS key) AS o GROUP BY o.key \
         HAVING EXISTS (\
             SELECT 1 / 0 FROM (SELECT 1 AS key) AS i WHERE i.key = o.key\
         )",
    )
    .await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        (
            int64(&batches[0], 0).value(0),
            int64(&batches[0], 1).value(0)
        ),
        (1, 1)
    );
}

#[tokio::test]
async fn exists_prunes_unused_aggregate_projection_state() {
    let batches = run(
        &Catalog::default(),
        "SELECT EXISTS (\
             SELECT sum(1 / 0) FROM (SELECT 1 AS key) AS i\
         ) AS plain_exists, \
         EXISTS (\
             SELECT sum(1 / 0) FROM (SELECT 1 AS key) AS i \
             WHERE i.key = o.key\
         ) AS correlated_exists, \
         EXISTS (\
             SELECT sum(1 / 0) FROM (SELECT 1 AS key) AS i \
             HAVING count(*) > 0\
         ) AS having_exists, \
         CASE WHEN EXISTS (\
             SELECT sum(1 / 0) FROM (SELECT 1 AS key) AS i \
             WHERE i.key = o.key HAVING count(*) > 0\
         ) THEN 7 ELSE 0 END AS case_value \
         FROM (SELECT 1 AS key) AS o",
    )
    .await;
    for column in 0..3 {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
    assert_eq!(int64(&batches[0], 3).value(0), 7);
}

#[tokio::test]
async fn exists_preserves_distinct_cardinality_before_a_positive_offset() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "duplicate_values",
        &["key"],
        vec![vec![1], vec![1]],
    );
    let batches = run(
        &catalog,
        "SELECT EXISTS (\
             SELECT DISTINCT key FROM duplicate_values OFFSET 1\
         ) AS has_second_distinct",
    )
    .await;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!value.value(0));
}

#[tokio::test]
async fn exists_erases_a_non_distinct_projection_before_a_positive_offset() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "duplicate_values",
        &["key"],
        vec![vec![1], vec![1]],
    );
    let batches = run(
        &catalog,
        "SELECT EXISTS (\
             SELECT 1 / 0 FROM duplicate_values OFFSET 1\
         ) AS has_second_row",
    )
    .await;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(value.value(0));
}

#[tokio::test]
async fn correlated_dead_branches_still_enforce_scalar_cardinality() {
    let catalog = Catalog::default();
    register(&catalog, "outer_guard", &["key", "flag"], vec![vec![1, 0]]);
    register(
        &catalog,
        "inner_guard",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20]],
    );
    let batches = run(
        &catalog,
        "SELECT false AND ((SELECT value FROM inner_guard) = 10) AS and_value, \
                true OR ((SELECT value FROM inner_guard) = 10) AS or_value, \
         FROM outer_guard AS o",
    )
    .await;
    for (column, expected) in [(0, false), (1, true)] {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(value.value(0), expected);
    }

    for sql in [
        "SELECT CASE WHEN o.flag = 1 THEN (\
             SELECT i.value FROM inner_guard AS i WHERE i.key = o.key\
         ) ELSE 0 END FROM outer_guard AS o",
        "SELECT false AND ((\
             SELECT i.value FROM inner_guard AS i WHERE i.key = o.key\
         ) = 10) FROM outer_guard AS o",
        "SELECT CASE WHEN false THEN (\
             SELECT DISTINCT i.value FROM inner_guard AS i WHERE i.key = o.key\
         ) ELSE 0 END FROM outer_guard AS o",
        "SELECT CASE WHEN false THEN (\
             SELECT max(i.value) FROM inner_guard AS i WHERE i.key = o.key \
             GROUP BY i.value\
         ) ELSE 0 END FROM outer_guard AS o",
    ] {
        let plan = plan_sql(&catalog, sql).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
        let error = execute(plan, context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("scalar subquery returned more than one row"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn guarded_distinct_and_grouped_scalars_preserve_single_and_empty_results() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "outer_guard_shapes",
        &["key", "active"],
        vec![vec![2, 1], vec![3, 1]],
    );
    register(
        &catalog,
        "inner_guard_shapes",
        &["key", "value"],
        vec![vec![2, 30]],
    );
    let batches = run(
        &catalog,
        "SELECT o.key, \
                CASE WHEN o.active = 1 THEN (\
                    SELECT DISTINCT i.value FROM inner_guard_shapes AS i \
                    WHERE i.key = o.key\
                ) ELSE 0 END AS distinct_value, \
                CASE WHEN o.active = 1 THEN (\
                    SELECT max(i.value) FROM inner_guard_shapes AS i \
                    WHERE i.key = o.key GROUP BY i.value\
                ) ELSE 0 END AS grouped_value \
         FROM outer_guard_shapes AS o ORDER BY o.key",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).values(), &[2, 3]);
    for column in [1, 2] {
        let values = int64(&batches[0], column);
        assert_eq!(values.value(0), 30);
        assert!(values.is_null(1));
    }
}

#[tokio::test]
async fn direct_marker_staging_does_not_hide_later_scalar_cardinality() {
    let catalog = Catalog::default();
    register(&catalog, "outer_marker_guard", &["key"], vec![vec![1]]);
    register(&catalog, "marker_rhs", &["key"], vec![vec![2]]);
    register(
        &catalog,
        "marker_scalar_rhs",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20]],
    );
    for predicate in [
        "o.key IN (SELECT r.key FROM marker_rhs AS r) \
         AND (SELECT i.value FROM marker_scalar_rhs AS i WHERE i.key = o.key) = 10",
        "(SELECT i.value FROM marker_scalar_rhs AS i WHERE i.key = o.key) = 10 \
         AND o.key IN (SELECT r.key FROM marker_rhs AS r)",
    ] {
        let plan = plan_sql(
            &catalog,
            &format!("SELECT o.key FROM outer_marker_guard AS o WHERE {predicate}"),
        )
        .unwrap();
        let plan_text = format!("{plan:?}");
        assert!(
            plan_text.contains("LeftSingleJoin"),
            "{predicate}\n{plan:?}"
        );
        assert!(
            plan_text.contains("MarkJoin keys=0")
                || plan_text.contains("SemiJoin keys=1 null_equal_keys=false residual=true"),
            "a potentially multi-row scalar must retain a cardinality-enforcing guard: {predicate}\n{plan:?}"
        );
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
        let error = execute(plan, context)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("scalar subquery returned more than one row"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn guarded_scalar_projection_runs_only_for_active_correlated_rows() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "outer_projection_guard",
        &["key", "active"],
        vec![vec![1, 1], vec![2, 0]],
    );
    register(
        &catalog,
        "inner_projection_guard",
        &["key", "value"],
        vec![vec![1, 10], vec![2, 20]],
    );
    let batches = run(
        &catalog,
        "SELECT o.key, CASE WHEN o.active = 1 THEN (\
                    SELECT 10 / (i.value - 20) \
                    FROM inner_projection_guard AS i WHERE i.key = o.key\
                ) ELSE 0 END AS guarded_value \
         FROM outer_projection_guard AS o ORDER BY o.key",
    )
    .await;
    let keys = int64(&batches[0], 0);
    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((keys.value(0), values.value(0)), (1, -1.0));
    assert_eq!((keys.value(1), values.value(1)), (2, 0.0));

    let batches = run(
        &Catalog::default(),
        "SELECT CASE WHEN false THEN (SELECT 1 / 0) ELSE 0 END AS value",
    )
    .await;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(value.value(0), 0.0);
}

#[tokio::test]
async fn dead_subquery_shapes_do_not_evaluate_their_rhs() {
    let catalog = Catalog::default();
    register(&catalog, "outer_guard", &["key"], vec![vec![1]]);
    register(
        &catalog,
        "inner_guard",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20]],
    );
    let batches = run(
        &catalog,
        "SELECT nullif(CAST(NULL AS BIGINT), (\
                    SELECT value FROM inner_guard\
                )) AS nullif_value, \
                CASE WHEN false THEN 1 IN (SELECT 1 / 0) ELSE false END AS in_value, \
                CASE WHEN false THEN (SELECT 1 / 0 ORDER BY 1 LIMIT 1) ELSE 0 END AS ordered_value, \
                CASE WHEN false THEN (\
                    SELECT min(i.value) / 0 FROM inner_guard AS i WHERE i.key = o.key\
                ) ELSE 0 END AS aggregate_value \
         FROM outer_guard AS o",
    )
    .await;
    assert!(int64(&batches[0], 0).is_null(0));
    let in_value = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(!in_value.value(0));
    for column in [2, 3] {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(value.value(0), 0.0);
    }
}

#[tokio::test]
async fn aggregate_guards_preserve_duckdb_scalar_cardinality() {
    let catalog = Catalog::default();
    register(&catalog, "outer_guard", &["key"], vec![vec![1]]);
    register(
        &catalog,
        "inner_guard",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20]],
    );
    let batches = run(
        &catalog,
        "SELECT o.key, CASE WHEN EXISTS (\
                    SELECT 1 FROM inner_guard AS i WHERE i.key = 99\
                ) THEN (SELECT value FROM inner_guard) ELSE 0 END AS dependent_guard \
         FROM outer_guard AS o GROUP BY o.key \
         HAVING true OR ((SELECT value FROM inner_guard) = 10)",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).value(0), 1);
    assert_eq!(int64(&batches[0], 1).value(0), 0);

    let plan = plan_sql(
        &catalog,
        "SELECT o.key, CASE WHEN count(*) = 0 THEN (\
             SELECT i.value FROM inner_guard AS i WHERE i.key = o.key\
         ) ELSE 0 END \
         FROM outer_guard AS o GROUP BY o.key",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let error = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("scalar subquery returned more than one row"),
        "{error}"
    );
}

#[tokio::test]
async fn rebuilds_empty_global_aggregate_expressions() {
    let batches = run(
        &Catalog::default(),
        "SELECT (SELECT count(*) + 1 \
                 FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS count_plus_one, \
                (SELECT coalesce(sum(i.key), 42) \
                 FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS sum_default \
         FROM (SELECT 2 AS key) AS d",
    )
    .await;
    let count = int64(&batches[0], 0);
    let sum = decimal(&batches[0], 1);
    assert_eq!((count.value(0), sum.value(0)), (1, 42));
}

#[tokio::test]
async fn synthetic_left_row_never_contributes_to_aggregate_arguments() {
    let batches = run(
        &Catalog::default(),
        "SELECT (SELECT count(1) \
                 FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS count_constant, \
                (SELECT sum(1) \
                 FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS sum_constant, \
                (SELECT sum(coalesce(i.key, 1)) \
                 FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS sum_coalesced \
         FROM (SELECT 2 AS key) AS d",
    )
    .await;
    assert_eq!(int64(&batches[0], 0).value(0), 0);
    assert!(decimal(&batches[0], 1).is_null(0));
    assert!(decimal(&batches[0], 2).is_null(0));
}

#[tokio::test]
async fn correlated_aggregate_expression_arguments_preserve_empty_groups() {
    let catalog = Catalog::default();
    register(&catalog, "direct_outer", &["key"], vec![vec![1], vec![2]]);
    register(
        &catalog,
        "direct_inner",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20]],
    );
    let batches = run(
        &catalog,
        "SELECT o.key, \
                (SELECT sum(i.value + 1) FROM direct_inner AS i \
                 WHERE i.key = o.key) AS sum_value, \
                (SELECT avg(CAST(i.value AS DOUBLE)) FROM direct_inner AS i \
                 WHERE i.key = o.key) AS avg_value, \
                (SELECT count(1) FROM direct_inner AS i \
                 WHERE i.key = o.key) AS count_value \
         FROM direct_outer AS o ORDER BY o.key",
    )
    .await;
    let keys = int64(&batches[0], 0);
    let sums = decimal(&batches[0], 1);
    let averages = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let counts = int64(&batches[0], 3);
    assert_eq!(keys.values(), &[1, 2]);
    assert_eq!(sums.value(0), 32);
    assert!(sums.is_null(1));
    assert_eq!(averages.value(0), 15.0);
    assert!(averages.is_null(1));
    assert_eq!(counts.values(), &[2, 0]);
}

#[tokio::test]
async fn unmatched_inner_key_does_not_evaluate_fallible_aggregate_argument() {
    let catalog = Catalog::default();
    let sql = "SELECT (SELECT sum(CAST(i.value AS BIGINT)) \
               FROM (SELECT 2 AS key, 'bad' AS value) AS i \
               WHERE i.key = o.key) AS value \
               FROM (SELECT 1 AS key) AS o";
    let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(explain.contains("__rustdb_inner_match"), "{explain}");
    assert!(
        !explain.contains("rewrite=direct_correlated_aggregate"),
        "{explain}"
    );

    let batches = run(&catalog, sql).await;
    assert!(decimal(&batches[0], 0).is_null(0));
}

#[tokio::test]
async fn applies_having_after_rebuilding_the_empty_group() {
    let batches = run(
        &Catalog::default(),
        "SELECT (SELECT count(*) \
                 FROM (SELECT 1 AS key) AS i \
                 WHERE i.key = d.key \
                 HAVING count(*) > 0) AS n \
         FROM (SELECT 2 AS key) AS d",
    )
    .await;
    assert!(int64(&batches[0], 0).is_null(0));
}

#[tokio::test]
async fn grouped_count_without_a_group_remains_null() {
    let batches = run(
        &Catalog::default(),
        "SELECT (SELECT count(*) \
                 FROM (SELECT 1 AS key) AS i \
                 WHERE i.key = d.key GROUP BY i.key) AS n \
         FROM (SELECT 2 AS key) AS d",
    )
    .await;
    assert!(int64(&batches[0], 0).is_null(0));
}

#[tokio::test]
async fn aggregate_rows_drive_exists_and_in_before_having() {
    let batches = run(
        &Catalog::default(),
        "SELECT EXISTS (SELECT count(*) \
                        FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS exists_group, \
                0 IN (SELECT count(*) \
                      FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS in_group \
         FROM (SELECT 2 AS key) AS d",
    )
    .await;
    for column in 0..2 {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
}

#[tokio::test]
async fn evaluates_cross_side_residual_before_correlated_aggregate() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "outer_values",
        &["key", "threshold"],
        vec![vec![1, 5], vec![1, 15]],
    );
    register(
        &catalog,
        "inner_values",
        &["key", "value"],
        vec![vec![1, 4], vec![1, 10], vec![2, 99]],
    );
    let batches = run(
        &catalog,
        "SELECT o.threshold, \
                (SELECT avg(i.value) \
                 FROM inner_values AS i \
                 WHERE i.key = o.key AND i.value > o.threshold) AS average \
         FROM outer_values AS o ORDER BY o.threshold",
    )
    .await;
    let threshold = int64(&batches[0], 0);
    let average = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((threshold.value(0), threshold.value(1)), (5, 15));
    assert_eq!(average.value(0), 10.0);
    assert!(average.is_null(1));
}

#[tokio::test]
async fn evaluates_cross_side_residual_before_grouped_correlated_aggregate() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "outer_values",
        &["key", "threshold"],
        vec![vec![1, 5], vec![1, 15]],
    );
    register(
        &catalog,
        "inner_values",
        &["key", "value"],
        vec![vec![1, 4], vec![1, 10], vec![2, 99]],
    );
    let batches = run(
        &catalog,
        "SELECT o.threshold, \
                (SELECT avg(i.value) \
                 FROM inner_values AS i \
                 WHERE i.key = o.key AND i.value > o.threshold \
                 GROUP BY i.key) AS average \
         FROM outer_values AS o ORDER BY o.threshold",
    )
    .await;
    let average = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(average.value(0), 10.0);
    assert!(average.is_null(1));
}

async fn run(catalog: &Catalog, sql: &str) -> Vec<RecordBatch> {
    let plan = plan_sql(catalog, sql).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn int64(batch: &RecordBatch, column: usize) -> &Int64Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
}

fn decimal(batch: &RecordBatch, column: usize) -> &Decimal128Array {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
}

#[tokio::test]
async fn complex_residual_domain_uses_only_the_parameter_lineage() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "domain_outer",
        &["key", "threshold"],
        vec![vec![1, 5], vec![2, 100]],
    );
    register(
        &catalog,
        "domain_payload",
        &["key"],
        vec![vec![1], vec![1], vec![3]],
    );
    register(
        &catalog,
        "domain_inner",
        &["key", "value"],
        vec![vec![1, 10], vec![1, 20], vec![2, 200]],
    );
    let sql = "SELECT o.key, (SELECT count(*) FROM domain_inner AS i \
               WHERE i.key = o.key AND i.value > o.threshold) AS matches \
               FROM domain_outer AS o LEFT JOIN domain_payload AS p ON o.key = p.key \
               WHERE o.threshold > 0 ORDER BY o.key";
    let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
        panic!("expected query plan")
    };
    let explain = plan.explain();
    assert_eq!(
        explain.matches("Scan table=domain_payload").count(),
        1,
        "the parameter domain must not clone the irrelevant left-join branch:\n{explain}"
    );
    assert_eq!(
        explain.matches("Scan table=domain_outer").count(),
        2,
        "{explain}"
    );
    assert!(!explain.contains("DependentJoin"), "{explain}");

    let batches = run(&catalog, sql).await;
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let keys = int64(batch, 0);
            let matches = int64(batch, 1);
            (0..batch.num_rows())
                .map(|row| (keys.value(row), matches.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![(1, 2), (1, 2), (2, 1)]);
}

fn register(catalog: &Catalog, name: &str, columns: &[&str], rows: Vec<Vec<i64>>) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..columns.len())
        .map(|column| {
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row[column]).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    catalog
        .register(TableEntry::new(
            name,
            Arc::new(MemoryTable {
                batch,
                source_identity: None,
            }),
        ))
        .unwrap();
}

#[tokio::test]
async fn q21_pair_uses_one_shared_summary_scan_with_null_safe_existence_semantics() {
    let catalog = Catalog::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("suppkey", DataType::Int64, true),
        Field::new("late", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                1, 2, 2, 3, 3, 4, 4, 4, 6, 6, 7, 7, 8, 8, 8,
            ])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(10),
                Some(20),
                Some(10),
                Some(20),
                Some(10),
                Some(20),
                Some(30),
                None,
                Some(20),
                Some(10),
                None,
                Some(10),
                None,
                Some(20),
            ])),
            Arc::new(Int64Array::from(vec![
                2, 2, 0, 2, 1, 2, 0, 0, 2, 0, 2, 0, 2, 1, 0,
            ])),
        ],
    )
    .unwrap();
    catalog
        .register(TableEntry::new(
            "q21_lines",
            Arc::new(MemoryTable {
                batch,
                source_identity: None,
            }),
        ))
        .unwrap();
    let sql = "SELECT l1.orderkey FROM q21_lines AS l1 \
        WHERE l1.late = 2 \
          AND EXISTS (SELECT 1 FROM q21_lines AS l2 \
                      WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey) \
          AND NOT EXISTS (SELECT 1 FROM q21_lines AS l3 \
                          WHERE l3.orderkey = l1.orderkey \
                            AND l3.suppkey <> l1.suppkey AND l3.late = 1) \
        ORDER BY l1.orderkey";
    let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert_eq!(
        explain.matches("Scan table=q21_lines").count(),
        2,
        "{explain}"
    );
    assert!(explain.contains("__q21_all_min"), "{explain}");
    assert!(
        explain.contains("rewrite=existence_summary shared_build=true"),
        "{explain}"
    );
    assert!(!explain.contains("SemiJoin"), "{explain}");
    assert!(!explain.contains("AntiJoin"), "{explain}");

    let batches = run(&catalog, sql).await;
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let values = int64(batch, 0);
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, vec![2, 4, 8]);
}

#[tokio::test]
async fn q21_pair_shares_distinct_providers_for_the_same_source_spec() {
    let catalog = Catalog::default();
    let schema = Arc::new(Schema::new(vec![
        Field::new("orderkey", DataType::Int64, false),
        Field::new("suppkey", DataType::Int64, true),
        Field::new("late", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(10),
                Some(20),
            ])),
            Arc::new(Int64Array::from(vec![2, 0, 2, 1])),
        ],
    )
    .unwrap();
    let identity = TableSourceIdentity::from_spec(
        "memory",
        &["snapshot://q21-lines".to_owned()],
        "schema=v1".to_owned(),
    );
    let all: Arc<dyn TableProvider> = Arc::new(MemoryTable {
        batch: batch.clone(),
        source_identity: Some(identity.clone()),
    });
    let late: Arc<dyn TableProvider> = Arc::new(MemoryTable {
        batch: batch.clone(),
        source_identity: Some(identity),
    });
    assert!(!Arc::ptr_eq(&all, &late));
    for (name, provider) in [
        (
            "q21_identity_outer",
            Arc::new(MemoryTable {
                batch: batch.clone(),
                source_identity: None,
            }) as Arc<dyn TableProvider>,
        ),
        ("q21_identity_all", all),
        ("q21_identity_late", late),
    ] {
        catalog.register(TableEntry::new(name, provider)).unwrap();
    }

    let sql = "SELECT l1.orderkey FROM q21_identity_outer AS l1 \
        WHERE l1.late = 2 \
          AND EXISTS (SELECT 1 FROM q21_identity_all AS l2 \
                      WHERE l2.orderkey = l1.orderkey AND l2.suppkey <> l1.suppkey) \
          AND NOT EXISTS (SELECT 1 FROM q21_identity_late AS l3 \
                          WHERE l3.orderkey = l1.orderkey \
                            AND l3.suppkey <> l1.suppkey AND l3.late = 1) \
        ORDER BY l1.orderkey";
    let StatementPlan::Query(plan) = plan_sql(&catalog, sql).unwrap() else {
        panic!("expected query plan");
    };
    let explain = plan.explain();
    assert!(
        explain.contains("rewrite=existence_summary shared_build=true"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=q21_identity_all"),
        "{explain}"
    );
    assert_eq!(
        explain.matches("Scan table=q21_identity_late").count(),
        1,
        "{explain}"
    );

    let batches = run(&catalog, sql).await;
    assert_eq!(int64(&batches[0], 0).values(), &[1]);
}

#[derive(Clone)]
struct MemoryTable {
    batch: RecordBatch,
    source_identity: Option<TableSourceIdentity>,
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

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        self.source_identity.clone()
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
