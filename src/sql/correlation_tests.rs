use arrow::array::{Array, Int64Array};
use futures::TryStreamExt;

use crate::execution::execute;
use crate::runtime::{MemoryPool, QueryContext};
use crate::{Catalog, Error};

use super::{StatementPlan, plan_sql};

fn explain(sql: &str) -> String {
    let StatementPlan::Query(plan) = plan_sql(&Catalog::default(), sql).unwrap() else {
        panic!("expected query plan")
    };
    plan.explain()
}

#[test]
fn decorrelates_exists_and_cross_side_residual() {
    let plan = explain(
        "SELECT d.value \
         FROM (SELECT 1 AS value) AS d \
         WHERE EXISTS (\
             SELECT 1 FROM (SELECT 1 AS key, 2 AS other_key) AS i \
             WHERE i.key = d.value AND i.other_key <> d.value\
         )",
    );
    assert!(
        plan.contains("SemiJoin keys=1 null_equal_keys=false residual=true"),
        "{plan}"
    );
    assert!(!plan.contains("DependentJoin"), "{plan}");
}

#[test]
fn keeps_exists_in_an_expression_as_mark_join() {
    let plan = explain(
        "SELECT d.value, \
                EXISTS (SELECT 1 FROM (SELECT 1 AS key) AS i WHERE i.key = d.value) \
         FROM (SELECT 1 AS value) AS d",
    );
    assert!(plan.contains("MarkJoin keys=1"), "{plan}");
}

#[test]
fn decorrelates_scalar_aggregate_and_preserves_count_empty_value() {
    let plan = explain(
        "SELECT d.value, \
                (SELECT count(*) FROM (SELECT 1 AS key) AS i WHERE i.key = d.value) \
         FROM (SELECT 1 AS value) AS d",
    );
    assert!(plan.contains("LeftJoin keys=1"), "{plan}");
    assert!(!plan.contains("DependentJoin"), "{plan}");
}

#[test]
fn compacts_correlated_aggregate_input_before_the_domain_join() {
    let plan = explain(
        "SELECT d.partkey, \
                (SELECT avg(i.quantity) \
                 FROM (SELECT 1 AS partkey, 10 AS quantity, 99 AS unused_payload) AS i \
                 WHERE i.partkey = d.partkey) \
         FROM (SELECT 1 AS partkey) AS d",
    );
    assert!(
        plan.contains("Projection [\"partkey\", \"quantity\"]"),
        "{plan}"
    );
    assert!(
        !plan.contains(
            "Projection [\"partkey\", \"quantity\", \"unused_payload\", \"__rustdb_inner_match\"]"
        ),
        "{plan}"
    );
}

#[test]
fn scalar_aggregate_equality_correlation_skips_outer_domain_scan() {
    let plan = explain(
        "SELECT d.partkey, \
                (SELECT avg(i.quantity) \
                 FROM (SELECT 1 AS partkey, 10 AS quantity) AS i \
                 WHERE i.partkey = d.partkey) \
         FROM (SELECT 1 AS partkey) AS d",
    );
    assert_eq!(
        plan.matches("Aggregate groups=[\"partkey\"]").count(),
        1,
        "{plan}"
    );
    assert!(!plan.contains("__rustdb_inner_match"), "{plan}");
    assert!(plan.contains("LeftJoin keys=1"), "{plan}");
    assert!(
        plan.contains("rewrite=direct_correlated_aggregate"),
        "{plan}"
    );
}

#[test]
fn guarded_scalar_aggregate_uses_direct_grouped_rewrite() {
    let plan = explain(
        "SELECT d.key FROM (SELECT 1 AS key, true AS enabled) AS d \
         WHERE d.enabled AND 5 < (SELECT 0.2 * avg(i.quantity) \
         FROM (SELECT 1 AS key, 30 AS quantity) AS i WHERE i.key = d.key)",
    );
    assert!(
        plan.contains("rewrite=direct_correlated_aggregate"),
        "{plan}"
    );
    assert!(!plan.contains("__rustdb_domain_value"), "{plan}");
}

#[test]
fn direct_scalar_aggregate_handles_count_sum_and_multiple_results() {
    for sql in [
        "SELECT (SELECT count(*) FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) \
         FROM (SELECT 2 AS key) AS d",
        "SELECT (SELECT sum(i.value) FROM (SELECT 1 AS key, 4 AS value) AS i \
         WHERE i.key = d.key) FROM (SELECT 2 AS key) AS d",
        "SELECT (SELECT count(*) + sum(i.value) \
         FROM (SELECT 1 AS key, 4 AS value) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
    ] {
        let plan = explain(sql);
        assert_eq!(
            plan.matches("Aggregate groups=[\"key\"]").count(),
            1,
            "{plan}"
        );
        assert!(!plan.contains("__rustdb_inner_match"), "{plan}");
    }
}

#[test]
fn direct_correlated_aggregate_accepts_infallible_expression_arguments() {
    for sql in [
        "SELECT (SELECT count(1) \
         FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
        "SELECT (SELECT sum(i.value * 1e0) \
         FROM (SELECT 1 AS key, 2e0 AS value) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
    ] {
        let plan = explain(sql);
        assert!(
            plan.contains("rewrite=direct_correlated_aggregate"),
            "{sql}\n{plan}"
        );
        assert!(!plan.contains("__rustdb_inner_match"), "{sql}\n{plan}");
    }
}

#[test]
fn fallible_correlated_aggregate_expression_keeps_the_domain_path() {
    for sql in [
        "SELECT (SELECT sum(i.value + 1) \
         FROM (SELECT 1 AS key, 2 AS value) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
        "SELECT (SELECT avg(CAST(i.value AS DOUBLE)) \
         FROM (SELECT 1 AS key, 2 AS value) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
        "SELECT (SELECT avg(10 / i.value) \
         FROM (SELECT 1 AS key, 2 AS value) AS i WHERE i.key = d.key) \
         FROM (SELECT 1 AS key) AS d",
    ] {
        let plan = explain(sql);
        assert!(plan.contains("__rustdb_inner_match"), "{sql}\n{plan}");
        assert!(
            !plan.contains("rewrite=direct_correlated_aggregate"),
            "{sql}\n{plan}"
        );
    }
}

#[test]
fn guarded_fallible_correlated_aggregate_keeps_the_domain_path() {
    let plan = explain(
        "SELECT CASE WHEN d.enabled THEN (SELECT avg(10 / i.value) \
         FROM (SELECT 1 AS key, 2 AS value) AS i WHERE i.key = d.key) ELSE 0 END \
         FROM (SELECT 1 AS key, false AS enabled) AS d",
    );
    assert!(plan.contains("__rustdb_inner_match"), "{plan}");
    assert!(
        !plan.contains("rewrite=direct_correlated_aggregate"),
        "{plan}"
    );
}

#[test]
fn uses_left_single_for_non_aggregate_scalar_subquery() {
    let plan = explain(
        "SELECT d.value, \
                (SELECT i.key FROM (SELECT 1 AS key) AS i WHERE i.key = d.value) \
         FROM (SELECT 1 AS value) AS d",
    );
    assert!(plan.contains("LeftSingleJoin keys=1"), "{plan}");
}

#[test]
fn lowers_top_level_not_in_to_null_aware_anti() {
    let plan = explain(
        "SELECT d.value \
         FROM (SELECT 1 AS value) AS d \
         WHERE d.value > 0 \
           AND d.value NOT IN (SELECT i.key FROM (SELECT 1 AS key) AS i)",
    );
    assert!(plan.contains("NullAwareAntiJoin"), "{plan}");
    assert!(plan.contains("null_aware=true"), "{plan}");
}

#[test]
fn stages_q16_style_rightmost_not_in_as_global_membership() {
    let plan = explain(
        "SELECT p.brand, count(DISTINCT ps.supp) \
         FROM (SELECT 1 AS part, 10 AS supp) AS ps \
         JOIN (SELECT 1 AS part, 'Brand#12' AS brand, 'SMALL' AS kind) AS p \
           ON ps.part = p.part \
         WHERE p.brand <> 'Brand#45' \
           AND p.kind NOT LIKE 'MEDIUM POLISHED%' \
           AND ps.supp NOT IN (\
               SELECT s.supp FROM (SELECT 20 AS supp, 'Customer Complaints' AS note) AS s \
               WHERE s.note LIKE '%Customer%Complaints%'\
           ) \
         GROUP BY p.brand",
    );
    assert!(
        plan.contains(
            "NullAwareAntiJoin keys=0 null_equal_keys=false residual=false null_aware=true"
        ),
        "{plan}"
    );
    assert!(!plan.contains("MarkJoin"), "{plan}");
}

#[test]
fn stages_q21_style_correlated_exists_pairs_independently() {
    let plan = explain(
        "SELECT l1.supp \
         FROM (SELECT 1 AS order_key, 10 AS supp, 1 AS late) AS l1 \
         WHERE l1.late = 1 \
           AND EXISTS (\
               SELECT 1 FROM (SELECT 1 AS order_key, 20 AS supp) AS l2 \
               WHERE l2.order_key = l1.order_key AND l2.supp <> l1.supp\
           ) \
           AND NOT EXISTS (\
               SELECT 1 FROM (SELECT 1 AS order_key, 30 AS supp, 0 AS late) AS l3 \
               WHERE l3.order_key = l1.order_key \
                 AND l3.supp <> l1.supp AND l3.late = 1\
           ) \
         GROUP BY l1.supp",
    );
    assert!(
        plan.contains("SemiJoin keys=1 null_equal_keys=false residual=true"),
        "{plan}"
    );
    assert!(
        plan.contains("AntiJoin keys=1 null_equal_keys=false residual=true"),
        "{plan}"
    );
    assert!(!plan.contains("MarkJoin"), "{plan}");
    assert!(!plan.contains("DependentJoin"), "{plan}");
}

#[test]
fn lowers_correlated_not_in_with_separate_group_and_membership_keys() {
    let plan = explain(
        "SELECT d.value \
         FROM (SELECT 1 AS value, 7 AS grp) AS d \
         WHERE d.value NOT IN (\
             SELECT i.key FROM (SELECT 2 AS key, 7 AS grp) AS i \
             WHERE i.grp = d.grp\
         )",
    );
    assert!(
        plan.contains(
            "NullAwareAntiJoin keys=1 null_equal_keys=false residual=false null_aware=true"
        ),
        "{plan}"
    );
}

#[test]
fn stages_direct_in_before_a_following_correlated_aggregate() {
    let plan = explain(
        "SELECT ps.supp \
         FROM (SELECT 1 AS supp, 10 AS part, 5 AS avail) AS ps \
         WHERE ps.part IN (\
             SELECT p.part FROM (SELECT 10 AS part) AS p\
         ) AND ps.avail > (\
             SELECT 0.5 * sum(l.qty) \
             FROM (SELECT 10 AS part, 1 AS supp, 2 AS qty) AS l \
             WHERE l.part = ps.part AND l.supp = ps.supp\
         )",
    );
    assert!(!plan.contains("MarkJoin keys=0"), "{plan}");
    assert_eq!(
        plan.matches("SemiJoin keys=1").count(),
        1,
        "direct aggregation must not rebuild the membership-filtered correlation domain:\n{plan}"
    );
    assert!(plan.contains("LeftJoin keys=2"), "{plan}");
    assert!(
        plan.contains("rewrite=direct_correlated_aggregate"),
        "{plan}"
    );
    assert!(!plan.contains("__rustdb_domain_value"), "{plan}");
}

#[test]
fn lowers_staged_direct_membership_and_existence_filters() {
    for (sql, expected) in [
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value IN (SELECT 1) AND TRUE",
            "SemiJoin keys=1",
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value NOT IN (SELECT 2) AND TRUE",
            "NullAwareAntiJoin keys=0",
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE EXISTS (SELECT 1) AND TRUE",
            "SemiJoin keys=0",
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE NOT EXISTS (SELECT 1 WHERE FALSE) AND TRUE",
            "AntiJoin keys=0",
        ),
    ] {
        let plan = explain(sql);
        assert!(plan.contains(expected), "expected {expected}:\n{plan}");
        assert!(!plan.contains("MarkJoin"), "{plan}");
    }
}

#[tokio::test]
async fn staged_direct_filters_preserve_in_and_exists_null_semantics() {
    for (sql, expected_rows) in [
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value IN (SELECT 1) AND TRUE",
            1,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value IN (SELECT CAST(NULL AS BIGINT)) AND TRUE",
            0,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value NOT IN (SELECT CAST(NULL AS BIGINT)) AND TRUE",
            0,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE d.value NOT IN (SELECT 1 WHERE FALSE) AND TRUE",
            1,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE EXISTS (SELECT 1) AND TRUE",
            1,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE NOT EXISTS (SELECT 1 WHERE FALSE) AND TRUE",
            1,
        ),
    ] {
        let rows = run(sql)
            .await
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(rows, expected_rows, "{sql}");
    }
}

#[tokio::test]
async fn rightmost_not_in_preserves_the_global_null_matrix() {
    for (sql, expected_rows) in [
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE TRUE AND d.value NOT IN (SELECT 2)",
            1,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE TRUE AND d.value NOT IN (SELECT 1)",
            0,
        ),
        (
            "SELECT d.value FROM (SELECT 1 AS value) AS d \
             WHERE TRUE AND d.value NOT IN (SELECT CAST(NULL AS BIGINT))",
            0,
        ),
        (
            "SELECT d.value FROM (SELECT CAST(NULL AS BIGINT) AS value) AS d \
             WHERE TRUE AND d.value NOT IN (SELECT 1 WHERE FALSE)",
            1,
        ),
        (
            "SELECT d.value FROM (SELECT CAST(NULL AS BIGINT) AS value) AS d \
             WHERE TRUE AND d.value NOT IN (SELECT 1)",
            0,
        ),
    ] {
        let rows = run(sql)
            .await
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(rows, expected_rows, "{sql}");
    }
}

#[test]
fn rejects_pure_inequality_correlation() {
    let error = plan_sql(
        &Catalog::default(),
        "SELECT d.value \
         FROM (SELECT 1 AS value) AS d \
         WHERE EXISTS (SELECT 1 FROM (SELECT 2 AS key) AS i WHERE i.key > d.value)",
    )
    .unwrap_err();
    assert!(
        matches!(error, Error::Unsupported(_))
            && error
                .to_string()
                .contains("at least one outer-to-inner equality key"),
        "{error}"
    );
}

#[tokio::test]
async fn nested_scalar_correlation_does_not_escape_into_enclosing_in_rhs() {
    let sql = "SELECT s.id \
               FROM (SELECT 1 AS id) AS s \
               WHERE s.id IN (\
                   SELECT ps.supp \
                   FROM (SELECT 1 AS supp, 10 AS part, 5 AS avail) AS ps \
                   WHERE ps.avail > (\
                       SELECT 0.5 * sum(l.qty) \
                       FROM (SELECT 10 AS part, 1 AS supp, 2 AS qty) AS l \
                       WHERE l.part = ps.part AND l.supp = ps.supp\
                   )\
               )";
    let plan = explain(sql);
    assert!(plan.contains("SemiJoin keys=1"), "{plan}");
    assert!(plan.contains("LeftJoin keys=2"), "{plan}");
    assert!(!plan.contains("DependentJoin"), "{plan}");

    let batches = run(sql).await;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value.value(0), 1);
}

#[tokio::test]
async fn executes_parser_visible_left_semi_and_anti_joins() {
    let semi = run("SELECT l.id \
         FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 2) AS l \
         LEFT SEMI JOIN (SELECT 2 AS id UNION ALL SELECT 3) AS r ON l.id = r.id \
         ORDER BY l.id")
    .await;
    assert_eq!(
        semi.iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        vec![2, 2]
    );

    let anti = run("SELECT l.id \
         FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 2) AS l \
         LEFT ANTI JOIN (SELECT 2 AS id UNION ALL SELECT 3) AS r ON l.id = r.id \
         ORDER BY l.id")
    .await;
    assert_eq!(int64_value(&anti[0], 0), 1);
}

#[tokio::test]
async fn exists_never_evaluates_its_visible_projection() {
    let batches = run(
        "SELECT EXISTS (SELECT 1 / 0 FROM (SELECT 1 AS key) AS i) AS plain_exists, \
                EXISTS (SELECT 1 / 0 FROM (SELECT 1 AS key) AS i \
                        WHERE i.key = o.key) AS correlated_exists, \
                NOT EXISTS (SELECT 1 / 0 FROM (SELECT 2 AS key) AS i \
                            WHERE i.key = o.key) AS correlated_not_exists, \
                CASE WHEN EXISTS (SELECT 1 / 0) THEN 7 ELSE 0 END AS case_value \
         FROM (SELECT 1 AS key) AS o",
    )
    .await;
    for column in 0..3 {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
    assert_eq!(int64_value(&batches[0], 3), 7);
}

#[tokio::test]
async fn exists_never_evaluates_a_distinct_visible_projection() {
    let batches = run("SELECT EXISTS (\
             SELECT DISTINCT 1 / 0 FROM (SELECT 1 AS key) AS i\
         ) AS plain_distinct, \
         EXISTS (\
             SELECT DISTINCT 1 / 0 FROM (SELECT 1 AS key) AS i \
             WHERE i.key = o.key\
         ) AS correlated_distinct \
         FROM (SELECT 1 AS key) AS o")
    .await;
    for column in 0..2 {
        let value = batches[0]
            .column(column)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(value.value(0));
    }
}

#[tokio::test]
async fn exists_with_offset_still_erases_a_non_distinct_projection() {
    let batches = run("SELECT EXISTS (\
             SELECT 1 / 0 FROM (SELECT 1 AS key) AS i OFFSET 0\
         ) AS offset_exists")
    .await;
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    assert!(value.value(0));
}

#[tokio::test]
async fn correlated_scalar_aggregate_uses_sql_empty_group_defaults() {
    let batches = run("SELECT (SELECT count(*) \
                 FROM (SELECT 1 AS key) AS i \
                 WHERE i.key = d.value) AS n, \
                (SELECT max(i.key) \
                 FROM (SELECT 1 AS key) AS i \
                 WHERE i.key = d.value) AS maximum \
         FROM (SELECT 2 AS value) AS d")
    .await;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 0);
    assert!(maximum.is_null(0));
}

#[tokio::test]
async fn direct_scalar_aggregate_preserves_count_sum_and_multiple_aggregate_semantics() {
    let batches = run("SELECT (SELECT count(*) \
             FROM (SELECT 1 AS key) AS i WHERE i.key = d.key) AS empty_count, \
            (SELECT sum(i.value) \
             FROM (SELECT 1 AS key, 4 AS value) AS i WHERE i.key = d.key) AS empty_sum, \
            (SELECT count(*) + sum(i.value) \
             FROM (SELECT 2 AS key, 4 AS value) AS i WHERE i.key = d.key) AS combined \
         FROM (SELECT 2 AS key) AS d")
    .await;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let combined = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(count.value(0), 0);
    assert!(sum.is_null(0));
    assert_eq!(combined.value(0), 5);
}

async fn run(sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
    let plan = plan_sql(&Catalog::default(), sql).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    execute(plan, context)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn int64_value(batch: &arrow::record_batch::RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
