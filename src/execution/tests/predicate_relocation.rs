use std::sync::Arc;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use crate::Catalog;

use super::{register, run};

#[test]
fn q21_shaped_filters_move_below_semi_and_anti_joins() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "relocation_q21_left",
        int_table(
            &[
                "orderkey",
                "suppkey",
                "orderstatus",
                "receipt",
                "commit",
                "nation",
            ],
            &[vec![1], vec![10], vec![1], vec![2], vec![1], vec![20]],
        ),
    );
    register(
        &catalog,
        "relocation_q21_rhs",
        int_table(
            &["orderkey", "suppkey", "receipt", "commit"],
            &[vec![1], vec![11], vec![2], vec![1]],
        ),
    );
    let sql = "SELECT l.orderkey FROM relocation_q21_left AS l \
               WHERE l.orderstatus = 1 \
                 AND l.receipt > l.commit \
                 AND l.nation = 20 \
                 AND EXISTS ( \
                   SELECT * FROM relocation_q21_rhs AS e \
                   WHERE e.orderkey = l.orderkey AND e.suppkey <> l.suppkey \
                 ) \
                 AND NOT EXISTS ( \
                   SELECT * FROM relocation_q21_rhs AS a \
                   WHERE a.orderkey = l.orderkey AND a.suppkey <> l.suppkey \
                     AND a.receipt > a.commit \
                 )";
    let explain = format!("{:?}", crate::sql::plan_sql(&catalog, sql).unwrap());
    let anti = position(&explain, "AntiJoin");
    let semi = position(&explain, "SemiJoin");
    let order_status = position(&explain, "Filter orderstatus = 1");
    let receipt = position(&explain, "Filter receipt > commit");
    let nation = position(&explain, "Filter nation = 20");
    let scan = position(&explain, "Scan table=relocation_q21_left");

    assert!(anti < semi, "{explain}");
    assert!(
        semi < order_status && semi < receipt && semi < nation,
        "{explain}"
    );
    assert!(
        order_status < scan && receipt < scan && nation < scan,
        "{explain}"
    );
}

#[tokio::test]
async fn relocated_null_aware_anti_filter_preserves_results() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "relocation_left",
        int_table(&["id", "keep"], &[vec![1, 2, 3], vec![1, 0, 1]]),
    );
    register(&catalog, "relocation_right", int_table(&["id"], &[vec![2]]));

    let batches = run(
        &catalog,
        "SELECT id FROM relocation_left WHERE keep = 1 \
         AND id NOT IN (SELECT id FROM relocation_right) ORDER BY id",
        1 << 20,
    )
    .await;
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 3]);
}

#[tokio::test]
async fn fallible_arithmetic_is_not_moved_before_anti_join() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "relocation_error_left",
        int_table(&["id", "divisor"], &[vec![1, 2], vec![0, 2]]),
    );
    register(
        &catalog,
        "relocation_error_right",
        int_table(&["id"], &[vec![1]]),
    );
    let sql = "SELECT id FROM relocation_error_left \
               WHERE 10 / divisor > 1 \
                 AND id NOT IN (SELECT id FROM relocation_error_right)";
    let explain = format!("{:?}", crate::sql::plan_sql(&catalog, sql).unwrap());
    assert!(
        position(&explain, "Filter 10 / divisor > 1") < position(&explain, "NullAwareAntiJoin"),
        "{explain}"
    );

    let batches = run(&catalog, sql, 1 << 20).await;
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[2]);
}

fn position(text: &str, needle: &str) -> usize {
    text.find(needle)
        .unwrap_or_else(|| panic!("missing '{needle}' in plan:\n{text}"))
}

fn int_table(names: &[&str], values: &[Vec<i64>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let columns = values
        .iter()
        .map(|values| Arc::new(Int64Array::from(values.clone())) as _)
        .collect();
    RecordBatch::try_new(schema, columns).unwrap()
}
