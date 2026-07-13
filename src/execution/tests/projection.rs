use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Date32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;

use crate::{
    Catalog,
    execution::execute,
    runtime::{MemoryPool, QueryContext},
};

use super::{int64_at, register};

#[tokio::test]
async fn optimizer_prunes_q20_shaped_subquery_attachments() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "partsupp_pruning",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("ps_partkey", DataType::Int64, false),
                Field::new("ps_suppkey", DataType::Int64, false),
                Field::new("ps_availqty", DataType::Int64, false),
                Field::new("ps_supplycost", DataType::Int64, false),
                Field::new("ps_comment", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![99])),
                Arc::new(Int64Array::from(vec![99])),
            ],
        )
        .unwrap(),
    );
    register(
        &catalog,
        "part_pruning",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "p_partkey",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap(),
    );
    register(&catalog, "lineitem_pruning", lineitem());

    let sql = "SELECT ps.ps_suppkey \
               FROM partsupp_pruning AS ps \
               WHERE ps.ps_partkey IN (SELECT p.p_partkey FROM part_pruning AS p) \
                 AND ps.ps_availqty > ( \
                   SELECT sum(l.l_quantity) FROM lineitem_pruning AS l \
                   WHERE l.l_partkey = ps.ps_partkey \
                     AND l.l_suppkey = ps.ps_suppkey \
                     AND l.l_shipdate >= DATE '1994-01-01' \
                 )";
    let plan = crate::sql::plan_sql(&catalog, sql).unwrap();
    let explain = format!("{plan:?}");
    assert!(
        explain.contains("Scan table=partsupp_pruning projection=Some([0, 1, 2])"),
        "{explain}"
    );
    assert_eq!(
        explain.matches("Scan table=partsupp_pruning").count(),
        1,
        "direct equality aggregation must not clone the outer domain scan:\n{explain}"
    );
    assert!(
        !explain.contains("Scan table=partsupp_pruning projection=Some([0, 1, 2, 3, 4])"),
        "{explain}"
    );
    assert!(
        explain.contains("Scan table=lineitem_pruning projection=Some([1, 2, 4, 10])"),
        "{explain}"
    );

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(int64_at(&batches[0], 0), 2);
}

#[test]
fn optimizer_prunes_unused_pass_throughs_but_keeps_complex_dependencies() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "projection_mix",
        int_table(&["kept", "dropped", "checked"]),
    );

    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT kept FROM ( \
           SELECT kept, dropped, checked + 1 AS evaluated, 7 AS constant \
           FROM projection_mix \
         ) AS projected",
    )
    .unwrap();
    let explain = format!("{plan:?}");

    assert!(
        explain.contains("Scan table=projection_mix projection=Some([0, 2])"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=projection_mix projection=Some([0, 1, 2])"),
        "{explain}"
    );
}

#[test]
fn optimizer_prunes_q2_shaped_guarded_scalar_pass_throughs() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "part_q2_pruning",
        int_table(&["p_partkey", "p_mfgr", "p_unused"]),
    );
    register(
        &catalog,
        "partsupp_q2_pruning",
        int_table(&[
            "ps_partkey",
            "ps_suppkey",
            "ps_availqty",
            "ps_supplycost",
            "ps_comment",
        ]),
    );

    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT p.p_partkey, p.p_mfgr, ps.ps_supplycost \
         FROM part_q2_pruning AS p \
         JOIN partsupp_q2_pruning AS ps ON p.p_partkey = ps.ps_partkey \
         WHERE ps.ps_supplycost = ( \
           SELECT min(ps2.ps_supplycost) \
           FROM partsupp_q2_pruning AS ps2 \
           WHERE ps2.ps_partkey = p.p_partkey \
         )",
    )
    .unwrap();
    let explain = format!("{plan:?}");

    assert!(
        explain.contains("Scan table=part_q2_pruning projection=Some([0, 1])"),
        "{explain}"
    );
    assert!(
        explain.contains("Scan table=partsupp_q2_pruning projection=Some([0, 3])"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=part_q2_pruning projection=Some([0, 1, 2])"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=partsupp_q2_pruning projection=Some([0, 1, 2, 3, 4])"),
        "{explain}"
    );
}

#[test]
fn optimizer_prunes_q17_shaped_guarded_scalar_pass_throughs() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "lineitem_q17_pruning",
        int_table(&["l_partkey", "l_quantity", "l_extendedprice", "l_unused"]),
    );
    register(
        &catalog,
        "part_q17_pruning",
        int_table(&["p_partkey", "p_brand", "p_container", "p_unused"]),
    );

    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT sum(l.l_extendedprice) / 7.0 AS avg_yearly \
         FROM lineitem_q17_pruning AS l \
         JOIN part_q17_pruning AS p ON l.l_partkey = p.p_partkey \
         WHERE p.p_brand = 23 \
           AND p.p_container = 7 \
           AND l.l_quantity < ( \
             SELECT 0.2 * avg(l2.l_quantity) \
             FROM lineitem_q17_pruning AS l2 \
             WHERE l2.l_partkey = p.p_partkey \
           )",
    )
    .unwrap();
    let explain = format!("{plan:?}");

    assert!(
        explain.contains("Scan table=lineitem_q17_pruning projection=Some([0, 1, 2])"),
        "{explain}"
    );
    assert!(
        explain.contains("Scan table=part_q17_pruning projection=Some([0, 1, 2])"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=lineitem_q17_pruning projection=Some([0, 1, 2, 3])"),
        "{explain}"
    );
    assert!(
        !explain.contains("Scan table=part_q17_pruning projection=Some([0, 1, 2, 3])"),
        "{explain}"
    );
}

fn int_table(names: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let columns = names
        .iter()
        .enumerate()
        .map(|(index, _)| Arc::new(Int64Array::from(vec![index as i64 + 1])) as ArrayRef)
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, columns).unwrap()
}

fn lineitem() -> RecordBatch {
    let names = [
        "l_orderkey",
        "l_partkey",
        "l_suppkey",
        "l_linenumber",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_returnflag",
        "l_linestatus",
        "l_shipdate",
        "l_comment",
    ];
    let fields = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            Field::new(
                *name,
                if index == 10 {
                    DataType::Date32
                } else {
                    DataType::Int64
                },
                false,
            )
        })
        .collect::<Vec<_>>();
    let columns = (0..names.len())
        .map(|index| {
            if index == 10 {
                Arc::new(Date32Array::from(vec![8_766])) as ArrayRef
            } else {
                let value = match index {
                    1 => 1,
                    2 => 2,
                    4 => 4,
                    _ => 99,
                };
                Arc::new(Int64Array::from(vec![value])) as ArrayRef
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}
