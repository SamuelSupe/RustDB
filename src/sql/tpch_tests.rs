use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, Date32Array, Decimal128Array, Int64Array, StringArray},
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

use super::{plan_sql, tpch_queries::ALL};

#[tokio::test]
async fn plans_and_executes_tpch_smoke_queries() {
    let catalog = tpch_catalog();
    for (name, sql) in ALL {
        let plan = plan_sql(&catalog, sql).unwrap_or_else(|error| panic!("{name} plan: {error}"));
        let expected_hash_joins = match name {
            "Q3" => 2,
            "Q11" => 4,
            "Q12" | "Q14" => 1,
            _ => 0,
        };
        let explain = format!("{plan:?}");
        assert!(
            !explain.contains("Join keys=0"),
            "{name} must not plan a Cartesian join:\n{explain}"
        );
        assert!(
            explain.matches("InnerJoin keys=1").count() >= expected_hash_joins,
            "{name} should plan at least {expected_hash_joins} keyed hash joins:\n{explain}"
        );
        let temp = tempfile::tempdir().unwrap();
        let context = QueryContext::shared(MemoryPool::new(8 << 20), temp.path()).unwrap();
        let batches = execute(plan, context)
            .await
            .unwrap_or_else(|error| panic!("{name} start: {error}"))
            .try_collect::<Vec<_>>()
            .await
            .unwrap_or_else(|error| panic!("{name} execute: {error}"));
        let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        assert!(rows > 0, "{name} should return a smoke-fixture row");
        assert_smoke_value(name, &batches[0]);
    }
}

fn assert_smoke_value(name: &str, batch: &RecordBatch) {
    match name {
        "Q1" => assert_eq!(int64(batch, 9), 3),
        "Q3" => {
            assert_eq!(int64(batch, 0), 10);
            assert!(decimal(batch, 1) > 0);
        }
        "Q6" => assert!(decimal(batch, 0) > 0),
        "Q11" => {
            assert_eq!(int64(batch, 0), 100);
            assert!(decimal(batch, 1) > 0);
        }
        "Q12" => {
            assert_eq!(string(batch, 0), "MAIL");
            assert_eq!((int64(batch, 1), int64(batch, 2)), (1, 0));
        }
        "Q13" => assert_eq!((int64(batch, 0), int64(batch, 1)), (1, 1)),
        "Q14" => {
            assert!(!batch.column(0).is_null(0));
            assert!(decimal(batch, 0) > 0);
        }
        _ => unreachable!(),
    }
}

fn tpch_catalog() -> Catalog {
    let catalog = Catalog::default();
    register(
        &catalog,
        "customer",
        batch(
            vec![i64_field("c_custkey"), text_field("c_mktsegment")],
            vec![i64s(&[1]), strings(&["BUILDING"])],
        ),
    );
    register(
        &catalog,
        "orders",
        batch(
            vec![
                i64_field("o_orderkey"),
                i64_field("o_custkey"),
                date_field("o_orderdate"),
                i64_field("o_shippriority"),
                text_field("o_orderpriority"),
                text_field("o_comment"),
            ],
            vec![
                i64s(&[10]),
                i64s(&[1]),
                dates(&["1995-03-01"]),
                i64s(&[0]),
                strings(&["1-URGENT"]),
                strings(&["ordinary order"]),
            ],
        ),
    );
    register(&catalog, "lineitem", lineitem());
    register(
        &catalog,
        "partsupp",
        batch(
            vec![
                i64_field("ps_partkey"),
                i64_field("ps_suppkey"),
                decimal_field("ps_supplycost", 15, 2),
                i64_field("ps_availqty"),
            ],
            vec![
                i64s(&[100]),
                i64s(&[200]),
                decimals(&[200], 15, 2),
                i64s(&[100]),
            ],
        ),
    );
    register(
        &catalog,
        "supplier",
        batch(
            vec![i64_field("s_suppkey"), i64_field("s_nationkey")],
            vec![i64s(&[200]), i64s(&[300])],
        ),
    );
    register(
        &catalog,
        "nation",
        batch(
            vec![i64_field("n_nationkey"), text_field("n_name")],
            vec![i64s(&[300]), strings(&["GERMANY"])],
        ),
    );
    register(
        &catalog,
        "part",
        batch(
            vec![i64_field("p_partkey"), text_field("p_type")],
            vec![i64s(&[100]), strings(&["PROMO ITEM"])],
        ),
    );
    catalog
}

fn lineitem() -> RecordBatch {
    batch(
        vec![
            i64_field("l_orderkey"),
            i64_field("l_partkey"),
            i64_field("l_quantity"),
            decimal_field("l_extendedprice", 10, 2),
            decimal_field("l_discount", 4, 2),
            decimal_field("l_tax", 4, 2),
            text_field("l_returnflag"),
            text_field("l_linestatus"),
            date_field("l_shipdate"),
            date_field("l_commitdate"),
            date_field("l_receiptdate"),
            text_field("l_shipmode"),
        ],
        vec![
            i64s(&[10, 10, 10]),
            i64s(&[100, 100, 100]),
            i64s(&[10, 10, 10]),
            decimals(&[10_000, 10_000, 10_000], 10, 2),
            decimals(&[5, 5, 5], 4, 2),
            decimals(&[8, 8, 8], 4, 2),
            strings(&["N", "N", "N"]),
            strings(&["O", "O", "O"]),
            dates(&["1995-04-01", "1994-06-01", "1995-09-15"]),
            dates(&["1995-04-02", "1994-06-02", "1995-09-16"]),
            dates(&["1995-04-03", "1994-06-03", "1995-09-17"]),
            strings(&["AIR", "MAIL", "AIR"]),
        ],
    )
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn i64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, false)
}

fn text_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}

fn date_field(name: &str) -> Field {
    Field::new(name, DataType::Date32, false)
}

fn decimal_field(name: &str, precision: u8, scale: i8) -> Field {
    Field::new(name, DataType::Decimal128(precision, scale), false)
}

fn i64s(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn dates(values: &[&str]) -> ArrayRef {
    Arc::new(Date32Array::from(
        values.iter().map(|value| date32(value)).collect::<Vec<_>>(),
    ))
}

fn decimals(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn int64(batch: &RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn decimal(batch: &RecordBatch, column: usize) -> i128 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .value(0)
}

fn string(batch: &RecordBatch, column: usize) -> &str {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

fn register(catalog: &Catalog, name: &str, value: RecordBatch) {
    catalog
        .register(TableEntry::new(name, Arc::new(MemoryTable(value))))
        .unwrap();
}

fn date32(value: &str) -> i32 {
    let parts = value
        .split('-')
        .map(|part| part.parse::<i32>().unwrap())
        .collect::<Vec<_>>();
    let (year, month, day) = (parts[0], parts[1], parts[2]);
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[derive(Clone)]
struct MemoryTable(RecordBatch);

#[async_trait]
impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics {
            row_count: Some(self.0.num_rows() as u64),
            total_byte_size: Some(self.0.get_array_memory_size() as u64),
            file_count: 1,
        }
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        context.check_cancelled()?;
        let batch = match request.projection {
            Some(projection) => self.0.project(&projection)?,
            None => self.0.clone(),
        };
        Ok(boxed_record_batch_stream(stream::once(
            async move { Ok(batch) },
        )))
    }
}
