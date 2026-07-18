use std::sync::Arc;

use arrow::{
    array::{Array, BooleanArray, StringArray, TimestampMillisecondArray, TimestampSecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
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

const ZONE: &str = "America/New_York";

#[tokio::test]
async fn same_timezone_operations_preserve_zone_and_choose_finer_units() {
    let catalog = timezone_catalog();
    let plan = plan_sql(
        &catalog,
        "SELECT seconds = millis, \
                CASE WHEN true THEN seconds ELSE millis END, \
                coalesce(seconds, millis), \
                seconds + INTERVAL '1' DAY, \
                seconds - INTERVAL '1' DAY, \
                seconds + INTERVAL '1' MONTH, \
                INTERVAL '1' MONTH + seconds \
         FROM timezone_values",
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let batches = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = &batches[0];

    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let zoned_millis = DataType::Timestamp(TimeUnit::Millisecond, Some(ZONE.into()));
    assert_eq!(batch.column(1).data_type(), &zoned_millis);
    assert_eq!(batch.column(2).data_type(), &zoned_millis);
    let zoned_seconds = DataType::Timestamp(TimeUnit::Second, Some(ZONE.into()));
    assert_eq!(batch.column(3).data_type(), &zoned_seconds);
    assert_eq!(batch.column(4).data_type(), &zoned_seconds);
    assert_eq!(batch.column(5).data_type(), &zoned_seconds);
    assert_eq!(batch.column(6).data_type(), &zoned_seconds);
    let value = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(0)
    };
    // 2024-03-09 11:00 in New York crosses the spring DST boundary:
    // adding one calendar day advances the UTC epoch by 23 hours, not 24.
    assert_eq!(value(3), 1_710_082_800);
    assert_eq!(value(4), 1_709_913_600);
    assert_eq!(value(5), 1_712_674_800);
    assert_eq!(value(6), 1_712_674_800);
}

#[tokio::test]
async fn zoned_comparison_literals_and_at_time_zone_use_iana_rules() {
    let catalog = timezone_catalog();
    let batches = run(
        &catalog,
        "SELECT seconds = utc_millis, \
         CAST(CASE WHEN true THEN seconds ELSE utc_millis END AS VARCHAR), \
         CAST(TIMESTAMP '2024-03-10 01:30:00' AT TIME ZONE 'America/New_York' AS VARCHAR), \
         CAST((TIMESTAMP '2024-03-10 01:30:00' AT TIME ZONE 'America/New_York') AT TIME ZONE 'UTC' AS VARCHAR), \
         CAST(TIMESTAMPTZ '2024-03-10 01:30:00-05:00' AS VARCHAR), \
         CAST(TIMESTAMPTZ '2024-03-10 01:30:00-05:00' AT TIME ZONE 'America/New_York' AS VARCHAR), \
         CAST(TIMESTAMP '2024-11-03 01:30:00' AT TIME ZONE 'America/New_York' AS VARCHAR) \
         FROM timezone_values",
    )
    .await;
    let batch = &batches[0];
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let string = |column| {
        batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned()
    };
    assert!(string(1).ends_with("+00:00"));
    assert_eq!(string(2), "2024-03-10 01:30:00-05:00");
    assert_eq!(string(3), "2024-03-10 06:30:00");
    assert_eq!(string(4), "2024-03-10 06:30:00+00:00");
    assert_eq!(string(5), "2024-03-10 01:30:00");
    assert_eq!(string(6), "2024-11-03 01:30:00-05:00");

    let plan = plan_sql(
        &Catalog::default(),
        "SELECT TIMESTAMP '2024-03-10 02:30:00' AT TIME ZONE 'America/New_York'",
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
    assert!(error.contains("nonexistent local time"), "{error}");
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

fn timezone_catalog() -> Catalog {
    let seconds = TimestampSecondArray::from(vec![Some(1_710_000_000)]).with_timezone(ZONE);
    let millis = TimestampMillisecondArray::from(vec![Some(1_710_000_000_000)]).with_timezone(ZONE);
    let utc = TimestampMillisecondArray::from(vec![Some(1_710_000_000_000)]).with_timezone("UTC");
    let schema = Arc::new(Schema::new(vec![
        Field::new("seconds", seconds.data_type().clone(), true),
        Field::new("millis", millis.data_type().clone(), true),
        Field::new("utc_millis", utc.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(seconds), Arc::new(millis), Arc::new(utc)],
    )
    .unwrap();
    let catalog = Catalog::default();
    catalog
        .register(TableEntry::new(
            "timezone_values",
            Arc::new(MemoryTable { batch }),
        ))
        .unwrap();
    catalog
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
        let batch = match request.projection {
            Some(projection) => self.batch.project(&projection)?,
            None => self.batch.clone(),
        };
        Ok(boxed_record_batch_stream(stream::once(
            async move { Ok(batch) },
        )))
    }
}
