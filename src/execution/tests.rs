use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};

use crate::datasource::{ScanRequest, TableProvider, TableStatistics};
use crate::runtime::{
    MemoryPool, QueryContext, QueryMetricsSnapshot, RecordBatchStream, boxed_record_batch_stream,
};
use crate::storage::ObjectSnapshot;
use crate::{Catalog, Error, Result, TableEntry};

use super::execute;

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

#[derive(Clone)]
struct SnapshotTable {
    batch: RecordBatch,
    uri: &'static str,
    version: Arc<AtomicUsize>,
    prepared: Arc<AtomicUsize>,
    mutate_on_read: Option<Arc<AtomicUsize>>,
}

#[async_trait]
impl TableProvider for SnapshotTable {
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

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        let version = self.version.load(Ordering::Acquire);
        context.register_object_snapshot(
            self.uri,
            ObjectSnapshot {
                size: 1,
                e_tag: Some(format!("v{version}")),
                version: None,
            },
        )?;
        self.prepared.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        if self.prepared.load(Ordering::Acquire) != 2 {
            return Err(Error::Internal(
                "a scan started before every provider was prepared".to_owned(),
            ));
        }
        let snapshot = context.object_snapshot(self.uri)?;
        let current = self.version.load(Ordering::Acquire);
        let expected = format!("v{current}");
        if snapshot.e_tag.as_deref() != Some(expected.as_str()) {
            return Err(Error::Execution(format!(
                "object changed during query: {}",
                self.uri
            )));
        }
        let batch = match request.projection {
            Some(projection) => self.batch.project(&projection)?,
            None => self.batch.clone(),
        };
        let mutate_on_read = self.mutate_on_read.clone();
        Ok(boxed_record_batch_stream(stream::once(async move {
            if let Some(version) = mutate_on_read {
                version.store(2, Ordering::Release);
            }
            Ok(batch)
        })))
    }
}

#[tokio::test]
async fn executes_filter_projection_and_limit() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "t",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("x", DataType::Int64, false),
                Field::new("label", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap(),
    );
    let batches = run(
        &catalog,
        "SELECT x + 10 AS y FROM t WHERE x >= 2 LIMIT 2",
        1 << 20,
    )
    .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![12, 13])
    );
}

#[tokio::test]
async fn executes_grouped_aggregates() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "events",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(Int64Array::from(vec![Some(1), Some(10), Some(3), None])),
            ],
        )
        .unwrap(),
    );
    let batches = run(
        &catalog,
        "SELECT g, count(*) AS n, sum(v) AS total, min(v) AS lo, max(v) AS hi, avg(v) AS mean FROM events GROUP BY g",
        1 << 20,
    )
    .await;
    let batch = &batches[0];
    let groups = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let counts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let averages = batch
        .column(5)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(groups.value(0), "a");
    assert_eq!(
        (counts.value(0), sums.value(0), averages.value(0)),
        (2, 4, 2.0)
    );
    assert_eq!(groups.value(1), "b");
    assert_eq!(
        (counts.value(1), sums.value(1), averages.value(1)),
        (2, 10, 10.0)
    );
}

#[tokio::test]
async fn executes_left_equi_join() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "l",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap(),
    );
    register(
        &catalog,
        "r",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap(),
    );
    let batches = run(
        &catalog,
        "SELECT l.id, r.label FROM l LEFT JOIN r ON l.id = r.id",
        1 << 20,
    )
    .await;
    let batch = &batches[0];
    let labels = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(labels.value(0), "one");
    assert_eq!(labels.value(1), "two");
    assert!(labels.is_null(2));
}

#[tokio::test]
async fn fixes_all_join_snapshots_before_build_side_is_consumed() {
    let catalog = Catalog::default();
    let prepared = Arc::new(AtomicUsize::new(0));
    let probe_version = Arc::new(AtomicUsize::new(1));
    let build_version = Arc::new(AtomicUsize::new(1));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let probe = SnapshotTable {
        batch: RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .unwrap(),
        uri: "s3://bucket/probe.parquet",
        version: Arc::clone(&probe_version),
        prepared: Arc::clone(&prepared),
        mutate_on_read: None,
    };
    let build = SnapshotTable {
        batch: RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap(),
        uri: "s3://bucket/build.parquet",
        version: build_version,
        prepared: Arc::clone(&prepared),
        mutate_on_read: Some(probe_version),
    };
    catalog
        .register(TableEntry::new("probe", Arc::new(probe)))
        .unwrap();
    catalog
        .register(TableEntry::new("build", Arc::new(build)))
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(1 << 20), temp.path()).unwrap();
    let plan = crate::sql::plan_sql(
        &catalog,
        "SELECT probe.id FROM probe JOIN build ON probe.id = build.id",
    )
    .unwrap();
    let error = execute(plan, context)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err()
        .to_string();

    assert_eq!(prepared.load(Ordering::Acquire), 2);
    assert!(error.contains("object changed during query"), "{error}");
}

#[tokio::test]
async fn aggregate_and_join_spill_under_small_memory_limit() {
    let catalog = Catalog::default();
    const ROWS: usize = 16_384;
    const MEMORY_LIMIT: usize = 2 << 20;
    let values: Vec<i64> = (0..ROWS as i64).collect();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(values.clone()))],
    )
    .unwrap();
    register(&catalog, "a", batch.clone());
    register(&catalog, "b", batch);

    let (grouped, grouped_metrics) = run_with_metrics(
        &catalog,
        "SELECT id, count(*) FROM a GROUP BY id",
        MEMORY_LIMIT,
    )
    .await;
    assert_eq!(
        grouped.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS
    );
    assert!(grouped_metrics.spill_partitions > 0);

    let (joined, joined_metrics) = run_with_metrics(
        &catalog,
        "SELECT a.id FROM a JOIN b ON a.id = b.id",
        MEMORY_LIMIT,
    )
    .await;
    assert_eq!(
        joined.iter().map(RecordBatch::num_rows).sum::<usize>(),
        ROWS
    );
    assert!(joined_metrics.spill_partitions > 0);
}

#[tokio::test]
async fn optimizer_prunes_operator_columns_and_uses_the_smaller_inner_build() {
    let catalog = Catalog::default();
    register(
        &catalog,
        "small",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
                Field::new("unused", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["small-unused"])),
            ],
        )
        .unwrap(),
    );
    register(
        &catalog,
        "large",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
                Field::new("unused", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap(),
    );

    let sql = "SELECT small.value AS small_value, large.value AS large_value FROM small \
               JOIN large ON small.id = large.id ORDER BY large_value";
    let plan = crate::sql::plan_sql(&catalog, sql).unwrap();
    let explain = format!("{:?}", plan);
    assert!(explain.contains("InnerJoin keys=1 build=right"));
    assert!(explain.find("Scan table=large").unwrap() < explain.find("Scan table=small").unwrap());
    assert!(explain.contains("Scan table=small projection=Some([0, 1])"));
    assert!(explain.contains("Scan table=large projection=Some([0, 1])"));

    let batches = run(&catalog, sql, 1 << 20).await;
    assert_eq!(int64_at(&batches[0], 0), 10);
    assert_eq!(int64_at(&batches[0], 1), 100);

    let left = crate::sql::plan_sql(
        &catalog,
        "SELECT small.value FROM small LEFT JOIN large ON small.id = large.id",
    )
    .unwrap();
    let explain = format!("{:?}", left);
    assert!(explain.find("Scan table=small").unwrap() < explain.find("Scan table=large").unwrap());

    let aggregate = crate::sql::plan_sql(
        &catalog,
        "SELECT id, sum(value) FROM large GROUP BY id ORDER BY id",
    )
    .unwrap();
    assert!(format!("{:?}", aggregate).contains("Scan table=large projection=Some([0, 1])"));
}

fn int64_at(batch: &RecordBatch, column: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn register(catalog: &Catalog, name: &str, batch: RecordBatch) {
    catalog
        .register(TableEntry::new(name, Arc::new(MemoryTable { batch })))
        .unwrap();
}

async fn run(catalog: &Catalog, sql: &str, memory_limit: usize) -> Vec<RecordBatch> {
    run_with_metrics(catalog, sql, memory_limit).await.0
}

async fn run_with_metrics(
    catalog: &Catalog,
    sql: &str,
    memory_limit: usize,
) -> (Vec<RecordBatch>, QueryMetricsSnapshot) {
    let temp = tempfile::tempdir().unwrap();
    let context = QueryContext::shared(MemoryPool::new(memory_limit), temp.path()).unwrap();
    let plan = crate::sql::plan_sql(catalog, sql).unwrap();
    let batches = execute(plan, Arc::clone(&context))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    (batches, context.metrics.snapshot())
}
