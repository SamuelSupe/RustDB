use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::{
    Result,
    datasource::{
        ComparisonOp, PredicateValue, ScanPredicate, ScanRequest, ScanTask, TableProvider,
        TableSourceIdentity, TableStatistics,
    },
    runtime::{MemoryReservation, QueryContext, RecordBatchStream},
    sql::{BoundExpr, ExprKind, LogicalPlan},
};

use super::value::CellValue;

pub(super) struct RuntimeFilterSlot {
    column: usize,
    data_type: DataType,
    state: Mutex<Option<PublishedFilter>>,
    ready: watch::Sender<bool>,
}

struct PublishedFilter {
    predicate: Option<ScanPredicate>,
    _memory: Option<MemoryReservation>,
}

impl RuntimeFilterSlot {
    fn new(column: usize, data_type: DataType) -> Arc<Self> {
        let (ready, _) = watch::channel(false);
        Arc::new(Self {
            column,
            data_type,
            state: Mutex::new(None),
            ready,
        })
    }

    pub(super) fn publish_none(&self) {
        self.publish(None, None);
    }

    pub(super) fn publish_hash(
        &self,
        hash: &HashMap<Vec<CellValue>, Vec<u32>>,
        context: &QueryContext,
    ) {
        let budget = context.execution.runtime_filter_bytes;
        if budget == 0 {
            self.publish_none();
            return;
        }
        let (key_count, exact_bytes) = hash.keys().fold((0usize, 256usize), |state, key| {
            if key.len() != 1 || key[0].is_null() {
                return state;
            }
            (
                state.0.saturating_add(1),
                state
                    .1
                    .saturating_add(128)
                    .saturating_add(value_bytes(&key[0])),
            )
        });
        let keys = hash
            .keys()
            .filter_map(|key| (key.len() == 1 && !key[0].is_null()).then_some(&key[0]))
            .collect::<Vec<_>>();
        let range = range_filter(self.column, &self.data_type, &keys);
        let exact = (key_count <= 65_536 && exact_bytes <= budget)
            .then(|| exact_filter(self.column, &self.data_type, &keys))
            .flatten();
        let filter = match (range.clone(), exact) {
            (Some(range), Some(exact)) => ScanPredicate::And(vec![range, exact]),
            (None, Some(exact)) => exact,
            (Some(range), None) => range,
            (None, None) => {
                self.publish_none();
                return;
            }
        };
        drop(keys);
        if let Some(memory) = reserve_filter(&filter, budget, context) {
            self.publish(Some(filter), Some(memory));
        } else if let Some(range) = range
            && let Some(memory) = reserve_filter(&range, budget, context)
        {
            self.publish(Some(range), Some(memory));
        } else {
            self.publish_none();
        }
    }

    fn publish(&self, predicate: Option<ScanPredicate>, memory: Option<MemoryReservation>) {
        let mut state = self.state.lock();
        if state.is_some() {
            return;
        }
        *state = Some(PublishedFilter {
            predicate,
            _memory: memory,
        });
        drop(state);
        self.ready.send_replace(true);
    }

    async fn wait(&self, context: &QueryContext) -> Result<Option<ScanPredicate>> {
        let mut ready = self.ready.subscribe();
        loop {
            if *ready.borrow() {
                return Ok(self
                    .state
                    .lock()
                    .as_ref()
                    .and_then(|published| published.predicate.clone()));
            }
            tokio::select! {
                _ = context.control.cancelled() => context.check_cancelled()?,
                changed = ready.changed() => {
                    if changed.is_err() {
                        return Ok(None);
                    }
                }
            }
        }
    }
}

pub(super) fn install(plan: &mut LogicalPlan, key: &BoundExpr) -> Option<Arc<RuntimeFilterSlot>> {
    let ExprKind::Column(index) = key.kind else {
        return None;
    };
    install_at(plan, index)
}

fn install_at(plan: &mut LogicalPlan, index: usize) -> Option<Arc<RuntimeFilterSlot>> {
    match plan {
        LogicalPlan::Scan {
            provider,
            projection,
            ..
        } => {
            let provider_index = projected_provider_index(projection.as_deref(), index)?;
            let field = provider.schema().fields().get(provider_index)?.clone();
            let slot = RuntimeFilterSlot::new(provider_index, field.data_type().clone());
            *provider = Arc::new(RuntimeFilteredProvider {
                inner: Arc::clone(provider),
                slot: Arc::clone(&slot),
            });
            Some(slot)
        }
        LogicalPlan::Filter { input, .. } => install_at(input, index),
        LogicalPlan::Projection {
            input, expressions, ..
        } => {
            let ExprKind::Column(input_index) = expressions.get(index)?.kind else {
                return None;
            };
            install_at(input, input_index)
        }
        _ => None,
    }
}

fn projected_provider_index(projection: Option<&[usize]>, index: usize) -> Option<usize> {
    // A Scan keeps the provider's logical schema width even when projection
    // pruning reads only a compact physical subset; scan expansion restores
    // each decoded column to its original provider position. The join key is
    // therefore already a provider index, not an index into `projection`.
    projection.map_or(Some(index), |columns| {
        columns.contains(&index).then_some(index)
    })
}

struct RuntimeFilteredProvider {
    inner: Arc<dyn TableProvider>,
    slot: Arc<RuntimeFilterSlot>,
}

impl RuntimeFilteredProvider {
    async fn merge_request(
        &self,
        mut request: ScanRequest,
        context: &QueryContext,
    ) -> Result<ScanRequest> {
        let Some(filter) = self.slot.wait(context).await? else {
            return Ok(request);
        };
        request.predicate = Some(match request.predicate.take() {
            Some(predicate) => ScanPredicate::And(vec![predicate, filter]),
            None => filter,
        });
        Ok(request)
    }
}

#[async_trait]
impl TableProvider for RuntimeFilteredProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn statistics(&self) -> TableStatistics {
        self.inner.statistics()
    }

    fn source_identity(&self) -> Option<TableSourceIdentity> {
        self.inner.source_identity()
    }

    fn explain_scan(&self) -> Option<String> {
        self.inner
            .explain_scan()
            .map(|details| format!("{details} runtime_filter=installed"))
    }

    fn query_statistics(&self, context: &QueryContext) -> TableStatistics {
        self.inner.query_statistics(context)
    }

    async fn prepare(&self, context: Arc<QueryContext>) -> Result<()> {
        self.inner.prepare(context).await
    }

    async fn refreshed(&self) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.refreshed().await
    }

    async fn scan(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        let request = self.merge_request(request, &context).await?;
        let before = pruning_events(&context);
        let stream = self.inner.scan(request, Arc::clone(&context)).await?;
        record_pruning_hit(&context, before);
        Ok(stream)
    }

    async fn scan_tasks(
        &self,
        request: ScanRequest,
        context: Arc<QueryContext>,
        target_tasks: usize,
    ) -> Result<Vec<ScanTask>> {
        let request = self.merge_request(request, &context).await?;
        let before = pruning_events(&context);
        let tasks = self
            .inner
            .scan_tasks(request, Arc::clone(&context), target_tasks)
            .await?;
        record_pruning_hit(&context, before);
        Ok(tasks)
    }
}

fn pruning_events(context: &QueryContext) -> u64 {
    let metrics = context.metrics.snapshot();
    metrics
        .files_pruned
        .saturating_add(metrics.row_groups_pruned)
        .saturating_add(metrics.parquet_pages_pruned)
        .saturating_add(metrics.parquet_bloom_row_groups_pruned)
}

fn record_pruning_hit(context: &QueryContext, before: u64) {
    if pruning_events(context) > before {
        context.metrics.record_runtime_filter();
    }
}

fn exact_filter(column: usize, data_type: &DataType, keys: &[&CellValue]) -> Option<ScanPredicate> {
    let predicates = keys
        .iter()
        .map(|key| {
            Some(ScanPredicate::Comparison {
                column,
                op: ComparisonOp::Eq,
                value: predicate_value(key, data_type)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ScanPredicate::Or(predicates))
}

fn range_filter(column: usize, data_type: &DataType, keys: &[&CellValue]) -> Option<ScanPredicate> {
    let first = *keys.first()?;
    let (mut min, mut max) = (first, first);
    for key in keys.iter().copied().skip(1) {
        if key.compare(min).ok()?.is_lt() {
            min = key;
        }
        if key.compare(max).ok()?.is_gt() {
            max = key;
        }
    }
    Some(ScanPredicate::And(vec![
        ScanPredicate::Comparison {
            column,
            op: ComparisonOp::GtEq,
            value: predicate_value(min, data_type)?,
        },
        ScanPredicate::Comparison {
            column,
            op: ComparisonOp::LtEq,
            value: predicate_value(max, data_type)?,
        },
    ]))
}

fn predicate_value(value: &CellValue, data_type: &DataType) -> Option<PredicateValue> {
    match (value, data_type) {
        (CellValue::Boolean(value), DataType::Boolean) => Some(PredicateValue::Boolean(*value)),
        (CellValue::Int64(value), DataType::Date32) => {
            i32::try_from(*value).ok().map(PredicateValue::Date32)
        }
        (CellValue::Int64(value), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
            Some(PredicateValue::TimestampMicros(*value))
        }
        (CellValue::Int64(value), _) => Some(PredicateValue::Int64(*value)),
        (CellValue::UInt64(value), _) => Some(PredicateValue::UInt64(*value)),
        (CellValue::Float64(value), _) if value.is_finite() => {
            Some(PredicateValue::Float64(*value))
        }
        (CellValue::Utf8(value), _) => Some(PredicateValue::Utf8(value.clone())),
        (CellValue::Binary(value), _) => Some(PredicateValue::Binary(value.clone())),
        (CellValue::Decimal128(value), DataType::Decimal128(precision, scale)) => {
            Some(PredicateValue::Decimal128 {
                value: *value,
                precision: *precision,
                scale: *scale,
            })
        }
        _ => None,
    }
}

fn value_bytes(value: &CellValue) -> usize {
    match value {
        CellValue::Utf8(value) => value.len(),
        CellValue::Binary(value) => value.len(),
        _ => 32,
    }
}

fn filter_memory_bytes(predicate: &ScanPredicate) -> usize {
    match predicate {
        ScanPredicate::Comparison { value, .. } => {
            64 + match value {
                PredicateValue::Utf8(value) => value.len(),
                PredicateValue::Binary(value) => value.len(),
                _ => 32,
            }
        }
        ScanPredicate::And(predicates) | ScanPredicate::Or(predicates) => {
            predicates.iter().fold(64, |bytes, predicate| {
                bytes.saturating_add(filter_memory_bytes(predicate))
            })
        }
        ScanPredicate::IsNull { .. } | ScanPredicate::IsNotNull { .. } => 32,
    }
}

fn reserve_filter(
    predicate: &ScanPredicate,
    budget: usize,
    context: &QueryContext,
) -> Option<MemoryReservation> {
    let bytes = filter_memory_bytes(predicate).max(1);
    (bytes <= budget)
        .then(|| context.memory.try_reserve(bytes).ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use super::{CellValue, RuntimeFilterSlot, projected_provider_index};
    use crate::datasource::ScanPredicate;
    use crate::runtime::{MemoryPool, QueryContext};

    #[tokio::test]
    async fn low_memory_publish_falls_back_to_accounted_range_filter() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        context.execution.runtime_filter_bytes = 8 << 20;
        let slot = RuntimeFilterSlot::new(0, DataType::Utf8);
        let hash = (0..20_000_u32)
            .map(|index| {
                (
                    vec![CellValue::Utf8(format!("runtime-filter-{index:08}"))],
                    vec![index],
                )
            })
            .collect::<HashMap<_, _>>();
        slot.publish_hash(&hash, &context);
        let filter = slot.wait(&context).await.unwrap().unwrap();
        assert!(matches!(filter, ScanPredicate::And(_)));
        assert!(context.memory.used() > 0);
        drop(slot);
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn exact_filter_also_keeps_a_min_max_range() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Int64);
        let hash = [1_i64, 3]
            .into_iter()
            .enumerate()
            .map(|(row, value)| (vec![CellValue::Int64(value)], vec![row as u32]))
            .collect::<HashMap<_, _>>();

        slot.publish_hash(&hash, &context);
        let filter = slot.wait(&context).await.unwrap().unwrap();
        let ScanPredicate::And(parts) = filter else {
            panic!("exact runtime filter must include its range")
        };
        assert!(matches!(
            parts.as_slice(),
            [ScanPredicate::And(_), ScanPredicate::Or(_)]
        ));
    }

    #[test]
    fn projected_scan_key_keeps_its_logical_provider_index() {
        assert_eq!(projected_provider_index(Some(&[2, 0]), 0), Some(0));
        assert_eq!(projected_provider_index(Some(&[2, 0]), 1), None);
        assert_eq!(projected_provider_index(Some(&[2, 0]), 2), Some(2));
        assert_eq!(projected_provider_index(None, 3), Some(3));
    }

    #[tokio::test]
    async fn waiting_scan_observes_query_cancellation() {
        let directory = tempfile::tempdir().unwrap();
        let context = std::sync::Arc::new(
            QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap(),
        );
        let slot = RuntimeFilterSlot::new(0, DataType::Int64);
        let waiter_context = std::sync::Arc::clone(&context);
        let waiter_slot = std::sync::Arc::clone(&slot);
        let waiter = tokio::spawn(async move { waiter_slot.wait(&waiter_context).await });
        tokio::task::yield_now().await;
        context.cancel();
        assert!(matches!(
            waiter.await.unwrap(),
            Err(crate::Error::Cancelled)
        ));
    }
}
