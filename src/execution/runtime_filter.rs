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

const EXACT_KEY_LIMIT: usize = 65_536;
const NUMERIC_COMPARISON_BYTES: usize = 96;
const NUMERIC_RANGE_FILTER_BYTES: usize = 64 + 2 * NUMERIC_COMPARISON_BYTES;

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
        if context.execution.runtime_filter_bytes == 0 || hash.len() > EXACT_KEY_LIMIT {
            self.publish_none();
            return;
        }
        if matches!(self.data_type, DataType::Binary | DataType::LargeBinary) {
            let keys = hash
                .keys()
                .filter_map(|key| match key.as_slice() {
                    [CellValue::Binary(value)] => Some(value.as_slice()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            self.publish_binary_keys(keys.into_iter(), context);
            return;
        }
        let keys = hash
            .keys()
            .filter_map(|key| (key.len() == 1 && !key[0].is_null()).then_some(&key[0]))
            .collect::<Vec<_>>();
        self.publish_key_refs(&keys, context);
    }

    pub(super) fn publish_fixed_keys<I>(&self, keys: I, context: &QueryContext)
    where
        I: Clone + ExactSizeIterator<Item = CellValue>,
    {
        let budget = context.execution.runtime_filter_bytes;
        if budget == 0 {
            self.publish_none();
            return;
        }
        let key_count = keys.len();
        if key_count > EXACT_KEY_LIMIT {
            self.publish_none();
            return;
        }
        let Some((min, max)) = fixed_key_range(keys.clone()) else {
            self.publish_none();
            return;
        };

        let exact_bytes = fixed_filter_bytes(key_count);
        if key_count <= EXACT_KEY_LIMIT
            && exact_bytes <= budget
            && let Ok(memory) = context.memory.try_reserve(exact_bytes)
        {
            let Some(range) = range_filter_from_bounds(self.column, &self.data_type, &min, &max)
            else {
                self.publish_none();
                return;
            };
            let Some(exact) = exact_filter_owned(self.column, &self.data_type, keys) else {
                self.publish_none();
                return;
            };
            self.publish(Some(ScanPredicate::And(vec![range, exact])), Some(memory));
            return;
        }

        if NUMERIC_RANGE_FILTER_BYTES <= budget
            && let Ok(memory) = context.memory.try_reserve(NUMERIC_RANGE_FILTER_BYTES)
            && let Some(range) = range_filter_from_bounds(self.column, &self.data_type, &min, &max)
        {
            self.publish(Some(range), Some(memory));
        } else {
            self.publish_none();
        }
    }

    pub(super) fn publish_utf8_keys<'a, I>(&self, keys: I, context: &QueryContext)
    where
        I: Clone + ExactSizeIterator<Item = &'a str>,
    {
        let budget = context.execution.runtime_filter_bytes;
        if budget == 0 || keys.len() > EXACT_KEY_LIMIT {
            self.publish_none();
            return;
        }
        let Some(first) = keys.clone().next() else {
            self.publish_none();
            return;
        };
        let (min, max, exact_bytes) =
            keys.clone()
                .fold((first, first, 64usize), |(min, max, bytes), key| {
                    (
                        min.min(key),
                        max.max(key),
                        bytes.saturating_add(64).saturating_add(key.len()),
                    )
                });
        let range_bytes = 64usize
            .saturating_add(64 + min.len())
            .saturating_add(64 + max.len());
        let combined_bytes = 64usize
            .saturating_add(range_bytes)
            .saturating_add(exact_bytes);
        let (include_exact, bytes) = if combined_bytes <= budget {
            (true, combined_bytes)
        } else if range_bytes <= budget {
            (false, range_bytes)
        } else {
            self.publish_none();
            return;
        };
        let Ok(memory) = context.memory.try_reserve(bytes.max(1)) else {
            self.publish_none();
            return;
        };
        let range = ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: self.column,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Utf8(min.to_owned()),
            },
            ScanPredicate::Comparison {
                column: self.column,
                op: ComparisonOp::LtEq,
                value: PredicateValue::Utf8(max.to_owned()),
            },
        ]);
        let predicate = if include_exact {
            let exact = ScanPredicate::Or(
                keys.map(|key| ScanPredicate::Comparison {
                    column: self.column,
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Utf8(key.to_owned()),
                })
                .collect(),
            );
            ScanPredicate::And(vec![range, exact])
        } else {
            range
        };
        self.publish(Some(predicate), Some(memory));
    }

    pub(super) fn publish_binary_keys<'a, I>(&self, keys: I, context: &QueryContext)
    where
        I: Clone + ExactSizeIterator<Item = &'a [u8]>,
    {
        let budget = context.execution.runtime_filter_bytes;
        if budget == 0 || keys.len() > EXACT_KEY_LIMIT {
            self.publish_none();
            return;
        }
        let Some(first) = keys.clone().next() else {
            self.publish_none();
            return;
        };
        let (min, max, exact_bytes) =
            keys.clone()
                .fold((first, first, 64usize), |(min, max, bytes), key| {
                    (
                        min.min(key),
                        max.max(key),
                        bytes.saturating_add(64).saturating_add(key.len()),
                    )
                });
        let range_bytes = 64usize
            .saturating_add(64 + min.len())
            .saturating_add(64 + max.len());
        let combined_bytes = 64usize
            .saturating_add(range_bytes)
            .saturating_add(exact_bytes);
        let (include_exact, bytes) = if combined_bytes <= budget {
            (true, combined_bytes)
        } else if range_bytes <= budget {
            (false, range_bytes)
        } else {
            self.publish_none();
            return;
        };
        let Ok(memory) = context.memory.try_reserve(bytes.max(1)) else {
            self.publish_none();
            return;
        };
        let range = ScanPredicate::And(vec![
            ScanPredicate::Comparison {
                column: self.column,
                op: ComparisonOp::GtEq,
                value: PredicateValue::Binary(min.to_vec()),
            },
            ScanPredicate::Comparison {
                column: self.column,
                op: ComparisonOp::LtEq,
                value: PredicateValue::Binary(max.to_vec()),
            },
        ]);
        let predicate = if include_exact {
            let exact = ScanPredicate::Or(
                keys.map(|key| ScanPredicate::Comparison {
                    column: self.column,
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Binary(key.to_vec()),
                })
                .collect(),
            );
            ScanPredicate::And(vec![range, exact])
        } else {
            range
        };
        self.publish(Some(predicate), Some(memory));
    }

    fn publish_key_refs(&self, keys: &[&CellValue], context: &QueryContext) {
        let budget = context.execution.runtime_filter_bytes;
        if budget == 0 {
            self.publish_none();
            return;
        }
        let (key_count, exact_bytes) = keys.iter().fold((0usize, 256usize), |state, key| {
            (
                state.0.saturating_add(1),
                state.1.saturating_add(128).saturating_add(value_bytes(key)),
            )
        });
        let range = range_filter(self.column, &self.data_type, keys);
        let exact = (key_count <= 65_536 && exact_bytes <= budget)
            .then(|| exact_filter(self.column, &self.data_type, keys))
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
            exact_filter: Some(_),
            ..
        } => None,
        LogicalPlan::Scan {
            provider,
            projection,
            exact_filter: None,
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
        request.reject_unsupported_exact("runtime-filter provider")?;
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

fn exact_filter_owned(
    column: usize,
    data_type: &DataType,
    keys: impl Iterator<Item = CellValue>,
) -> Option<ScanPredicate> {
    let predicates = keys
        .map(|key| {
            Some(ScanPredicate::Comparison {
                column,
                op: ComparisonOp::Eq,
                value: predicate_value(&key, data_type)?,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(ScanPredicate::Or(predicates))
}

fn fixed_key_range(mut keys: impl Iterator<Item = CellValue>) -> Option<(CellValue, CellValue)> {
    let first = keys.next()?;
    let (mut min, mut max) = (first.clone(), first);
    for key in keys {
        if key.compare(&min).ok()?.is_lt() {
            min = key.clone();
        }
        if key.compare(&max).ok()?.is_gt() {
            max = key;
        }
    }
    Some((min, max))
}

fn fixed_filter_bytes(key_count: usize) -> usize {
    // outer AND + range + exact OR + fixed-width comparison nodes
    64usize
        .saturating_add(NUMERIC_RANGE_FILTER_BYTES)
        .saturating_add(64)
        .saturating_add(key_count.saturating_mul(NUMERIC_COMPARISON_BYTES))
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
    range_filter_from_bounds(column, data_type, min, max)
}

fn range_filter_from_bounds(
    column: usize,
    data_type: &DataType,
    min: &CellValue,
    max: &CellValue,
) -> Option<ScanPredicate> {
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
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;

    use super::{CellValue, RuntimeFilterSlot, install, projected_provider_index};
    use crate::datasource::{
        ComparisonOp, PredicateValue, ScanPredicate, ScanRequest, TableProvider, TableStatistics,
    };
    use crate::runtime::{MemoryPool, QueryContext, RecordBatchStream};
    use crate::sql::{BoundExpr, LogicalPlan, PlanSchema};
    use crate::{Error, Result};

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

    #[tokio::test]
    async fn fixed_keys_over_exact_limit_do_not_scan_or_publish_a_range() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        context.execution.runtime_filter_bytes = 8 << 20;
        let slot = RuntimeFilterSlot::new(0, DataType::Int64);
        let calls = Arc::new(AtomicUsize::new(0));

        slot.publish_fixed_keys(CountingKeys::new(65_537, Arc::clone(&calls)), &context);

        assert!(slot.wait(&context).await.unwrap().is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn generic_keys_over_exact_limit_do_not_publish_a_range() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        context.execution.runtime_filter_bytes = 8 << 20;
        let slot = RuntimeFilterSlot::new(0, DataType::Utf8);
        let hash = (0..=super::EXACT_KEY_LIMIT)
            .map(|value| {
                (
                    vec![CellValue::Utf8(format!("key-{value}"))],
                    vec![value as u32],
                )
            })
            .collect();

        slot.publish_hash(&hash, &context);

        assert!(slot.wait(&context).await.unwrap().is_none());
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn fixed_exact_filter_is_built_only_after_budget_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let exact_bytes = super::fixed_filter_bytes(2);
        context.execution.runtime_filter_bytes = exact_bytes - 1;
        let slot = RuntimeFilterSlot::new(0, DataType::Int64);

        slot.publish_fixed_keys(
            [CellValue::Int64(1), CellValue::Int64(3)].into_iter(),
            &context,
        );

        let filter = slot.wait(&context).await.unwrap().unwrap();
        assert!(is_range_only(&filter));
        assert_eq!(context.memory.used(), super::NUMERIC_RANGE_FILTER_BYTES);
    }

    #[tokio::test]
    async fn fixed_exact_filter_respects_the_key_budget_when_it_fits() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let exact_bytes = super::fixed_filter_bytes(2);
        context.execution.runtime_filter_bytes = exact_bytes;
        let slot = RuntimeFilterSlot::new(0, DataType::Int64);

        slot.publish_fixed_keys(
            [CellValue::Int64(1), CellValue::Int64(3)].into_iter(),
            &context,
        );

        let filter = slot.wait(&context).await.unwrap().unwrap();
        assert!(matches!(
            filter,
            ScanPredicate::And(ref parts)
                if matches!(
                    parts.as_slice(),
                    [ScanPredicate::And(_), ScanPredicate::Or(_)]
                )
        ));
        assert_eq!(context.memory.used(), exact_bytes);
    }

    #[tokio::test]
    async fn borrowed_utf8_keys_publish_an_accounted_exact_filter() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Utf8);
        let keys = ["north", "south"];

        slot.publish_utf8_keys(keys.into_iter(), &context);

        let filter = slot.wait(&context).await.unwrap().unwrap();
        assert!(matches!(
            filter,
            ScanPredicate::And(ref parts)
                if matches!(parts.as_slice(), [ScanPredicate::And(_), ScanPredicate::Or(_)])
        ));
        assert!(context.memory.used() > 0);
        drop(slot);
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn borrowed_binary_keys_preserve_bytes_in_range_and_exact_filters() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Binary);
        let keys = [
            b"a\0\xff".as_slice(),
            b"\0\xfe".as_slice(),
            b"\xff\0".as_slice(),
        ];

        slot.publish_binary_keys(keys.into_iter(), &context);

        let filter = slot.wait(&context).await.unwrap().unwrap();
        let ScanPredicate::And(parts) = &filter else {
            panic!("binary exact runtime filter must include its range")
        };
        let [ScanPredicate::And(range), ScanPredicate::Or(exact)] = parts.as_slice() else {
            panic!("binary exact runtime filter must contain range and exact predicates")
        };
        assert!(matches!(
            range.as_slice(),
            [
                ScanPredicate::Comparison {
                    op: ComparisonOp::GtEq,
                    value: PredicateValue::Binary(min),
                    ..
                },
                ScanPredicate::Comparison {
                    op: ComparisonOp::LtEq,
                    value: PredicateValue::Binary(max),
                    ..
                }
            ] if min == b"\0\xfe" && max == b"\xff\0"
        ));
        let exact_values = exact
            .iter()
            .map(|predicate| match predicate {
                ScanPredicate::Comparison {
                    op: ComparisonOp::Eq,
                    value: PredicateValue::Binary(value),
                    ..
                } => value.as_slice(),
                _ => panic!("binary exact filter contains a non-binary comparison"),
            })
            .collect::<Vec<_>>();
        assert_eq!(exact_values, keys);
        assert!(context.memory.used() > 0);
        drop(slot);
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn borrowed_binary_keys_fall_back_to_the_budgeted_range() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Binary);
        let keys = [b"low".as_slice(), b"high".as_slice()];
        let range_bytes = 64 + (64 + b"high".len()) + (64 + b"low".len());
        context.execution.runtime_filter_bytes = range_bytes;

        slot.publish_binary_keys(keys.into_iter(), &context);

        let filter = slot.wait(&context).await.unwrap().unwrap();
        assert!(is_range_only(&filter));
        assert_eq!(context.memory.used(), range_bytes);
    }

    #[tokio::test]
    async fn borrowed_binary_keys_publish_none_below_the_range_budget() {
        let directory = tempfile::tempdir().unwrap();
        let mut context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Binary);
        let keys = [b"low".as_slice(), b"high".as_slice()];
        let range_bytes = 64 + (64 + b"high".len()) + (64 + b"low".len());
        context.execution.runtime_filter_bytes = range_bytes - 1;

        slot.publish_binary_keys(keys.into_iter(), &context);

        assert!(slot.wait(&context).await.unwrap().is_none());
        assert_eq!(context.memory.used(), 0);
    }

    #[tokio::test]
    async fn borrowed_binary_keys_over_the_exact_limit_publish_none() {
        let directory = tempfile::tempdir().unwrap();
        let context = QueryContext::new(MemoryPool::new(1 << 20), directory.path()).unwrap();
        let slot = RuntimeFilterSlot::new(0, DataType::Binary);
        let keys = vec![b"key".as_slice(); super::EXACT_KEY_LIMIT + 1];

        slot.publish_binary_keys(keys.into_iter(), &context);

        assert!(slot.wait(&context).await.unwrap().is_none());
        assert_eq!(context.memory.used(), 0);
    }

    #[test]
    fn projected_scan_key_keeps_its_logical_provider_index() {
        assert_eq!(projected_provider_index(Some(&[2, 0]), 0), Some(0));
        assert_eq!(projected_provider_index(Some(&[2, 0]), 1), None);
        assert_eq!(projected_provider_index(Some(&[2, 0]), 2), Some(2));
        assert_eq!(projected_provider_index(None, 3), Some(3));
    }

    #[test]
    fn exact_scan_does_not_install_a_runtime_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut plan = LogicalPlan::Scan {
            table_name: "exact".into(),
            provider: Arc::new(TestTable(Arc::clone(&schema))),
            statistics: TableStatistics::default(),
            projection: Some(vec![0]),
            pushed_filter: None,
            exact_filter: Some(ScanPredicate::Comparison {
                column: 0,
                op: ComparisonOp::Gt,
                value: PredicateValue::Int64(0),
            }),
            limit: None,
            schema: PlanSchema::unqualified(schema),
        };
        let key = BoundExpr::column(0, DataType::Int64, "id");

        assert!(install(&mut plan, &key).is_none());
        let LogicalPlan::Scan { provider, .. } = plan else {
            unreachable!()
        };
        assert!(
            !provider
                .explain_scan()
                .is_some_and(|details| details.contains("runtime_filter=installed"))
        );
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

    fn is_range_only(predicate: &ScanPredicate) -> bool {
        matches!(
            predicate,
            ScanPredicate::And(parts)
                if matches!(
                    parts.as_slice(),
                    [
                        ScanPredicate::Comparison { .. },
                        ScanPredicate::Comparison { .. }
                    ]
                )
        )
    }

    struct TestTable(SchemaRef);

    #[async_trait]
    impl TableProvider for TestTable {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.0)
        }

        fn statistics(&self) -> TableStatistics {
            TableStatistics::default()
        }

        async fn scan(
            &self,
            _request: ScanRequest,
            _context: Arc<QueryContext>,
        ) -> Result<RecordBatchStream> {
            Err(Error::Internal("test provider must not be scanned".into()))
        }
    }

    #[derive(Clone)]
    struct CountingKeys {
        next: usize,
        end: usize,
        calls: Arc<AtomicUsize>,
    }

    impl CountingKeys {
        fn new(end: usize, calls: Arc<AtomicUsize>) -> Self {
            Self {
                next: 0,
                end,
                calls,
            }
        }
    }

    impl Iterator for CountingKeys {
        type Item = CellValue;

        fn next(&mut self) -> Option<Self::Item> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let value = self.next;
            self.next = self.next.checked_add(1)?;
            (value < self.end).then_some(CellValue::Int64(value as i64))
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let len = self.len();
            (len, Some(len))
        }
    }

    impl ExactSizeIterator for CountingKeys {
        fn len(&self) -> usize {
            self.end.saturating_sub(self.next)
        }
    }
}
