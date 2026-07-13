use arrow::datatypes::{DataType, Schema, TimeUnit};
use parquet::{
    arrow::ParquetRecordBatchStreamBuilder, bloom_filter::Sbbf, file::metadata::ParquetMetaData,
};

use super::{
    ComparisonOp, MetadataCache, PredicateValue, ScanPredicate,
    metadata_cache::BloomLoad,
    parquet_metadata::ParquetMetadata,
    parquet_pruning_budget::{MAX_FILE_PAGE_INDEX_BYTES, PruningBudget},
    parquet_reader::{QueryIo, SnapshotParquetReader, into_query_error},
};
use crate::{Error, Result, runtime::QueryContext, storage::ObjectSource};

pub(super) fn supports_bloom(predicate: Option<&ScanPredicate>) -> bool {
    predicate.is_some_and(|predicate| !equality_groups(predicate).is_empty())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn bloom_prunes_row_group(
    file: &ObjectSource,
    metadata: &ParquetMetadata,
    file_schema: &Schema,
    table_schema: &Schema,
    row_group: usize,
    predicate: Option<&ScanPredicate>,
    context: &QueryContext,
    cache: &MetadataCache,
    budget: &PruningBudget,
) -> Result<bool> {
    let Some(predicate) = predicate else {
        return Ok(false);
    };
    for group in equality_groups(predicate) {
        let mut all_absent = true;
        for predicate in group {
            match equality_absent(
                file,
                metadata,
                file_schema,
                table_schema,
                row_group,
                predicate,
                context,
                cache,
                budget,
            )
            .await?
            {
                Some(true) => {}
                Some(false) | None => {
                    all_absent = false;
                    break;
                }
            }
        }
        if all_absent {
            return Ok(true);
        }
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
async fn equality_absent(
    file: &ObjectSource,
    metadata: &ParquetMetadata,
    file_schema: &Schema,
    table_schema: &Schema,
    row_group: usize,
    predicate: &ScanPredicate,
    context: &QueryContext,
    cache: &MetadataCache,
    budget: &PruningBudget,
) -> Result<Option<bool>> {
    let ScanPredicate::Comparison { column, value, .. } = predicate else {
        return Ok(None);
    };
    let parquet_metadata = metadata.reader_metadata().metadata();
    let Some((file_column, leaf)) =
        column_leaf(parquet_metadata, file_schema, table_schema, *column)
    else {
        return Ok(None);
    };
    let Some(value) = BloomValue::new(file_schema.field(file_column).data_type(), value) else {
        return Ok(None);
    };
    if matches!(value, BloomValue::DefinitelyAbsent) {
        return Ok(Some(true));
    }

    let column_metadata = parquet_metadata.row_group(row_group).column(leaf);
    let column_name = file_schema.field(file_column).name();
    let Some((start, length)) = bloom_location(
        file.uri(),
        row_group,
        column_name,
        column_metadata.bloom_filter_offset(),
        column_metadata.bloom_filter_length(),
    )?
    else {
        return Ok(None);
    };
    let Some(length) = length else {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(None);
    };
    if length > MAX_FILE_PAGE_INDEX_BYTES {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(None);
    }
    let snapshot = context.object_snapshot(file.uri())?;
    let end = start
        .checked_add(u64::try_from(length).unwrap_or(u64::MAX))
        .ok_or_else(|| {
            bloom_error(
                file.uri(),
                row_group,
                file_schema.field(file_column).name(),
                "filter range overflows u64",
            )
        })?;
    if end > snapshot.size {
        return Err(bloom_error(
            file.uri(),
            row_group,
            file_schema.field(file_column).name(),
            "filter range exceeds object size",
        ));
    }
    let Some(_budget_lease) = budget.try_reserve(length) else {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(None);
    };
    let Ok(_memory) = context.memory.try_reserve(length) else {
        context.metrics.add_parquet_pruning_budget_skip();
        return Ok(None);
    };

    let _load_guard = match cache
        .acquire_bloom(
            file,
            &snapshot,
            row_group,
            leaf,
            start,
            length,
            Some(&context.control),
        )
        .await?
    {
        ready @ (BloomLoad::Cached { .. } | BloomLoad::Shared { .. }) => {
            let (filter, wait, cache_hit) = match ready {
                BloomLoad::Cached { filter, wait } => (filter, wait, true),
                BloomLoad::Shared { filter, wait } => (filter, wait, false),
                BloomLoad::Leader { .. } => unreachable!(),
            };
            if cache_hit {
                context.metrics.record_metadata_cache_hit();
            } else {
                context.metrics.record_metadata_cache_miss();
            }
            if !wait.is_zero() {
                context.metrics.record_metadata_singleflight_wait(wait);
            }
            return Ok(filter.map(|filter| !value.might_be_present(&filter)));
        }
        BloomLoad::Leader { guard, wait } => {
            context.metrics.record_metadata_cache_miss();
            if !wait.is_zero() {
                context.metrics.record_metadata_singleflight_wait(wait);
            }
            guard
        }
    };

    let query = QueryIo::for_bloom_filter(context.control.clone(), context.metrics.clone());
    let reader = SnapshotParquetReader::new(file, snapshot.clone(), Some(query));
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        reader,
        metadata.reader_metadata().clone(),
    );
    let loaded = builder
        .get_row_group_column_bloom_filter(row_group, leaf)
        .await
        .map_err(|error| {
            contextual_bloom_error(
                file.uri(),
                row_group,
                file_schema.field(file_column).name(),
                error,
            )
        });
    let filter = match loaded {
        Ok(filter) => filter,
        Err(error) => return Err(_load_guard.fail(error)),
    };
    let Some(filter) = filter else {
        _load_guard.succeed(None);
        return Ok(None);
    };
    let filter = std::sync::Arc::new(filter);
    cache.insert_bloom(
        file,
        &snapshot,
        row_group,
        leaf,
        start,
        length,
        std::sync::Arc::clone(&filter),
    );
    _load_guard.succeed(Some(std::sync::Arc::clone(&filter)));
    Ok(Some(!value.might_be_present(&filter)))
}

fn contextual_bloom_error(
    uri: &str,
    row_group: usize,
    column: &str,
    error: parquet::errors::ParquetError,
) -> Error {
    match into_query_error(error) {
        error @ (Error::Cancelled | Error::ResourceExhausted(_)) => error,
        error => bloom_error(uri, row_group, column, &error.to_string()),
    }
}

fn bloom_location(
    uri: &str,
    row_group: usize,
    column: &str,
    offset: Option<i64>,
    length: Option<i32>,
) -> Result<Option<(u64, Option<usize>)>> {
    let Some(offset) = offset else {
        return match length {
            None => Ok(None),
            Some(length) => Err(bloom_error(
                uri,
                row_group,
                column,
                &format!("filter length {length} is present without an offset"),
            )),
        };
    };
    let offset = u64::try_from(offset).map_err(|_| {
        bloom_error(
            uri,
            row_group,
            column,
            "filter offset is negative or does not fit u64",
        )
    })?;
    let length = length
        .map(|length| {
            if length <= 0 {
                return Err(bloom_error(
                    uri,
                    row_group,
                    column,
                    &format!("filter length {length} is non-positive"),
                ));
            }
            usize::try_from(length).map_err(|_| {
                bloom_error(
                    uri,
                    row_group,
                    column,
                    "filter length is non-positive or does not fit usize",
                )
            })
        })
        .transpose()?;
    Ok(Some((offset, length)))
}

fn equality_groups(predicate: &ScanPredicate) -> Vec<Vec<&ScanPredicate>> {
    let mut output = Vec::new();
    collect_equality_groups(predicate, &mut output);
    output
}

fn collect_equality_groups<'a>(
    predicate: &'a ScanPredicate,
    output: &mut Vec<Vec<&'a ScanPredicate>>,
) {
    match predicate {
        ScanPredicate::And(predicates) => {
            for predicate in predicates {
                collect_equality_groups(predicate, output);
            }
        }
        ScanPredicate::Or(predicates) => {
            if let Some(group) = same_column_equalities(predicates) {
                output.push(group);
            }
        }
        ScanPredicate::Comparison {
            op: ComparisonOp::Eq,
            ..
        } => output.push(vec![predicate]),
        _ => {}
    }
}

fn same_column_equalities(predicates: &[ScanPredicate]) -> Option<Vec<&ScanPredicate>> {
    let mut output = Vec::with_capacity(predicates.len());
    let mut expected = None;
    for predicate in predicates {
        let ScanPredicate::Comparison {
            column,
            op: ComparisonOp::Eq,
            ..
        } = predicate
        else {
            return None;
        };
        if expected.is_some_and(|expected| expected != *column) {
            return None;
        }
        expected = Some(*column);
        output.push(predicate);
    }
    (!output.is_empty()).then_some(output)
}

fn column_leaf(
    metadata: &ParquetMetaData,
    file_schema: &Schema,
    table_schema: &Schema,
    table_column: usize,
) -> Option<(usize, usize)> {
    let field = table_schema.fields().get(table_column)?;
    let file_column = file_schema.index_of(field.name()).ok()?;
    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut leaves = (0..parquet_schema.num_columns())
        .filter(|leaf| parquet_schema.get_column_root_idx(*leaf) == file_column);
    let leaf = leaves.next()?;
    if leaves.next().is_some() {
        return None;
    }
    Some((file_column, leaf))
}

enum BloomValue {
    I32(i32),
    I64(i64),
    Bytes(Vec<u8>),
    DefinitelyAbsent,
}

impl BloomValue {
    fn new(data_type: &DataType, value: &PredicateValue) -> Option<Self> {
        match (data_type, value) {
            (DataType::Int8 | DataType::Int16 | DataType::Int32, PredicateValue::Int64(value)) => {
                Some(i32::try_from(*value).map_or(Self::DefinitelyAbsent, Self::I32))
            }
            (DataType::Int64, PredicateValue::Int64(value)) => Some(Self::I64(*value)),
            (
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32,
                PredicateValue::UInt64(value),
            ) => Some(
                u32::try_from(*value)
                    .map(|value| Self::I32(value as i32))
                    .unwrap_or(Self::DefinitelyAbsent),
            ),
            (DataType::UInt64, PredicateValue::UInt64(value)) => Some(Self::I64(*value as i64)),
            (DataType::Date32, PredicateValue::Date32(value)) => Some(Self::I32(*value)),
            (DataType::Timestamp(unit, _), PredicateValue::TimestampMicros(value)) => Some(
                timestamp_physical(*value, *unit)
                    .map(Self::I64)
                    .unwrap_or(Self::DefinitelyAbsent),
            ),
            (DataType::Utf8 | DataType::LargeUtf8, PredicateValue::Utf8(value)) => {
                Some(Self::Bytes(value.as_bytes().to_vec()))
            }
            (
                DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_),
                PredicateValue::Binary(value),
            ) => Some(Self::Bytes(value.clone())),
            _ => None,
        }
    }

    fn might_be_present(&self, filter: &Sbbf) -> bool {
        match self {
            Self::I32(value) => filter.check(value),
            Self::I64(value) => filter.check(value),
            Self::Bytes(value) => filter.check(value.as_slice()),
            Self::DefinitelyAbsent => false,
        }
    }
}

fn timestamp_physical(micros: i64, unit: TimeUnit) -> Option<i64> {
    match unit {
        TimeUnit::Second if micros % 1_000_000 == 0 => Some(micros / 1_000_000),
        TimeUnit::Millisecond if micros % 1_000 == 0 => Some(micros / 1_000),
        TimeUnit::Microsecond => Some(micros),
        TimeUnit::Nanosecond => micros.checked_mul(1_000),
        _ => None,
    }
}

fn bloom_error(uri: &str, row_group: usize, column: &str, reason: &str) -> Error {
    Error::Execution(format!(
        "invalid Parquet Bloom filter in '{uri}', row group {row_group}, column '{column}': {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};
    use parquet::errors::ParquetError;

    use super::{
        BloomValue, bloom_error, bloom_location, contextual_bloom_error, timestamp_physical,
    };
    use crate::{Error, datasource::PredicateValue};

    #[test]
    fn query_local_bloom_errors_are_not_relabelled_as_corruption() {
        let error = contextual_bloom_error(
            "s3://bucket/data.parquet",
            0,
            "id",
            ParquetError::External(Box::new(Error::ResourceExhausted("query budget".into()))),
        );
        assert!(matches!(error, Error::ResourceExhausted(_)));
    }

    #[test]
    fn timestamp_bloom_conversion_requires_exact_coarse_units() {
        assert_eq!(timestamp_physical(2_000_000, TimeUnit::Second), Some(2));
        assert_eq!(timestamp_physical(2_000_001, TimeUnit::Second), None);
        assert_eq!(timestamp_physical(2, TimeUnit::Nanosecond), Some(2_000));
    }

    #[test]
    fn malformed_bloom_error_identifies_object_group_and_column() {
        let message = bloom_error("s3://bucket/corrupt.parquet", 4, "id", "bad header").to_string();
        assert!(message.contains("s3://bucket/corrupt.parquet"), "{message}");
        assert!(message.contains("row group 4"), "{message}");
        assert!(message.contains("id"), "{message}");
    }

    #[test]
    fn bloom_offset_and_length_metadata_handles_all_presence_combinations() {
        let uri = "s3://bucket/bloom.parquet";
        assert_eq!(bloom_location(uri, 2, "id", None, None).unwrap(), None);
        assert_eq!(
            bloom_location(uri, 2, "id", Some(10), None).unwrap(),
            Some((10, None))
        );
        assert_eq!(
            bloom_location(uri, 2, "id", Some(10), Some(20)).unwrap(),
            Some((10, Some(20)))
        );

        let error = bloom_location(uri, 2, "id", None, Some(20))
            .unwrap_err()
            .to_string();
        assert!(error.contains(uri), "{error}");
        assert!(error.contains("row group 2"), "{error}");
        assert!(error.contains("id"), "{error}");
        assert!(error.contains("without an offset"), "{error}");

        let zero = bloom_location(uri, 2, "id", Some(10), Some(0))
            .unwrap_err()
            .to_string();
        assert!(zero.contains("non-positive"), "{zero}");
    }

    #[test]
    fn unsupported_bloom_types_fall_back_without_pruning() {
        assert!(BloomValue::new(&DataType::Float64, &PredicateValue::Float64(1.5)).is_none());
        assert!(matches!(
            BloomValue::new(&DataType::UInt32, &PredicateValue::UInt64(u64::MAX)),
            Some(BloomValue::DefinitelyAbsent)
        ));
    }
}
