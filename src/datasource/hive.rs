use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray, new_null_array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};

use super::{ComparisonOp, PredicateValue, ScanPredicate};
use crate::{Error, Result, storage::ObjectSource};

const NULL_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

#[derive(Clone, Debug)]
pub(super) struct HivePartitions {
    columns: Arc<[PartitionColumn]>,
    files: Arc<[FilePartition]>,
    physical_columns: usize,
}

#[derive(Clone, Debug)]
struct PartitionColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
}

#[derive(Clone, Debug)]
struct FilePartition {
    values: Vec<Option<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InferredType {
    Boolean,
    Int64,
    Date32,
    Utf8,
}

impl HivePartitions {
    pub(super) fn discover(files: &[ObjectSource], physical: &Schema) -> Result<Option<Self>> {
        let partitions = files
            .iter()
            .map(|file| parse_partitions(file.location().as_ref()))
            .collect::<Result<Vec<_>>>()?;
        Self::from_maps(partitions, physical)
    }

    fn from_maps(
        partitions: Vec<Vec<(String, Option<String>)>>,
        physical: &Schema,
    ) -> Result<Option<Self>> {
        let mut names = Vec::<String>::new();
        for file in &partitions {
            for (name, _) in file {
                if physical
                    .fields()
                    .iter()
                    .any(|field| field.name().eq_ignore_ascii_case(name))
                {
                    return Err(Error::InvalidArgument(format!(
                        "Hive partition column '{name}' conflicts with a Parquet column"
                    )));
                }
                if let Some(existing) = names
                    .iter()
                    .find(|existing| existing.eq_ignore_ascii_case(name))
                {
                    if existing != name {
                        return Err(Error::InvalidArgument(format!(
                            "Hive partition column has inconsistent casing: '{existing}' and '{name}'"
                        )));
                    }
                } else {
                    names.push(name.clone());
                }
            }
        }
        if names.is_empty() {
            return Ok(None);
        }

        let files = partitions
            .iter()
            .map(|partition| {
                let values: HashMap<_, _> = partition.iter().cloned().collect();
                FilePartition {
                    values: names
                        .iter()
                        .map(|name| values.get(name).cloned().flatten())
                        .collect(),
                }
            })
            .collect::<Vec<_>>();
        let columns = names
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                let values = files.iter().map(|file| file.values[index].as_deref());
                let (data_type, nullable) = infer_column(values);
                PartitionColumn {
                    name,
                    data_type,
                    nullable,
                }
            })
            .collect::<Vec<_>>();

        Ok(Some(Self {
            columns: columns.into(),
            files: files.into(),
            physical_columns: physical.fields().len(),
        }))
    }

    pub(super) fn append_schema(&self, physical: &SchemaRef) -> SchemaRef {
        let mut fields = physical.fields().iter().cloned().collect::<Vec<_>>();
        fields.extend(self.columns.iter().map(|column| {
            Arc::new(Field::new(
                &column.name,
                column.data_type.clone(),
                column.nullable,
            ))
        }));
        Arc::new(Schema::new_with_metadata(
            fields,
            physical.metadata().clone(),
        ))
    }

    pub(super) fn validate_physical_schema(&self, physical: &Schema) -> Result<()> {
        for column in self.columns.iter() {
            if physical
                .fields()
                .iter()
                .any(|field| field.name().eq_ignore_ascii_case(&column.name))
            {
                return Err(Error::Execution(format!(
                    "Parquet schema changed to include Hive partition column '{}'",
                    column.name
                )));
            }
        }
        Ok(())
    }

    pub(super) fn can_prune(&self, file: usize, predicate: Option<&ScanPredicate>) -> bool {
        predicate.is_some_and(|predicate| self.predicate_prunes(file, predicate))
    }

    fn predicate_prunes(&self, file: usize, predicate: &ScanPredicate) -> bool {
        match predicate {
            ScanPredicate::And(predicates) => predicates
                .iter()
                .any(|predicate| self.predicate_prunes(file, predicate)),
            ScanPredicate::Comparison { column, op, value } => {
                let Some(partition) = column.checked_sub(self.physical_columns) else {
                    return false;
                };
                self.comparison_prunes(file, partition, *op, value)
            }
            ScanPredicate::IsNull { column } => self
                .partition_value(file, *column)
                .is_some_and(|value| value.is_some()),
            ScanPredicate::IsNotNull { column } => self
                .partition_value(file, *column)
                .is_some_and(|value| value.is_none()),
        }
    }

    fn comparison_prunes(
        &self,
        file: usize,
        partition: usize,
        op: ComparisonOp,
        predicate: &PredicateValue,
    ) -> bool {
        let Some(column) = self.columns.get(partition) else {
            return false;
        };
        let Some(raw) = self
            .files
            .get(file)
            .and_then(|file| file.values.get(partition))
        else {
            return false;
        };
        let Some(raw) = raw else {
            return true;
        };

        match (&column.data_type, predicate) {
            (DataType::Boolean, PredicateValue::Boolean(value)) => {
                parse_bool(raw).is_some_and(|actual| comparison_is_false(actual, *value, op))
            }
            (DataType::Int64, PredicateValue::Int64(value)) => raw
                .parse::<i64>()
                .ok()
                .is_some_and(|actual| comparison_is_false(actual, *value, op)),
            (DataType::Date32, PredicateValue::Date32(value)) => {
                parse_date32(raw).is_some_and(|actual| comparison_is_false(actual, *value, op))
            }
            (DataType::Utf8, PredicateValue::Utf8(value)) => {
                comparison_is_false(raw.as_str(), value.as_str(), op)
            }
            _ => false,
        }
    }

    fn partition_value(&self, file: usize, table_column: usize) -> Option<&Option<String>> {
        let partition = table_column.checked_sub(self.physical_columns)?;
        self.files.get(file)?.values.get(partition)
    }

    pub(super) fn array(&self, file: usize, name: &str, rows: usize) -> Result<Option<ArrayRef>> {
        let Some((index, column)) = self
            .columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
        else {
            return Ok(None);
        };
        let raw = self
            .files
            .get(file)
            .and_then(|file| file.values.get(index))
            .ok_or_else(|| Error::Internal("missing Hive partition value".to_owned()))?;
        let Some(raw) = raw else {
            return Ok(Some(new_null_array(&column.data_type, rows)));
        };
        let array: ArrayRef = match &column.data_type {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![parse_bool(raw); rows])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![raw.parse::<i64>().ok(); rows])),
            DataType::Date32 => Arc::new(Date32Array::from(vec![parse_date32(raw); rows])),
            DataType::Utf8 => Arc::new(StringArray::from(vec![raw.as_str(); rows])),
            data_type => {
                return Err(Error::Internal(format!(
                    "unsupported Hive partition type {data_type:?}"
                )));
            }
        };
        Ok(Some(array))
    }
}

fn parse_partitions(path: &str) -> Result<Vec<(String, Option<String>)>> {
    let mut partitions = Vec::<(String, Option<String>)>::new();
    let mut segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .peekable();
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            break;
        }
        let Some((name, value)) = segment.split_once('=') else {
            continue;
        };
        if name.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "Hive partition path has an empty key: {path}"
            )));
        }
        if partitions.iter().any(|(existing, _)| existing == name) {
            return Err(Error::InvalidArgument(format!(
                "Hive partition key '{name}' appears more than once in {path}"
            )));
        }
        let value = (value != NULL_PARTITION).then(|| value.to_owned());
        partitions.push((name.to_owned(), value));
    }
    Ok(partitions)
}

fn infer_column<'a>(values: impl Iterator<Item = Option<&'a str>>) -> (DataType, bool) {
    let mut inferred = None;
    let mut nullable = false;
    for value in values {
        let Some(value) = value else {
            nullable = true;
            continue;
        };
        let candidate = infer_value(value);
        inferred = Some(match inferred {
            None => candidate,
            Some(current) if current == candidate => current,
            Some(_) => InferredType::Utf8,
        });
    }
    let data_type = match inferred.unwrap_or(InferredType::Utf8) {
        InferredType::Boolean => DataType::Boolean,
        InferredType::Int64 => DataType::Int64,
        InferredType::Date32 => DataType::Date32,
        InferredType::Utf8 => DataType::Utf8,
    };
    (data_type, nullable)
}

fn infer_value(value: &str) -> InferredType {
    if parse_bool(value).is_some() {
        InferredType::Boolean
    } else if value.parse::<i64>().is_ok() {
        InferredType::Int64
    } else if parse_date32(value).is_some() {
        InferredType::Date32
    } else {
        InferredType::Utf8
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    if value.eq_ignore_ascii_case("true") {
        Some(true)
    } else if value.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

fn parse_date32(value: &str) -> Option<i32> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        28 + u32::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day == 0 || day > month_days[usize::try_from(month - 1).ok()?] {
        return None;
    }

    let adjusted_year = year - i32::from(month <= 2);
    let era = adjusted_year.div_euclid(400);
    let year_of_era = adjusted_year - era * 400;
    let shifted_month = i32::try_from(month).ok()? + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + i32::try_from(day).ok()? - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era.checked_mul(146_097)?
        .checked_add(day_of_era)?
        .checked_sub(719_468)
}

fn comparison_is_false<T: PartialOrd + PartialEq>(
    actual: T,
    expected: T,
    op: ComparisonOp,
) -> bool {
    let ordering = actual.partial_cmp(&expected);
    let matches = match op {
        ComparisonOp::Eq => actual == expected,
        ComparisonOp::NotEq => actual != expected,
        ComparisonOp::Lt => ordering == Some(Ordering::Less),
        ComparisonOp::LtEq => matches!(ordering, Some(Ordering::Less | Ordering::Equal)),
        ComparisonOp::Gt => ordering == Some(Ordering::Greater),
        ComparisonOp::GtEq => matches!(ordering, Some(Ordering::Greater | Ordering::Equal)),
    };
    !matches
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{HivePartitions, infer_column, parse_date32, parse_partitions};
    use crate::datasource::{ComparisonOp, PredicateValue, ScanPredicate};

    #[test]
    fn extracts_only_directory_partitions() {
        let values =
            parse_partitions("warehouse/year=2026/active=true/part=ignored.parquet").unwrap();
        assert_eq!(
            values,
            vec![
                ("year".to_owned(), Some("2026".to_owned())),
                ("active".to_owned(), Some("true".to_owned())),
            ]
        );
    }

    #[test]
    fn infers_supported_types_and_falls_back_to_utf8() {
        assert_eq!(
            infer_column([Some("1"), Some("2")].into_iter()).0,
            DataType::Int64
        );
        assert_eq!(
            infer_column([Some("true"), Some("false")].into_iter()).0,
            DataType::Boolean
        );
        assert_eq!(
            infer_column([Some("2026-01-01")].into_iter()).0,
            DataType::Date32
        );
        assert_eq!(
            infer_column([Some("1"), Some("x")].into_iter()).0,
            DataType::Utf8
        );
        assert_eq!(parse_date32("1970-01-01"), Some(0));
        assert_eq!(parse_date32("2025-02-29"), None);
    }

    #[test]
    fn prunes_equal_and_range_partition_predicates() {
        let hive = HivePartitions::from_maps(
            vec![
                vec![("year".to_owned(), Some("2025".to_owned()))],
                vec![("year".to_owned(), Some("2026".to_owned()))],
            ],
            &Schema::empty(),
        )
        .unwrap()
        .unwrap();
        let equal = ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::Eq,
            value: PredicateValue::Int64(2026),
        };
        let range = ScanPredicate::Comparison {
            column: 0,
            op: ComparisonOp::GtEq,
            value: PredicateValue::Int64(2026),
        };
        assert!(hive.can_prune(0, Some(&equal)));
        assert!(!hive.can_prune(1, Some(&equal)));
        assert!(hive.can_prune(0, Some(&range)));
        assert!(!hive.can_prune(1, Some(&range)));

        let changed = Schema::new(vec![Field::new("year", DataType::Int64, false)]);
        assert!(hive.validate_physical_schema(&changed).is_err());
    }
}
