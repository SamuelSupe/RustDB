use arrow::array::{
    Array, BinaryArray, Date32Array, Decimal128Array, Int64Array, TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use futures::TryStreamExt;

use super::ParameterValue;
use crate::{Engine, EngineConfig};

#[tokio::test]
async fn executes_question_and_reused_numbered_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    let question = session.prepare("SELECT ? + ? AS value").unwrap();
    assert_eq!(question.parameter_count(), 2);
    let batches = question
        .execute(&[ParameterValue::Int64(2), ParameterValue::Int64(3)])
        .await
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );

    let numbered = session.prepare("SELECT $1 + $1").unwrap();
    assert_eq!(numbered.parameter_count(), 1);
    assert!(numbered.execute(&[ParameterValue::Int64(4)]).await.is_ok());
}

#[tokio::test]
async fn execution_of_cached_ast_reports_no_sql_parse_time() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    let result = session
        .prepare("SELECT ? AS value")
        .unwrap()
        .execute(&[ParameterValue::Int64(7)])
        .await
        .unwrap();
    assert_eq!(
        result.metrics().snapshot().sql_parse_time,
        std::time::Duration::ZERO
    );
}

#[tokio::test]
async fn integer_parameters_work_in_limit_offset_and_fetch() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();

    let limited = session
        .prepare("SELECT 1 AS value UNION ALL SELECT 2 ORDER BY value LIMIT ? OFFSET ?")
        .unwrap()
        .execute(&[ParameterValue::Int64(1), ParameterValue::UInt64(1)])
        .await
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        limited[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );

    let fetched = session
        .prepare("SELECT 9 AS value FETCH FIRST $1 ROW ONLY")
        .unwrap()
        .execute(&[ParameterValue::UInt64(1)])
        .await
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        fetched.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn preserves_binary_decimal_temporal_and_typed_null_values() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    let statement = session.prepare("SELECT ?, ?, ?, ?, ?").unwrap();
    let batches = statement
        .execute(&[
            ParameterValue::Binary(vec![0, 0xab, 0xff]),
            ParameterValue::Decimal128 {
                value: -12_345,
                precision: 8,
                scale: 2,
            },
            ParameterValue::Date32(0),
            ParameterValue::TimestampMicrosecond(1_000_001),
            ParameterValue::Null(DataType::Binary),
        ])
        .await
        .unwrap()
        .into_stream()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = &batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 0xab, 0xff]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        -12_345
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        1_000_001
    );
    assert_eq!(batch.column(4).data_type(), &DataType::Binary);
    assert!(batch.column(4).is_null(0));
}

#[test]
fn rejects_mixed_gapped_and_file_function_parameters() {
    let directory = tempfile::tempdir().unwrap();
    let session = Engine::new(
        EngineConfig::builder()
            .spill_directory(directory.path().join("spill"))
            .build(),
    )
    .unwrap()
    .session();
    assert!(session.prepare("SELECT ? + $1").is_err());
    assert!(session.prepare("SELECT $2").is_err());
    assert!(
        session
            .prepare("SELECT * FROM read_csv(?, header = true)")
            .is_err()
    );
}

#[tokio::test]
async fn binding_errors_keep_the_original_placeholder_position() {
    let session = Engine::new(EngineConfig::default()).unwrap().session();
    let error = session
        .prepare("SELECT 1\nUNION ALL SELECT 2\nORDER BY ?")
        .unwrap()
        .execute(&[ParameterValue::Utf8("not-an-output-column".into())])
        .await;
    let error = match error {
        Ok(_) => panic!("incompatible parameter unexpectedly bound"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("line 3, column 10"), "{error}");
}
