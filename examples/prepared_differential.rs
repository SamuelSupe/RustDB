use arrow::{csv::WriterBuilder, datatypes::DataType};
use futures::StreamExt;
use rustdb::{Engine, EngineConfig, ParameterValue, Session};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Engine::new(EngineConfig::default())?.session();
    if std::env::args().nth(1).as_deref() == Some("--verify-errors") {
        verify_errors(&session).await?;
        println!("prepared-errors,ok");
        return Ok(());
    }

    let statement = session.prepare(
        "SELECT $1 + $1 AS repeated_total, \
         $2 = CAST('18446744073709551615' AS UBIGINT) AS uint64_ok, \
         $3 = CAST('3.5' AS DOUBLE) AS float64_ok, \
         $4 = CAST('ABC' AS BLOB) AS binary_ok, \
         $5 = TIMESTAMP '2024-01-02 03:04:05.123456' AS timestamp_ok, \
         $6 IS NULL AS typed_null_ok, \
         $7 AS flag, $8 AS text_value, \
         $9 = DATE '2024-01-02' AS date_ok, \
         $10 = CAST('12.34' AS DECIMAL(10, 2)) AS decimal_ok",
    )?;
    if statement.parameter_count() != 10 {
        return Err("reused $1 must count as one numbered parameter".into());
    }
    let mut result = statement
        .execute(&[
            ParameterValue::Int64(21),
            ParameterValue::UInt64(u64::MAX),
            ParameterValue::Float64(3.5),
            ParameterValue::Binary(b"ABC".to_vec()),
            ParameterValue::TimestampMicrosecond(1_704_164_645_123_456),
            ParameterValue::Null(DataType::Int64),
            ParameterValue::Boolean(true),
            ParameterValue::Utf8("prepared".to_owned()),
            ParameterValue::Date32(19_724),
            ParameterValue::Decimal128 {
                value: 1_234,
                precision: 10,
                scale: 2,
            },
        ])
        .await?;
    let mut writer = WriterBuilder::new()
        .with_header(true)
        .build(std::io::stdout());
    while let Some(batch) = result.stream().next().await {
        writer.write(&batch?)?;
    }
    Ok(())
}

async fn verify_errors(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let mixed = match session.prepare("SELECT ? + $1") {
        Ok(_) => return Err("mixed placeholder styles unexpectedly succeeded".into()),
        Err(error) => error.to_string(),
    };
    if !mixed.contains("cannot mix '?' and '$n' parameters") {
        return Err(format!("unexpected mixed-placeholder error: {mixed}").into());
    }

    let statement = session.prepare("SELECT $1 + $2")?;
    if statement.parameter_count() != 2 {
        return Err("numbered parameter count must be the highest contiguous index".into());
    }
    let count = match statement.execute(&[ParameterValue::Int64(1)]).await {
        Ok(_) => return Err("wrong prepared parameter count unexpectedly succeeded".into()),
        Err(error) => error.to_string(),
    };
    if !count.contains("prepared statement expects 2 parameters, got 1") {
        return Err(format!("unexpected parameter-count error: {count}").into());
    }
    Ok(())
}
