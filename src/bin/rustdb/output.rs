use std::io::{self, Write};

use arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::LineDelimitedWriter, record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use futures::StreamExt;

use rustdb::{Error, QueryResult, Result};

use super::args::OutputFormat;

pub async fn write_result(result: &mut QueryResult, format: OutputFormat) -> Result<()> {
    let schema = result.schema();
    let mut first = true;
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        match format {
            OutputFormat::Table => {
                write_table_batch(&batch, first)?;
            }
            OutputFormat::Csv => {
                let mut bytes = Vec::new();
                CsvWriterBuilder::new()
                    .with_header(first)
                    .build(&mut bytes)
                    .write(&batch)?;
                io::stdout()
                    .write_all(&bytes)
                    .map_err(|error| Error::io(None, error))?;
            }
            OutputFormat::Jsonl => {
                let mut bytes = Vec::new();
                LineDelimitedWriter::new(&mut bytes).write(&batch)?;
                io::stdout()
                    .write_all(&bytes)
                    .map_err(|error| Error::io(None, error))?;
            }
        }
        first = false;
    }
    if first {
        let empty = RecordBatch::new_empty(schema);
        match format {
            OutputFormat::Table => write_table_batch(&empty, true)?,
            OutputFormat::Csv => {
                let mut bytes = Vec::new();
                CsvWriterBuilder::new()
                    .with_header(true)
                    .build(&mut bytes)
                    .write(&empty)?;
                io::stdout()
                    .write_all(&bytes)
                    .map_err(|error| Error::io(None, error))?;
            }
            OutputFormat::Jsonl => {}
        }
    }
    Ok(())
}

fn write_table_batch(batch: &RecordBatch, header: bool) -> Result<()> {
    let mut stdout = io::stdout().lock();
    if header {
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        writeln!(stdout, "{}", names.join(" | ")).map_err(|error| Error::io(None, error))?;
        writeln!(
            stdout,
            "{}",
            names
                .iter()
                .map(|name| "-".repeat(name.chars().count().max(1)))
                .collect::<Vec<_>>()
                .join("-+-")
        )
        .map_err(|error| Error::io(None, error))?;
    }
    for row in 0..batch.num_rows() {
        let values = batch
            .columns()
            .iter()
            .map(|column| array_value_to_string(column.as_ref(), row))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        writeln!(stdout, "{}", values.join(" | ")).map_err(|error| Error::io(None, error))?;
    }
    Ok(())
}

pub fn print_metrics(result: &QueryResult) {
    let metrics = result.metrics().snapshot();
    eprintln!(
        "elapsed={:?} rows={} scanned_rows={} scanned_bytes={} peak_memory={} spill_bytes={} s3_requests={} s3_bytes={}",
        metrics.elapsed,
        metrics.rows_returned,
        metrics.rows_scanned,
        metrics.bytes_scanned,
        metrics.peak_memory_bytes,
        metrics.spill_bytes,
        metrics.s3_requests,
        metrics.s3_bytes_transferred,
    );
}
