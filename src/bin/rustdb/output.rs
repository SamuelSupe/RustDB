use std::io::{self, Write};

use arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::LineDelimitedWriter, record_batch::RecordBatch,
    util::display::array_value_to_string,
};
use futures::StreamExt;

use rustdb::{Error, QueryResult, Result};

use super::args::OutputFormat;

pub async fn write_result(
    result: &mut QueryResult,
    format: OutputFormat,
    csv_null: Option<&str>,
) -> Result<()> {
    let schema = result.schema();
    let mut first = true;
    while let Some(batch) = result.stream().next().await {
        let batch = batch?;
        write_batch(&batch, format, csv_null, first)?;
        first = false;
    }
    if first {
        let empty = RecordBatch::new_empty(schema);
        match format {
            OutputFormat::Table => write_table_batch(&empty, true)?,
            OutputFormat::Csv => {
                let mut bytes = Vec::new();
                csv_writer(true, csv_null).build(&mut bytes).write(&empty)?;
                io::stdout()
                    .write_all(&bytes)
                    .map_err(|error| Error::io(None, error))?;
            }
            OutputFormat::Jsonl => {}
        }
    }
    Ok(())
}

pub(crate) fn write_batch(
    batch: &RecordBatch,
    format: OutputFormat,
    csv_null: Option<&str>,
    header: bool,
) -> Result<()> {
    match format {
        OutputFormat::Table => write_table_batch(batch, header),
        OutputFormat::Csv => {
            let mut bytes = Vec::new();
            csv_writer(header, csv_null)
                .build(&mut bytes)
                .write(batch)?;
            io::stdout()
                .write_all(&bytes)
                .map_err(|error| Error::io(None, error))
        }
        OutputFormat::Jsonl => {
            let mut bytes = Vec::new();
            LineDelimitedWriter::new(&mut bytes).write(batch)?;
            io::stdout()
                .write_all(&bytes)
                .map_err(|error| Error::io(None, error))
        }
    }
}

fn csv_writer(header: bool, null_value: Option<&str>) -> CsvWriterBuilder {
    let writer = CsvWriterBuilder::new().with_header(header);
    match null_value {
        Some(value) => writer.with_null(value.to_owned()),
        None => writer,
    }
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
        "elapsed={:?} rows={} scanned_rows={} scanned_bytes={} discovered_files={} peak_memory={} peak_lanes={} scheduler_wait={:?} spill_bytes={} spill_read={} spill_write={} spill_logical_input={} spill_write_amplification_millionths={} spill_active={} spill_peak={} spill_files={} spill_files_active={} spill_files_peak={} repartition_bytes={} repartition_depth={} max_partition={} quota_rejections={} join_candidates={} join_short_circuits={} runtime_filter_hits={} csv_source_bytes={} csv_decompressed_bytes={} csv_morsels={} csv_parser_lanes={} metadata_hits={} metadata_misses={} metadata_wait={:?} cancel_quiesce={:?} parquet_index_bytes={} parquet_bloom_bytes={} pages_pruned={} page_rows_pruned={} bloom_row_groups_pruned={} pruning_budget_skips={} s3_requests={} s3_bytes={}",
        metrics.elapsed,
        metrics.rows_returned,
        metrics.rows_scanned,
        metrics.bytes_scanned,
        metrics.discovered_files,
        metrics.peak_memory_bytes,
        metrics.peak_active_lanes,
        metrics.scheduler_wait,
        metrics.spill_bytes,
        metrics.spill_read_bytes,
        metrics.spill_write_bytes,
        metrics.spill_logical_input_bytes,
        metrics.spill_write_amplification_millionths,
        metrics.active_spill_bytes,
        metrics.peak_active_spill_bytes,
        metrics.spill_files,
        metrics.active_spill_files,
        metrics.peak_active_spill_files,
        metrics.spill_repartition_bytes,
        metrics.max_repartition_depth,
        metrics.max_spill_partition_bytes,
        metrics.spill_quota_rejections,
        metrics.join_candidate_pairs,
        metrics.join_short_circuits,
        metrics.runtime_filter_hits,
        metrics.csv_source_bytes,
        metrics.csv_decompressed_bytes,
        metrics.csv_morsels,
        metrics.peak_csv_parser_lanes,
        metrics.metadata_cache_hits,
        metrics.metadata_cache_misses,
        metrics.metadata_singleflight_wait,
        metrics.cancel_to_quiesce,
        metrics.parquet_page_index_bytes_read,
        metrics.parquet_bloom_filter_bytes_read,
        metrics.parquet_pages_pruned,
        metrics.parquet_page_rows_pruned,
        metrics.parquet_bloom_row_groups_pruned,
        metrics.parquet_pruning_budget_skips,
        metrics.s3_requests,
        metrics.s3_bytes_transferred,
    );
}
