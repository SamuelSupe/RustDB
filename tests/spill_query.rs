use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use arrow::array::{Array, Int64Array, StringArray};
use futures::StreamExt;
use rustdb::{CsvHeader, CsvOptions, Engine, EngineConfig, QueryResult, Result};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

fn low_memory_config(temp_dir: &Path, memory_limit: usize) -> EngineConfig {
    EngineConfig::builder()
        .memory_limit(memory_limit)
        .spill_directory(temp_dir.join("spill"))
        .batch_size(4_096)
        .build()
}

async fn consume(result: &mut QueryResult) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = result.stream().next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

fn assert_spilled_within_limit(result: &QueryResult, memory_limit: usize) {
    let metrics = result.metrics().snapshot();
    assert!(metrics.spill_bytes > 0, "query did not write spill bytes");
    assert!(
        metrics.spill_partitions > 0,
        "query did not write spill partitions"
    );
    assert!(
        metrics.peak_memory_bytes > 0,
        "query reported no memory use"
    );
    assert!(
        metrics.peak_memory_bytes <= memory_limit as u64,
        "peak reservation {} exceeded configured limit {memory_limit}",
        metrics.peak_memory_bytes
    );
}

fn write_aggregate_input(path: &Path, groups: i64, repeats: i64) {
    let mut output = BufWriter::new(File::create(path).expect("create aggregate CSV"));
    writeln!(output, "key,value").expect("write header");
    for repeat in 0..repeats {
        for key in 0..groups {
            writeln!(output, "{key},{}", key + repeat).expect("write aggregate row");
        }
    }
    output.flush().expect("flush aggregate CSV");
}

fn write_join_input(path: &Path, value_name: &str, rows: i64, multiplier: i64, add: i64) {
    let mut output = BufWriter::new(File::create(path).expect("create join CSV"));
    writeln!(output, "key,{value_name}").expect("write header");
    for key in 0..rows {
        writeln!(output, "{key},{}", key * multiplier + add).expect("write join row");
    }
    output.flush().expect("flush join CSV");
}

fn write_sort_inputs(directory: &Path, rows: i64, rows_per_file: i64) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for start in (0..rows).step_by(rows_per_file as usize) {
        let path = directory.join(format!("sort-{start:05}.csv"));
        let mut output = BufWriter::new(File::create(&path).expect("create sort CSV"));
        writeln!(output, "sort_key,payload").expect("write header");
        for index in start..(start + rows_per_file).min(rows) {
            let key = index * 7_919 % rows;
            writeln!(output, "{key},payload-{key:08}-xxxxxxxx").expect("write sort row");
        }
        output.flush().expect("flush sort CSV");
        paths.push(path);
    }
    paths
}

fn assert_multichunk_fixture(path: &Path) {
    let bytes = std::fs::metadata(path).expect("fixture metadata").len();
    assert!(
        bytes > (64 * KIB) as u64,
        "fixture must span multiple object-store chunks, only {bytes} bytes"
    );
}

#[tokio::test]
async fn high_cardinality_aggregate_spills_and_cleans_query_directory() -> Result<()> {
    const GROUPS: i64 = 12_000;
    const REPEATS: i64 = 3;
    // Full v0.2 batch, queue, state, and spill-writer accounting needs room
    // for one decoded batch plus the partition writers. The state budget is
    // still half this value, so the fixture is forced to spill.
    const MEMORY_LIMIT: usize = 4 * MIB;

    let temp = tempfile::tempdir().expect("tempdir");
    let csv = temp.path().join("aggregate.csv");
    write_aggregate_input(&csv, GROUPS, REPEATS);
    assert_multichunk_fixture(&csv);

    let config = low_memory_config(temp.path(), MEMORY_LIMIT);
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    session
        .register_csv(
            "events",
            [csv.to_string_lossy().into_owned()],
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;

    let mut result = session
        .execute(
            "SELECT key, count(*) AS rows_per_key, sum(value) AS total \
             FROM events GROUP BY key",
        )
        .await?;
    let query_dir = spill_root.join(format!("query-{}", result.query_id()));
    assert!(query_dir.is_dir(), "query spill directory was not created");
    let batches = consume(&mut result).await?;

    let mut actual = HashMap::with_capacity(GROUPS as usize);
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let totals = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.insert(keys.value(row), (counts.value(row), totals.value(row)));
        }
    }
    assert_eq!(actual.len(), GROUPS as usize);
    for key in 0..GROUPS {
        let expected_total = REPEATS * key + REPEATS * (REPEATS - 1) / 2;
        assert_eq!(actual[&key], (REPEATS, expected_total));
    }
    assert_spilled_within_limit(&result, MEMORY_LIMIT);
    assert!(
        !query_dir.exists(),
        "spill directory remained after stream completion"
    );
    drop(result);
    assert!(
        !query_dir.exists(),
        "spill directory remained after dropping the result"
    );
    Ok(())
}

#[tokio::test]
async fn hash_join_spills_and_cleans_query_directory() -> Result<()> {
    const ROWS: i64 = 20_000;
    const MEMORY_LIMIT: usize = 2 * MIB;

    let temp = tempfile::tempdir().expect("tempdir");
    let left_csv = temp.path().join("left.csv");
    let right_csv = temp.path().join("right.csv");
    write_join_input(&left_csv, "left_value", ROWS, 2, 7);
    write_join_input(&right_csv, "right_value", ROWS, 3, 1);
    assert_multichunk_fixture(&left_csv);
    assert_multichunk_fixture(&right_csv);

    let config = low_memory_config(temp.path(), MEMORY_LIMIT);
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    let csv_options = CsvOptions {
        header: CsvHeader::Present,
        ..CsvOptions::default()
    };
    session
        .register_csv(
            "left_rows",
            [left_csv.to_string_lossy().into_owned()],
            csv_options.clone(),
        )
        .await?;
    session
        .register_csv(
            "right_rows",
            [right_csv.to_string_lossy().into_owned()],
            csv_options,
        )
        .await?;

    let mut result = session
        .execute(
            "SELECT l.key, l.left_value, r.right_value \
             FROM left_rows AS l INNER JOIN right_rows AS r ON l.key = r.key",
        )
        .await?;
    let query_dir = spill_root.join(format!("query-{}", result.query_id()));
    assert!(query_dir.is_dir(), "query spill directory was not created");
    let batches = consume(&mut result).await?;

    let mut output_rows = 0_i64;
    let mut key_sum = 0_i64;
    let mut left_sum = 0_i64;
    let mut right_sum = 0_i64;
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            output_rows += 1;
            key_sum += keys.value(row);
            left_sum += left.value(row);
            right_sum += right.value(row);
        }
    }
    let expected_key_sum = ROWS * (ROWS - 1) / 2;
    assert_eq!(output_rows, ROWS);
    assert_eq!(key_sum, expected_key_sum);
    assert_eq!(left_sum, 2 * expected_key_sum + 7 * ROWS);
    assert_eq!(right_sum, 3 * expected_key_sum + ROWS);
    assert_spilled_within_limit(&result, MEMORY_LIMIT);
    assert!(
        !query_dir.exists(),
        "spill directory remained after stream completion"
    );
    drop(result);
    assert!(
        !query_dir.exists(),
        "spill directory remained after dropping the result"
    );
    Ok(())
}

#[tokio::test]
async fn left_hash_join_spills_preserves_unmatched_rows_and_cleans_up() -> Result<()> {
    const LEFT_ROWS: i64 = 24_000;
    const RIGHT_ROWS: i64 = 20_000;
    const MEMORY_LIMIT: usize = 2 * MIB;

    let temp = tempfile::tempdir().expect("tempdir");
    let left_csv = temp.path().join("left-outer.csv");
    let right_csv = temp.path().join("right-outer.csv");
    write_join_input(&left_csv, "left_value", LEFT_ROWS, 2, 7);
    write_join_input(&right_csv, "right_value", RIGHT_ROWS, 3, 1);
    assert_multichunk_fixture(&left_csv);
    assert_multichunk_fixture(&right_csv);

    let config = low_memory_config(temp.path(), MEMORY_LIMIT);
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    let csv_options = CsvOptions {
        header: CsvHeader::Present,
        ..CsvOptions::default()
    };
    session
        .register_csv(
            "left_outer_rows",
            [left_csv.to_string_lossy().into_owned()],
            csv_options.clone(),
        )
        .await?;
    session
        .register_csv(
            "right_outer_rows",
            [right_csv.to_string_lossy().into_owned()],
            csv_options,
        )
        .await?;

    let mut result = session
        .execute(
            "SELECT l.key, l.left_value, r.right_value \
             FROM left_outer_rows AS l LEFT JOIN right_outer_rows AS r ON l.key = r.key",
        )
        .await?;
    let query_dir = spill_root.join(format!("query-{}", result.query_id()));
    assert!(query_dir.is_dir(), "query spill directory was not created");
    let batches = consume(&mut result).await?;

    let mut output_rows = 0_i64;
    let mut key_sum = 0_i64;
    let mut left_sum = 0_i64;
    let mut right_sum = 0_i64;
    let mut unmatched_rows = 0_i64;
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            output_rows += 1;
            key_sum += key;
            left_sum += left.value(row);
            if right.is_null(row) {
                assert!(
                    key >= RIGHT_ROWS,
                    "matched key {key} unexpectedly became NULL"
                );
                unmatched_rows += 1;
            } else {
                assert!(key < RIGHT_ROWS, "unmatched key {key} unexpectedly joined");
                right_sum += right.value(row);
            }
        }
    }

    let expected_left_key_sum = LEFT_ROWS * (LEFT_ROWS - 1) / 2;
    let expected_right_key_sum = RIGHT_ROWS * (RIGHT_ROWS - 1) / 2;
    assert_eq!(output_rows, LEFT_ROWS);
    assert_eq!(unmatched_rows, LEFT_ROWS - RIGHT_ROWS);
    assert_eq!(key_sum, expected_left_key_sum);
    assert_eq!(left_sum, 2 * expected_left_key_sum + 7 * LEFT_ROWS);
    assert_eq!(right_sum, 3 * expected_right_key_sum + RIGHT_ROWS);
    assert_spilled_within_limit(&result, MEMORY_LIMIT);
    assert!(
        !query_dir.exists(),
        "left-join spill directory remained after stream completion"
    );
    drop(result);
    assert!(
        !query_dir.exists(),
        "left-join spill directory remained after dropping the result"
    );
    Ok(())
}

#[tokio::test]
async fn order_by_top_k_and_full_sort_spill_and_cleanup() -> Result<()> {
    const ROWS: i64 = 40_000;
    const TOP_K: i64 = 257;
    const MEMORY_LIMIT: usize = 2 * MIB;

    let temp = tempfile::tempdir().expect("tempdir");
    let csv_files = write_sort_inputs(temp.path(), ROWS, ROWS);

    let config = low_memory_config(temp.path(), MEMORY_LIMIT);
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    session
        .register_csv(
            "sortable",
            csv_files
                .iter()
                .map(|path| path.to_string_lossy().into_owned()),
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;

    let mut top_k = session
        .execute(
            "SELECT sort_key, payload FROM sortable \
             ORDER BY sort_key DESC LIMIT 257",
        )
        .await?;
    let top_k_dir = spill_root.join(format!("query-{}", top_k.query_id()));
    let top_k_batches = consume(&mut top_k).await?;
    let mut top_k_keys = Vec::new();
    let mut top_k_payloads = Vec::new();
    for batch in top_k_batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            top_k_keys.push(keys.value(row));
            top_k_payloads.push(payloads.value(row).to_owned());
        }
    }
    let expected_top_k = ((ROWS - TOP_K)..ROWS).rev().collect::<Vec<_>>();
    assert_eq!(top_k_keys, expected_top_k);
    for (key, payload) in top_k_keys.iter().zip(&top_k_payloads) {
        assert_eq!(payload, &format!("payload-{key:08}-xxxxxxxx"));
    }
    assert_spilled_within_limit(&top_k, MEMORY_LIMIT);
    assert!(!top_k_dir.exists(), "Top-K spill directory remained");
    drop(top_k);
    assert!(!top_k_dir.exists(), "Top-K directory remained after drop");

    let mut full_sort = session
        .execute("SELECT sort_key FROM sortable ORDER BY sort_key ASC")
        .await?;
    let full_sort_dir = spill_root.join(format!("query-{}", full_sort.query_id()));
    let full_batches = consume(&mut full_sort).await?;
    let mut full_keys = Vec::<i64>::with_capacity(ROWS as usize);
    for batch in full_batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        full_keys.extend_from_slice(keys.values());
    }
    assert_eq!(full_keys, (0..ROWS).collect::<Vec<_>>());
    assert_spilled_within_limit(&full_sort, MEMORY_LIMIT);
    assert!(
        !full_sort_dir.exists(),
        "full-sort spill directory remained"
    );
    drop(full_sort);
    assert!(
        !full_sort_dir.exists(),
        "full-sort directory remained after drop"
    );
    Ok(())
}

#[tokio::test]
async fn dropping_a_partially_consumed_spilling_query_cleans_up_immediately() -> Result<()> {
    const ROWS: i64 = 40_000;
    const MEMORY_LIMIT: usize = 2 * MIB;

    let temp = tempfile::tempdir().expect("tempdir");
    let csv_files = write_sort_inputs(temp.path(), ROWS, ROWS);
    let config = low_memory_config(temp.path(), MEMORY_LIMIT);
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    session
        .register_csv(
            "abandoned_sort",
            csv_files
                .iter()
                .map(|path| path.to_string_lossy().into_owned()),
            CsvOptions {
                header: CsvHeader::Present,
                ..CsvOptions::default()
            },
        )
        .await?;

    let mut result = session
        .execute("SELECT sort_key, payload FROM abandoned_sort ORDER BY sort_key")
        .await?;
    let query_dir = spill_root.join(format!("query-{}", result.query_id()));
    let first = result
        .stream()
        .next()
        .await
        .expect("spilling sort must produce a batch")?;
    assert!(first.num_rows() < ROWS as usize, "query was fully consumed");
    assert_spilled_within_limit(&result, MEMORY_LIMIT);
    assert!(
        query_dir.is_dir(),
        "query directory disappeared before the consumer was dropped"
    );

    let metrics = result.metrics();
    drop(result);

    assert!(
        !query_dir.exists(),
        "abandoned query did not synchronously remove its spill directory"
    );
    assert!(
        !metrics.snapshot().elapsed.is_zero(),
        "abandoned query metrics were not finalized"
    );
    Ok(())
}
