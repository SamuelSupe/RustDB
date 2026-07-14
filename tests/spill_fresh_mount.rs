use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use futures::StreamExt;
use rustdb::{CsvHeader, CsvOptions, Engine, EngineConfig, Result};

const GROUPS: i64 = 50_000;
const MEMORY_LIMIT: usize = 4 * 1024 * 1024;

#[tokio::test]
#[ignore = "run through scripts/ci/spill_fresh_mount.sh"]
async fn fresh_mount_cleanup_probe() -> Result<()> {
    let root = PathBuf::from(
        std::env::var_os("RUSTDB_SPILL_REPLAY_ROOT")
            .expect("RUSTDB_SPILL_REPLAY_ROOT must name the bind-mounted probe directory"),
    );
    assert!(
        root.is_dir(),
        "probe root '{}' does not exist",
        root.display()
    );

    let input = root.join("input.csv");
    write_input(&input);

    let config = EngineConfig::builder()
        .memory_limit(MEMORY_LIMIT)
        .batch_size(4_096)
        .compute_threads(1)
        .spill_directory(root.join("spill"))
        .build();
    let spill_root = config.spill.directory.clone();
    let session = Engine::new(config)?.session();
    session
        .register_csv(
            "events",
            [input.to_string_lossy().into_owned()],
            CsvOptions::builder().header(CsvHeader::Present).build(),
        )
        .await?;

    let mut result = session
        .execute("SELECT key, sum(value) FROM events GROUP BY key")
        .await?;
    let query_directory = spill_root.join(format!("query-{}", result.query_id()));
    let mut rows = 0;
    while let Some(batch) = result.stream().next().await {
        rows += batch?.num_rows();
    }

    let metrics = result.metrics().snapshot();
    assert_eq!(rows, GROUPS as usize);
    assert!(metrics.spill_bytes > 0, "probe query did not spill");
    assert_eq!(metrics.active_spill_bytes, 0);
    assert_eq!(metrics.active_spill_files, 0);
    assert!(
        !query_directory.exists(),
        "query spill directory remained after stream completion"
    );
    assert!(
        std::fs::read_dir(&spill_root)
            .expect("read spill root")
            .next()
            .is_none(),
        "spill root was not empty immediately after cleanup"
    );
    Ok(())
}

fn write_input(path: &Path) {
    let mut output = BufWriter::new(File::create(path).expect("create probe CSV"));
    writeln!(output, "key,value").expect("write probe header");
    for repeat in 0..3 {
        for key in 0..GROUPS {
            writeln!(output, "{key},{}", key + repeat).expect("write probe row");
        }
    }
    output.flush().expect("flush probe CSV");
}
