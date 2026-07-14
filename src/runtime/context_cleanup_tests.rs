use std::{sync::Arc, time::Duration};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use tokio::sync::{Notify, oneshot};

use super::QueryContext;
use crate::{Error, runtime::MemoryPool};

#[tokio::test]
async fn terminal_cleanup_owns_unfinished_task_group_writer_deletion() {
    let root = tempfile::tempdir().expect("tempdir");
    let context =
        Arc::new(QueryContext::new(MemoryPool::new(1 << 20), root.path()).expect("context"));
    let directory = context.spill.directory().to_owned();
    let release_writer = Arc::new(Notify::new());
    let release_task = Arc::new(Notify::new());
    let (path_sender, path_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let task_context = Arc::clone(&context);
    let task_writer_release = Arc::clone(&release_writer);
    let task_release = Arc::clone(&release_task);

    context
        .tasks
        .spawn("unfinished-spill-writer", async move {
            let schema = Arc::new(Schema::empty());
            let mut writer = task_context.spill.writer("join-left", schema.clone())?;
            writer.write_batch(&RecordBatch::new_empty(schema))?;
            path_sender
                .send(writer.path().to_owned())
                .map_err(|_| Error::Internal("test could not publish spill path".to_owned()))?;
            task_writer_release.notified().await;
            drop(writer);
            dropped_sender
                .send(())
                .map_err(|_| Error::Internal("test could not publish writer drop".to_owned()))?;
            task_release.notified().await;
            Ok(())
        })
        .unwrap();
    let path = path_receiver.await.unwrap();

    let cleanup_context = Arc::clone(&context);
    let cleanup = tokio::spawn(async move { cleanup_context.cleanup_spill_after_tasks().await });
    tokio::task::yield_now().await;
    assert!(
        !cleanup.is_finished(),
        "cleanup passed an active query task"
    );

    release_writer.notify_one();
    dropped_receiver.await.unwrap();
    assert!(
        path.exists(),
        "task-group writer was unlinked before the terminal cleanup barrier"
    );
    release_task.notify_one();
    cleanup.await.unwrap().unwrap();

    assert!(!directory.exists());
    assert_eq!(context.metrics.snapshot().active_spill_files, 0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !directory.exists(),
        "spill directory reappeared after cleanup"
    );
}

#[tokio::test]
async fn terminal_cleanup_orders_finished_file_deletions() {
    let root = tempfile::tempdir().expect("tempdir");
    let context = QueryContext::new(MemoryPool::new(1 << 20), root.path()).expect("context");
    let directory = context.spill.directory().to_owned();
    let schema = Arc::new(Schema::empty());

    for partition in 0..32 {
        let mut writer = context
            .spill
            .writer(&format!("join-partition-{partition}"), schema.clone())
            .unwrap();
        writer
            .write_batch(&RecordBatch::new_empty(schema.clone()))
            .unwrap();
        let spill_file = writer.finish(1).unwrap();
        context.spill.remove_file(&spill_file).unwrap();
    }

    context.cleanup_spill_after_tasks().await.unwrap();
    assert!(!directory.exists());
    assert_eq!(context.metrics.snapshot().active_spill_files, 0);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !directory.exists(),
        "spill directory reappeared after ordered file deletion cleanup"
    );
}
