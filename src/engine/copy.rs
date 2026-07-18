use std::{sync::Arc, time::Duration};

use arrow::{
    array::{StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::StreamExt;
use sqlparser::ast::Statement;
use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session, query_result};
use crate::{
    Error, Result,
    command::CopyToCommand,
    runtime::{BatchEnvelope, boxed_memory_batch_stream},
    sql::StatementPlan,
    storage::WriteDestination,
};

impl Session {
    pub(super) async fn execute_copy_to(
        &self,
        command: CopyToCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        let prepared = self
            .prepare_ast_for_query(Statement::Query(command.query), Some(Arc::clone(&context)))
            .await
            .map_err(|error| context.error_with_cleanup(error))?;
        let StatementPlan::Query(plan) = prepared else {
            return Err(context.error_with_cleanup(Error::Internal(
                "COPY TO source produced an EXPLAIN plan".to_owned(),
            )));
        };
        let source_schema = Arc::clone(plan.schema().arrow());
        let input =
            crate::execution::execute_internal(StatementPlan::Query(plan), Arc::clone(&context))
                .await
                .map_err(|error| context.error_with_cleanup(error))?;
        let destination =
            WriteDestination::resolve(&command.location, &self.engine.inner.config.s3)
                .map_err(|error| context.error_with_cleanup(error))?;
        let result_schema = copy_status(0, 0, &command.location)?.schema();
        let sink_context = Arc::clone(&context);
        let io = self.engine.inner.spill_io.clone();
        let location = command.location;
        let sink = boxed_memory_batch_stream(async_stream::try_stream! {
            let mut input = input;
            let mut sink = Some(super::copy_sink::CopySink::create(
                destination,
                command.format,
                command.csv,
                source_schema,
                sink_context.as_ref(),
                io,
            )
            .await?);
            let mut rows = 0_u64;
            while let Some(batch) = input.next().await {
                if let Err(error) = sink_context.check_cancelled() {
                    Err(sink.take().expect("COPY sink exists").abort(error).await)?;
                }
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => Err(sink.take().expect("COPY sink exists").abort(error).await)?,
                };
                rows = match rows.checked_add(
                    u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                ) {
                    Some(rows) => rows,
                    None => Err(sink
                        .take()
                        .expect("COPY sink exists")
                        .abort(Error::ResourceExhausted(
                            "COPY row count overflow".to_owned(),
                        ))
                        .await)?,
                };
                if let Err(error) = sink
                    .as_mut()
                    .expect("COPY sink exists")
                    .write(batch.batch().clone(), sink_context.as_ref())
                    .await
                {
                    Err(sink.take().expect("COPY sink exists").abort(error).await)?;
                }
                drop(batch);
            }
            if let Err(error) = sink_context.check_cancelled() {
                Err(sink.take().expect("COPY sink exists").abort(error).await)?;
            }
            let bytes = sink
                .take()
                .expect("COPY sink exists")
                .finish(sink_context.as_ref())
                .await?;
            sink_context.mark_copy_commit(std::path::PathBuf::from(&location));
            let status = copy_status(rows, bytes, &location)?;
            yield BatchEnvelope::try_new(status, &sink_context.memory, "COPY TO status")?;
        });
        let stream = self.engine.inner.compute.pipe(sink, Arc::clone(&context));
        Ok(query_result(
            result_schema,
            stream,
            context,
            permit,
            self.engine.clone(),
        ))
    }
}

fn copy_status(rows: u64, bytes: u64, location: &str) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("rows_copied", DataType::UInt64, false),
        Field::new("bytes_written", DataType::UInt64, false),
        Field::new("location", DataType::Utf8, false),
    ]));
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![rows])),
            Arc::new(UInt64Array::from(vec![bytes])),
            Arc::new(StringArray::from(vec![location])),
        ],
    )?)
}
