use std::sync::Arc;

use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

use super::{NativeSidecarMorsel, ParquetFilePlan};
use crate::{
    Error, Result,
    datasource::native::SidecarProjectionExecution,
    runtime::{BatchEnvelope, MemoryReservation, QueryContext},
};

pub(super) enum SidecarDecode {
    Projected(Vec<BatchEnvelope>),
    Empty,
    Parquet {
        row_selection: Option<RowSelection>,
        apply_row_filter: bool,
        leases: Vec<MemoryReservation>,
    },
}

pub(super) async fn decode(
    mode: NativeSidecarMorsel,
    file: &Arc<ParquetFilePlan>,
    row_groups: &[usize],
    row_selection: Option<RowSelection>,
    max_output_rows: usize,
    context: &QueryContext,
) -> Result<SidecarDecode> {
    if let Some(candidate) = &mode.projection {
        let parquet_metadata = file.metadata().reader_metadata().metadata();
        let parquet_row_groups = row_groups
            .iter()
            .map(|row_group| {
                if *row_group >= parquet_metadata.num_row_groups() {
                    return Err(Error::Internal(format!(
                        "Native sidecar planned invalid Parquet row group {row_group}"
                    )));
                }
                Ok(parquet_metadata.row_group(*row_group))
            })
            .collect::<Result<Vec<_>>>()?;
        match mode
            .sidecar
            .try_project_chunk(
                candidate,
                row_groups,
                &parquet_row_groups,
                row_selection.as_ref(),
                max_output_rows,
                context,
            )
            .await?
        {
            SidecarProjectionExecution::Projected(batches) => {
                // A full projection does not otherwise touch the data segment.
                // Keep Native's query-snapshot contract by validating the
                // segment identity after the companion has been decoded.
                validate_data_snapshot(file).await?;
                return if batches.is_empty() {
                    Ok(SidecarDecode::Empty)
                } else {
                    Ok(SidecarDecode::Projected(batches))
                };
            }
            SidecarProjectionExecution::Fallback => {}
        }
    }

    let [row_group] = row_groups else {
        return Ok(parquet(row_selection, true, Vec::new()));
    };
    let metadata = file.metadata().reader_metadata().metadata();
    let parquet_row_group = metadata.row_group(*row_group);
    let row_count = usize::try_from(parquet_row_group.num_rows()).map_err(|_| {
        Error::Execution(format!(
            "Parquet row group {row_group} in {} has an invalid row count",
            file.file().uri()
        ))
    })?;
    let Some(sidecar) = mode
        .sidecar
        .try_select(
            *row_group,
            row_count,
            &mode.predicate,
            &mode.table_schema,
            file.metadata().reader_metadata().schema(),
            parquet_row_group,
            file.projection(),
            context,
        )
        .await?
    else {
        return Ok(parquet(row_selection, true, Vec::new()));
    };
    if sidecar.selected_rows == 0 {
        validate_data_snapshot(file).await?;
        return Ok(SidecarDecode::Empty);
    }
    let combined = match row_selection {
        Some(page_selection) => page_selection.intersection(&sidecar.selection),
        None => sidecar.selection,
    };
    if selected_rows(&combined) == 0 {
        validate_data_snapshot(file).await?;
        return Ok(SidecarDecode::Empty);
    }
    Ok(parquet(combined.into(), false, vec![sidecar.lease]))
}

async fn validate_data_snapshot(file: &Arc<ParquetFilePlan>) -> Result<()> {
    file.reader().query_range(0..1).await?;
    Ok(())
}

fn parquet(
    row_selection: Option<RowSelection>,
    apply_row_filter: bool,
    leases: Vec<MemoryReservation>,
) -> SidecarDecode {
    SidecarDecode::Parquet {
        row_selection,
        apply_row_filter,
        leases,
    }
}

fn selected_rows(selection: &RowSelection) -> usize {
    Vec::<RowSelector>::from(selection.clone())
        .into_iter()
        .filter(|selector| !selector.skip)
        .map(|selector| selector.row_count)
        .sum()
}
