use std::{io, io::SeekFrom, pin::Pin};

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use object_store::GetResultPayload;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    CsvCompression, Error, Result,
    runtime::{QueryControl, QueryMetrics},
    storage::{ObjectSnapshot, ObjectSource},
};

pub(super) type CsvInput = Pin<Box<dyn AsyncRead + Send>>;

const LOCAL_READ_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct QueryIo {
    control: QueryControl,
    metrics: QueryMetrics,
}

pub(super) async fn open_csv_input(
    file: &ObjectSource,
    snapshot: &ObjectSnapshot,
    compression: CsvCompression,
    query: Option<(&QueryControl, &QueryMetrics)>,
) -> Result<CsvInput> {
    let query = query.map(|(control, metrics)| QueryIo {
        control: control.clone(),
        metrics: metrics.clone(),
    });
    if let Some(query) = &query {
        query.control.check_cancelled()?;
        if file.is_s3() {
            query.metrics.add_s3_requests(1);
        }
    }

    let request = file
        .store()
        .get_opts(file.location(), file.get_options_for(snapshot));
    let get = match &query {
        Some(query) => tokio::select! {
            _ = query.control.cancelled() => Err(Error::Cancelled),
            result = request => result.map_err(|error| object_error(file.uri(), error)),
        },
        None => request
            .await
            .map_err(|error| object_error(file.uri(), error)),
    }?;
    snapshot.validate_get_response(file.uri(), &get.meta)?;

    let uri = file.uri().to_owned();
    let source_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes>> + Send>> = match get.payload
    {
        GetResultPayload::File(file, _) => {
            let mut file = tokio::fs::File::from_std(file);
            if get.range.start != 0 {
                file.seek(SeekFrom::Start(get.range.start))
                    .await
                    .map_err(|error| input_error(&uri, error))?;
            }
            let remaining = get.range.end.saturating_sub(get.range.start);
            let stream_uri = uri.clone();
            let stream = ReaderStream::with_capacity(file.take(remaining), LOCAL_READ_CHUNK_BYTES)
                .map(move |result| result.map_err(|error| input_error(&stream_uri, error)));
            Box::pin(stream)
        }
        GetResultPayload::Stream(stream) => {
            let stream_uri = uri.clone();
            Box::pin(
                stream.map(move |result| result.map_err(|error| object_error(&stream_uri, error))),
            )
        }
    };
    let s3 = file.is_s3();
    let stream = try_stream! {
        let mut source_stream = source_stream;
        loop {
            let next = match &query {
                Some(query) => tokio::select! {
                    _ = query.control.cancelled() => Err(Error::Cancelled),
                    next = source_stream.next() => Ok(next),
                }?,
                None => source_stream.next().await,
            };
            let Some(bytes) = next else { break };
            let bytes = bytes?;
            if let Some(query) = &query {
                let bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                query.metrics.add_csv_source_bytes(bytes);
                if s3 {
                    query.metrics.add_s3_bytes_transferred(bytes);
                }
            }
            yield bytes;
        }
    }
    .map(|result: Result<_>| {
        result.map_err(|error| match error {
            Error::Cancelled => io::Error::new(io::ErrorKind::Interrupted, "query cancelled"),
            error => io::Error::other(error.to_string()),
        })
    });
    let stream: Pin<Box<dyn Stream<Item = io::Result<bytes::Bytes>> + Send>> = Box::pin(stream);
    let mut reader = BufReader::new(StreamReader::new(stream));
    let detected = if compression == CsvCompression::Auto {
        detect(
            reader
                .fill_buf()
                .await
                .map_err(|error| input_error(&uri, error))?,
        )
    } else {
        compression
    };

    match detected {
        CsvCompression::Auto | CsvCompression::None => Ok(Box::pin(reader)),
        CsvCompression::Gzip => {
            let mut decoder = GzipDecoder::new(reader);
            decoder.multiple_members(true);
            Ok(Box::pin(decoder))
        }
        CsvCompression::Zstd => {
            let mut decoder = ZstdDecoder::new(reader);
            decoder.multiple_members(true);
            Ok(Box::pin(decoder))
        }
    }
}

fn detect(prefix: &[u8]) -> CsvCompression {
    if prefix.starts_with(&[0x1f, 0x8b]) {
        return CsvCompression::Gzip;
    }
    if prefix.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) || is_zstd_skippable(prefix) {
        return CsvCompression::Zstd;
    }
    CsvCompression::None
}

fn is_zstd_skippable(prefix: &[u8]) -> bool {
    let Some(magic) = prefix
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
    else {
        return false;
    };
    (0x184d_2a50..=0x184d_2a5f).contains(&magic)
}

fn object_error(uri: &str, error: object_store::Error) -> Error {
    if matches!(
        error,
        object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. }
    ) {
        Error::Execution(format!("object changed during query: {uri}: {error}"))
    } else {
        Error::Execution(format!("object read failed for {uri}: {error}"))
    }
}

pub(super) fn input_error(uri: &str, error: io::Error) -> Error {
    if error.kind() == io::ErrorKind::Interrupted {
        Error::Cancelled
    } else {
        Error::Execution(format!("CSV input failed for {uri}: {error}"))
    }
}

#[cfg(test)]
mod tests {
    use async_compression::tokio::write::GzipEncoder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{detect, open_csv_input};
    use crate::{
        CsvCompression, Error, S3Config,
        runtime::{MemoryPool, QueryContext},
        storage::LocationResolver,
    };

    #[test]
    fn detects_compression_from_magic_only() {
        assert_eq!(detect(&[0x1f, 0x8b, 0]), CsvCompression::Gzip);
        assert_eq!(detect(&[0x28, 0xb5, 0x2f, 0xfd]), CsvCompression::Zstd);
        assert_eq!(detect(b"id,name\n"), CsvCompression::None);
    }

    #[tokio::test]
    async fn cancellation_survives_the_compressed_io_adapter() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cancel.csv.gz");
        let mut state = 0x1234_5678_u32;
        let input = (0..2 * 1024 * 1024)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect::<Vec<_>>();
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(&input).await.unwrap();
        encoder.shutdown().await.unwrap();
        std::fs::write(&path, encoder.into_inner()).unwrap();

        let files = LocationResolver::new(S3Config::default())
            .resolve(&[path.to_string_lossy().into_owned()])
            .await
            .unwrap();
        let file = &files[0];
        let context =
            QueryContext::new(MemoryPool::new(4 * 1024 * 1024), directory.path()).unwrap();
        let mut reader = open_csv_input(
            file,
            file.snapshot(),
            CsvCompression::Auto,
            Some((&context.control, &context.metrics)),
        )
        .await
        .unwrap();
        context.cancel();
        let mut output = [0_u8; 4096];
        let error = loop {
            match reader.read(&mut output).await {
                Ok(0) => panic!("cancelled compressed input reached EOF"),
                Ok(_) => continue,
                Err(error) => break error,
            }
        };
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(matches!(
            super::input_error(file.uri(), error),
            Error::Cancelled
        ));
    }
}
