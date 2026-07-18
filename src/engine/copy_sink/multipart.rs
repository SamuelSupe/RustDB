use std::{
    future::Future,
    io::{self, Write},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::{StreamExt, stream::FuturesUnordered};
use object_store::{MultipartUpload, PutPayload, PutResult};
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};

use crate::{
    Error, Result,
    runtime::{MemoryPool, MemoryReservation, TaskGroup},
};

const PART_BYTES: usize = 8 << 20;

#[derive(Clone)]
pub(super) struct SharedMultipart {
    inner: Arc<ClientState>,
}

struct ClientState {
    sender: mpsc::UnboundedSender<Command>,
    buffer: Mutex<PartBuffer>,
    memory: MemoryPool,
    resource_error: Mutex<Option<String>>,
    terminal: AtomicBool,
    bytes: AtomicU64,
    sha256: Mutex<Sha256>,
}

struct PartBuffer {
    bytes: Vec<u8>,
    memory: MemoryReservation,
}

enum Command {
    Part {
        data: Bytes,
        memory: MemoryReservation,
    },
    Barrier(usize, oneshot::Sender<std::result::Result<(), String>>),
    Finish(oneshot::Sender<std::result::Result<PutResult, String>>),
    Abort(oneshot::Sender<std::result::Result<(), String>>),
}

impl SharedMultipart {
    pub(super) async fn create(
        upload: Box<dyn MultipartUpload>,
        tasks: &TaskGroup,
        memory: MemoryPool,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let upload = Arc::new(Mutex::new(Some(upload)));
        let actor_upload = Arc::clone(&upload);
        if let Err(error) = tasks.spawn("remote-copy-upload", async move {
            let mut upload = actor_upload.lock().take().ok_or_else(|| {
                Error::Internal("remote COPY multipart actor lost its upload".to_owned())
            })?;
            run_actor(&mut upload, receiver).await
        }) {
            let cleanup = upload.lock().take();
            if let Some(mut upload) = cleanup
                && let Err(cleanup) = upload.abort().await
            {
                return Err(Error::Execution(format!(
                    "{error}; remote COPY multipart startup cleanup failed: {cleanup}"
                )));
            }
            return Err(error);
        }
        Ok(Self {
            inner: Arc::new(ClientState {
                sender,
                buffer: Mutex::new(PartBuffer {
                    bytes: Vec::new(),
                    memory: memory.reservation(),
                }),
                memory,
                resource_error: Mutex::new(None),
                terminal: AtomicBool::new(false),
                bytes: AtomicU64::new(0),
                sha256: Mutex::new(Sha256::new()),
            }),
        })
    }

    pub(super) async fn wait_for_capacity(&self, max_concurrency: usize) -> Result<()> {
        if self.inner.terminal.load(Ordering::Acquire) {
            return Err(Error::Internal(
                "remote COPY multipart upload is already terminal".to_owned(),
            ));
        }
        let (reply, result) = oneshot::channel();
        self.inner
            .sender
            .send(Command::Barrier(max_concurrency, reply))
            .map_err(|_| actor_stopped())?;
        result
            .await
            .map_err(|_| actor_stopped())?
            .map_err(actor_error)
    }

    pub(super) async fn finish(&self) -> Result<PutResult> {
        self.flush_tail().map_err(|error| Error::io(None, error))?;
        if self.inner.terminal.swap(true, Ordering::AcqRel) {
            return Err(Error::Internal(
                "remote COPY multipart upload is already terminal".to_owned(),
            ));
        }
        let (reply, result) = oneshot::channel();
        self.inner
            .sender
            .send(Command::Finish(reply))
            .map_err(|_| actor_stopped())?;
        result
            .await
            .map_err(|_| actor_stopped())?
            .map_err(actor_error)
    }

    /// Abort is idempotent from the client's perspective. If a concurrent
    /// finish already made the actor terminal, deletion of the completed data
    /// object is handled by the caller.
    pub(super) async fn abort(&self) -> Result<()> {
        self.inner.terminal.store(true, Ordering::Release);
        {
            let mut buffer = self.inner.buffer.lock();
            buffer.bytes = Vec::new();
            buffer.memory = self.inner.memory.reservation();
        }
        let (reply, result) = oneshot::channel();
        if self.inner.sender.send(Command::Abort(reply)).is_err() {
            return Ok(());
        }
        result.await.unwrap_or(Ok(())).map_err(actor_error)
    }

    pub(super) fn bytes(&self) -> u64 {
        self.inner.bytes.load(Ordering::Relaxed)
    }

    pub(super) fn sha256(&self) -> String {
        format!("{:x}", self.inner.sha256.lock().clone().finalize())
    }

    pub(super) fn take_resource_error(&self) -> Option<Error> {
        self.inner
            .resource_error
            .lock()
            .take()
            .map(Error::ResourceExhausted)
    }

    fn flush_tail(&self) -> io::Result<()> {
        let tail = {
            let mut buffer = self.inner.buffer.lock();
            if buffer.bytes.is_empty() {
                None
            } else {
                let bytes = Bytes::from(std::mem::take(&mut buffer.bytes));
                let memory = std::mem::replace(&mut buffer.memory, self.inner.memory.reservation());
                Some((bytes, memory))
            }
        };
        if let Some((tail, memory)) = tail {
            self.send_part(tail, memory)?;
        }
        Ok(())
    }

    fn send_part(&self, data: Bytes, memory: MemoryReservation) -> io::Result<()> {
        self.inner
            .sender
            .send(Command::Part { data, memory })
            .map_err(|_| io::Error::other("remote COPY multipart actor stopped"))
    }

    fn reserve_buffer(&self, buffer: &mut PartBuffer) -> io::Result<()> {
        if buffer.bytes.capacity() != 0 {
            return Ok(());
        }
        let mut reservation = self.inner.memory.try_reserve(PART_BYTES).map_err(|error| {
            let message =
                format!("remote COPY multipart buffer requires {PART_BYTES} bytes: {error}");
            *self.inner.resource_error.lock() = Some(message.clone());
            io::Error::other(message)
        })?;
        let bytes = Vec::with_capacity(PART_BYTES);
        if bytes.capacity() > PART_BYTES
            && let Err(error) = reservation.try_grow(bytes.capacity() - PART_BYTES)
        {
            let message = format!(
                "remote COPY multipart buffer requires {} bytes: {error}",
                bytes.capacity()
            );
            *self.inner.resource_error.lock() = Some(message.clone());
            return Err(io::Error::other(message));
        }
        buffer.bytes = bytes;
        buffer.memory = reservation;
        Ok(())
    }
}

impl Write for SharedMultipart {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if self.inner.terminal.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "remote COPY multipart upload is already terminal",
            ));
        }
        let written = input.len();
        self.inner.sha256.lock().update(input);
        self.inner.bytes.fetch_add(
            u64::try_from(input.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        let mut input = input;
        while !input.is_empty() {
            let part = {
                let mut buffer = self.inner.buffer.lock();
                self.reserve_buffer(&mut buffer)?;
                let available = PART_BYTES.saturating_sub(buffer.bytes.len());
                let copied = available.min(input.len());
                buffer.bytes.extend_from_slice(&input[..copied]);
                input = &input[copied..];
                if buffer.bytes.len() == PART_BYTES {
                    let bytes = Bytes::from(std::mem::take(&mut buffer.bytes));
                    let memory =
                        std::mem::replace(&mut buffer.memory, self.inner.memory.reservation());
                    Some((bytes, memory))
                } else {
                    None
                }
            };
            if let Some((part, memory)) = part {
                self.send_part(part, memory)?;
            }
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

async fn run_actor(
    upload: &mut Box<dyn MultipartUpload>,
    mut receiver: mpsc::UnboundedReceiver<Command>,
) -> Result<()> {
    let mut pending = FuturesUnordered::<PendingPart>::new();
    loop {
        tokio::select! {
            result = pending.next(), if !pending.is_empty() => {
                if let Some((Err(error), _memory)) = result {
                    return abort_after_failure(upload, &mut pending, format!(
                        "remote COPY multipart part failed: {error}"
                    )).await;
                }
            }
            command = receiver.recv() => match command {
                Some(Command::Part { data, memory }) => {
                    let upload = upload.put_part(PutPayload::from(data));
                    pending.push(Box::pin(async move { (upload.await, memory) }));
                }
                Some(Command::Barrier(max, reply)) => {
                    if let Err(message) = wait_for_capacity(&mut pending, max).await {
                        let _ = reply.send(Err(message.clone()));
                        return abort_after_failure(upload, &mut pending, message).await;
                    }
                    let _ = reply.send(Ok(()));
                }
                Some(Command::Finish(reply)) => {
                    if let Err(message) = wait_for_capacity(&mut pending, 0).await {
                        let _ = reply.send(Err(message.clone()));
                        return abort_after_failure(upload, &mut pending, message).await;
                    }
                    match upload.complete().await {
                        Ok(result) => {
                            let _ = reply.send(Ok(result));
                            return Ok(());
                        }
                        Err(error) => {
                            let message = format!("remote COPY multipart completion failed: {error}");
                            let _ = reply.send(Err(message.clone()));
                            return abort_after_failure(upload, &mut pending, message).await;
                        }
                    }
                }
                Some(Command::Abort(reply)) => {
                    let old = std::mem::take(&mut pending);
                    drop(old);
                    let result = upload.abort().await.map_err(|error| error.to_string());
                    let _ = reply.send(result.clone());
                    return result.map_err(actor_error);
                }
                None => {
                    let old = std::mem::take(&mut pending);
                    drop(old);
                    return upload.abort().await.map_err(Error::from);
                }
            },
        }
    }
}

async fn wait_for_capacity(
    pending: &mut FuturesUnordered<PendingPart>,
    max_concurrency: usize,
) -> std::result::Result<(), String> {
    while !pending.is_empty() && (max_concurrency == 0 || pending.len() >= max_concurrency) {
        match pending.next().await {
            Some((Ok(()), _memory)) => {}
            Some((Err(error), _memory)) => {
                return Err(format!("remote COPY multipart part failed: {error}"));
            }
            None => break,
        }
    }
    Ok(())
}

async fn abort_after_failure(
    upload: &mut Box<dyn MultipartUpload>,
    pending: &mut FuturesUnordered<PendingPart>,
    message: String,
) -> Result<()> {
    let old = std::mem::take(pending);
    drop(old);
    match upload.abort().await {
        Ok(()) => Err(actor_error(message)),
        Err(cleanup) => Err(Error::Execution(format!(
            "{message}; multipart abort failed: {cleanup}"
        ))),
    }
}

type PendingPart =
    Pin<Box<dyn Future<Output = (object_store::Result<()>, MemoryReservation)> + Send + 'static>>;

fn actor_stopped() -> Error {
    Error::Execution("remote COPY multipart actor stopped before replying".to_owned())
}

fn actor_error(message: String) -> Error {
    Error::Execution(message)
}

#[cfg(test)]
#[path = "multipart/tests.rs"]
mod tests;
