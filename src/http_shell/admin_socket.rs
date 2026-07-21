use std::{path::PathBuf, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

use super::{
    query::QueryManager,
    security::{Authenticator, PrincipalId, PrincipalStore, TokenId},
    service_io::ServiceIoPool,
};

const MAX_COMMAND_BYTES: usize = 64 * 1024;
const MAX_ADMIN_CONNECTIONS: usize = 8;
const ADMIN_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// One local administration command. The socket is protected by filesystem
/// permissions and is never exposed through the network listener.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum AdminCommand {
    Status {},
    ReloadTokens {},
    RotateToken { principal: String },
    RevokeToken { token_id: String },
    Shutdown {},
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[non_exhaustive]
pub struct AdminResponse {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone)]
pub(super) struct AdminContext {
    pub(super) queries: QueryManager,
    pub(super) authenticator: Authenticator,
    pub(super) principals: Option<PrincipalStore>,
    pub(super) shutdown: CancellationToken,
    pub(super) mutations: Arc<Mutex<()>>,
    pub(super) io: ServiceIoPool,
}

pub(super) struct AdminSocket {
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl AdminSocket {
    pub(super) async fn start(path: PathBuf, context: AdminContext) -> Result<Self> {
        prepare_socket_path(&path)?;
        let listener = UnixListener::bind(&path).map_err(|error| Error::io(path.clone(), error))?;
        if let Err(error) = set_socket_permissions(&path) {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            let _cleanup = SocketCleanup(path.clone());
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = task_stop.cancelled() => {
                        connections.abort_all();
                        while connections.join_next().await.is_some() {}
                        return Ok(());
                    }
                    accepted = listener.accept() => {
                        let (stream, _) = accepted.map_err(|error| Error::io(path.clone(), error))?;
                        if connections.len() >= MAX_ADMIN_CONNECTIONS {
                            tracing::warn!("local admin connection limit reached");
                            drop(stream);
                            continue;
                        }
                        let context = context.clone();
                        connections.spawn(async move {
                            if let Err(error) = serve_connection(stream, context).await {
                                tracing::warn!(%error, "local admin connection failed");
                            }
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Ok(Self { stop, task })
    }

    pub(super) async fn stop(self) -> Result<()> {
        self.stop.cancel();
        self.task
            .await
            .map_err(|error| Error::Internal(format!("local admin task panicked: {error}")))?
    }
}

/// Sends one command to a running local server.
pub async fn send_admin_command(
    path: impl Into<PathBuf>,
    command: AdminCommand,
) -> Result<AdminResponse> {
    let path = path.into();
    let mut stream = UnixStream::connect(&path)
        .await
        .map_err(|error| Error::io(path.clone(), error))?;
    let mut encoded = serde_json::to_vec(&command)
        .map_err(|error| Error::Internal(format!("failed to encode admin command: {error}")))?;
    encoded.push(b'\n');
    stream
        .write_all(&encoded)
        .await
        .map_err(|error| Error::io(path.clone(), error))?;
    let mut reader = BufReader::new(stream);
    let Some(response) = read_bounded_line(&mut reader, Some(path.clone())).await? else {
        return Err(Error::InvalidArgument(
            "local admin response is empty".into(),
        ));
    };
    serde_json::from_slice(&response)
        .map_err(|error| Error::InvalidArgument(format!("invalid local admin response: {error}")))
}

async fn serve_connection(stream: UnixStream, context: AdminContext) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let Some(line) = read_bounded_line(&mut reader, None).await? else {
            return Ok(());
        };
        let (response, shutdown) = match serde_json::from_slice::<AdminCommand>(&line) {
            Ok(command) => {
                let shutdown = matches!(&command, AdminCommand::Shutdown {});
                (execute(command, &context).await, shutdown)
            }
            Err(error) => (
                AdminResponse::error(format!("invalid admin command: {error}")),
                false,
            ),
        };
        let mut encoded = serde_json::to_vec(&response).map_err(|error| {
            Error::Internal(format!("failed to encode admin response: {error}"))
        })?;
        encoded.push(b'\n');
        writer
            .write_all(&encoded)
            .await
            .map_err(|error| Error::io(None, error))?;
        if shutdown && response.ok {
            context.shutdown.cancel();
            return Ok(());
        }
    }
}

async fn read_bounded_line<R>(
    reader: &mut BufReader<R>,
    path: Option<PathBuf>,
) -> Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    read_bounded_line_with_timeout(reader, path, ADMIN_READ_IDLE_TIMEOUT).await
}

async fn read_bounded_line_with_timeout<R>(
    reader: &mut BufReader<R>,
    path: Option<PathBuf>,
    idle_timeout: Duration,
) -> Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        let available = tokio::time::timeout(idle_timeout, reader.fill_buf())
            .await
            .map_err(|_| Error::ResourceExhausted("local admin JSON line read timed out".into()))?
            .map_err(|error| Error::io(path.clone(), error))?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err(Error::InvalidArgument(
                    "local admin JSON line is missing a newline terminator".into(),
                ))
            };
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if line.len().saturating_add(take) > MAX_COMMAND_BYTES {
            return Err(Error::ResourceExhausted(
                "local admin JSON line exceeds 64 KiB".into(),
            ));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
        }
    }
}

async fn execute(command: AdminCommand, context: &AdminContext) -> AdminResponse {
    match command {
        AdminCommand::Status {} => {
            let (queued, running, terminal) = context.queries.admin_counts();
            AdminResponse::ok(json!({
                "version": env!("CARGO_PKG_VERSION"),
                "queued_queries": queued,
                "running_queries": running,
                "terminal_queries": terminal,
            }))
        }
        AdminCommand::Shutdown {} => AdminResponse::ok(json!({ "shutdown": "requested" })),
        AdminCommand::ReloadTokens {} => {
            mutate(context, |store, authenticator| {
                store.reload_into(authenticator)?;
                Ok(json!({ "tokens": "reloaded" }))
            })
            .await
        }
        AdminCommand::RotateToken { principal } => {
            mutate(context, move |store, authenticator| {
                let principal = PrincipalId::new(principal)?;
                let provision = store.rotate_token(&principal)?;
                store.reload_into(authenticator)?;
                Ok(json!({
                    "principal": principal.as_str(),
                    "token_id": provision.token_id().as_str(),
                    "token_path": provision.token_path(),
                }))
            })
            .await
        }
        AdminCommand::RevokeToken { token_id } => {
            mutate(context, move |store, authenticator| {
                let token_id = TokenId::new(token_id)?;
                store.revoke_token_and_reload(&token_id, authenticator)?;
                Ok(json!({ "token_id": token_id.as_str(), "revoked": true }))
            })
            .await
        }
    }
}

async fn mutate(
    context: &AdminContext,
    operation: impl FnOnce(&PrincipalStore, &Authenticator) -> Result<Value> + Send + 'static,
) -> AdminResponse {
    let _guard = context.mutations.lock().await;
    let Some(store) = context.principals.clone() else {
        return AdminResponse::error("authentication is disabled");
    };
    let authenticator = context.authenticator.clone();
    match context
        .io
        .run_async(move || operation(&store, &authenticator))
        .await
    {
        Ok(value) => AdminResponse::ok(value),
        Err(error) => AdminResponse::error(error.to_string()),
    }
}

impl AdminResponse {
    fn ok(data: Value) -> Self {
        Self {
            ok: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: None,
            error: Some(error.into()),
        }
    }
}

fn prepare_socket_path(path: &std::path::Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::InvalidArgument("admin socket path has no parent".into()))?;
    check_private_directory(parent)?;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if !metadata.file_type().is_socket() {
                return Err(Error::InvalidArgument(format!(
                    "refusing to replace non-socket admin path {}",
                    path.display()
                )));
            }
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => Err(Error::InvalidArgument(format!(
                    "an active local admin socket already exists at {}",
                    path.display()
                ))),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path).map_err(|error| Error::io(path.to_owned(), error))
                }
                Err(error) => Err(Error::io(path.to_owned(), error)),
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(path.to_owned(), error)),
    }
}

fn set_socket_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|error| Error::io(path.to_owned(), error))
}

fn check_private_directory(path: &std::path::Path) -> Result<()> {
    let metadata =
        std::fs::symlink_metadata(path).map_err(|error| Error::io(path.to_owned(), error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::InvalidArgument(format!(
            "admin socket parent is not a real directory: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::InvalidArgument(format!(
                "admin socket parent must be private: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        use std::os::unix::fs::FileTypeExt;
        let removable = std::fs::symlink_metadata(&self.0)
            .is_ok_and(|metadata| metadata.file_type().is_socket());
        if removable
            && let Err(error) = std::fs::remove_file(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::error!(%error, path = %self.0.display(), "failed to remove local admin socket");
        }
    }
}

#[cfg(test)]
mod tests;
