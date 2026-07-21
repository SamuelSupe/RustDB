use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

use super::security::{SecurityState, ServerEndpoint, TlsMaterial};
use super::service_io::ServiceIoPool;

pub(super) struct TlsReloader {
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl TlsReloader {
    pub(super) fn start(
        interval: Duration,
        state: SecurityState,
        endpoint: ServerEndpoint,
        config: RustlsConfig,
        loaded_not_after: i64,
        io: ServiceIoPool,
    ) -> Result<Self> {
        if interval.is_zero() {
            return Err(Error::InvalidArgument(
                "server.tls_renew_interval_secs must be greater than zero".into(),
            ));
        }
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(async move {
            let mut loaded_not_after = loaded_not_after;
            let start = tokio::time::Instant::now() + interval;
            let mut ticker = tokio::time::interval_at(start, interval);
            loop {
                tokio::select! {
                    _ = task_stop.cancelled() => return,
                    _ = ticker.tick() => {}
                }
                let state_for_load = state.clone();
                let endpoint_for_load = endpoint.clone();
                let load = io.run_async(move || {
                    TlsMaterial::load_or_create(&state_for_load, &endpoint_for_load)
                });
                tokio::pin!(load);
                let material = tokio::select! {
                    _ = task_stop.cancelled() => return,
                    material = &mut load => material,
                };
                let material = match material {
                    Ok(material) => material,
                    Err(error) => {
                        tracing::error!(%error, "background TLS renewal failed");
                        continue;
                    }
                };
                if material.leaf_not_after_unix() == loaded_not_after {
                    continue;
                }
                let reload = config.reload_from_pem_file(
                    material.server_certificate_path(),
                    material.server_private_key_path(),
                );
                tokio::pin!(reload);
                let reloaded = tokio::select! {
                    _ = task_stop.cancelled() => return,
                    reloaded = &mut reload => reloaded,
                };
                match reloaded {
                    Ok(()) => {
                        loaded_not_after = material.leaf_not_after_unix();
                        tracing::info!(
                            leaf_not_after_unix = loaded_not_after,
                            "reloaded renewed HTTP TLS identity"
                        );
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to reload renewed HTTP TLS identity");
                    }
                }
            }
        });
        Ok(Self { stop, task })
    }

    pub(super) async fn stop(self) -> Result<()> {
        self.stop.cancel();
        self.task
            .await
            .map_err(|error| Error::Internal(format!("TLS renewal task panicked: {error}")))
    }
}
