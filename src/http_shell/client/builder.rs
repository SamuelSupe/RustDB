use std::{fmt, time::Duration};

use reqwest::{Certificate, Client};
use url::Url;

use super::{RemoteClient, RemoteError, RemoteResult};
use crate::http_shell::security::ClientProfile;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct RemoteClientBuilder {
    base: Url,
    token: String,
    ca_pem: Vec<Vec<u8>>,
    connect_timeout: Duration,
    request_timeout: Duration,
    submit_attempts: usize,
}

impl fmt::Debug for RemoteClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteClientBuilder")
            .field("base", &self.base)
            .field("token", &"[REDACTED]")
            .field("ca_certificates", &self.ca_pem.len())
            .field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("submit_attempts", &self.submit_attempts)
            .finish()
    }
}

impl RemoteClientBuilder {
    pub fn new(base: Url, token: impl Into<String>) -> Self {
        Self {
            base,
            token: token.into(),
            ca_pem: Vec::new(),
            connect_timeout: CONNECT_TIMEOUT,
            request_timeout: REQUEST_TIMEOUT,
            submit_attempts: 2,
        }
    }

    pub fn from_profile(profile: &ClientProfile) -> RemoteResult<Self> {
        let ca = profile
            .read_ca()
            .map_err(|error| RemoteError::configuration(error.to_string()))?;
        let token = profile
            .read_token()
            .map_err(|error| RemoteError::configuration(error.to_string()))?;
        Ok(Self::new(profile.server_url().clone(), token).add_ca_certificate_pem(ca))
    }

    pub fn add_ca_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem.push(pem.into());
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn submit_attempts(mut self, attempts: usize) -> Self {
        self.submit_attempts = attempts;
        self
    }

    pub fn build(self) -> RemoteResult<RemoteClient> {
        if self.base.scheme() != "https" {
            return Err(RemoteError::configuration(
                "remote server URL must use https",
            ));
        }
        if self.token.is_empty() {
            return Err(RemoteError::configuration(
                "remote bearer token must not be empty",
            ));
        }
        if self.connect_timeout.is_zero() || self.request_timeout.is_zero() {
            return Err(RemoteError::configuration(
                "remote client timeouts must be greater than zero",
            ));
        }
        if self.submit_attempts == 0 {
            return Err(RemoteError::configuration(
                "remote submit attempts must be greater than zero",
            ));
        }

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certificates = self
            .ca_pem
            .iter()
            .map(|pem| {
                Certificate::from_pem(pem).map_err(|error| {
                    RemoteError::configuration(format!("invalid remote CA certificate: {error}"))
                })
            })
            .collect::<RemoteResult<Vec<_>>>()?;
        let client = Client::builder()
            .https_only(true)
            .tls_certs_only(certificates)
            .connect_timeout(self.connect_timeout)
            .timeout(self.request_timeout)
            .build()
            .map_err(RemoteError::transport)?;
        Ok(RemoteClient {
            client,
            base: self.base,
            token: self.token,
            submit_attempts: self.submit_attempts,
        })
    }
}
