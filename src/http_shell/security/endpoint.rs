use std::net::SocketAddr;

use url::{Host, Url};

use crate::{Error, Result};

/// Public listener identity and the host placed in the server certificate SAN.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerEndpoint {
    pub(super) public_url: Url,
    pub(super) certificate_host: String,
}

impl ServerEndpoint {
    /// Resolves the public endpoint. Non-loopback listeners must provide an
    /// explicit HTTPS advertise URL; wildcard addresses are never advertised.
    pub fn resolve(listen: SocketAddr, advertise_url: Option<&str>) -> Result<Self> {
        let public_url = match advertise_url {
            Some(value) => Url::parse(value).map_err(|error| {
                Error::InvalidArgument(format!("invalid advertise URL: {error}"))
            })?,
            None if listen.ip().is_loopback() => Url::parse(&format!("https://{listen}"))
                .map_err(|error| Error::Internal(format!("failed to form listen URL: {error}")))?,
            None => {
                return Err(Error::InvalidArgument(
                    "a non-loopback listener requires an explicit --advertise-url".to_owned(),
                ));
            }
        };
        validate_advertise_url(&public_url)?;
        let certificate_host = certificate_host(&public_url);
        if unspecified_host(&public_url)
            || (!listen.ip().is_loopback() && loopback_host(&public_url))
        {
            return Err(Error::InvalidArgument(
                "advertise URL for a non-loopback listener must use a routable host".to_owned(),
            ));
        }
        Ok(Self {
            public_url,
            certificate_host,
        })
    }

    pub fn public_url(&self) -> &Url {
        &self.public_url
    }

    pub fn certificate_host(&self) -> &str {
        &self.certificate_host
    }
}

fn validate_advertise_url(url: &Url) -> Result<()> {
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(Error::InvalidArgument(
            "advertise URL must be an HTTPS origin without credentials, path, query, or fragment"
                .to_owned(),
        ));
    }
    Ok(())
}

fn certificate_host(url: &Url) -> String {
    match url.host().expect("validated URL has a host") {
        Host::Ipv4(address) => address.to_string(),
        Host::Ipv6(address) => address.to_string(),
        Host::Domain(name) => name.to_owned(),
    }
}

fn unspecified_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_unspecified(),
        Some(Host::Ipv6(address)) => address.is_unspecified(),
        Some(Host::Domain(_)) => false,
        None => true,
    }
}

fn loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(name)) => name.eq_ignore_ascii_case("localhost"),
        None => true,
    }
}
