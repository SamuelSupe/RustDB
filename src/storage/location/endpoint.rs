use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use url::Url;

use crate::{Error, Result};

pub(super) fn builder_from_env(
    explicit_endpoint: Option<&str>,
    allow_http: bool,
) -> Result<AmazonS3Builder> {
    let environment = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    builder_from_env_snapshot(environment, explicit_endpoint, allow_http)
}

fn builder_from_env_snapshot(
    environment: impl IntoIterator<Item = (String, String)>,
    explicit_endpoint: Option<&str>,
    allow_http: bool,
) -> Result<AmazonS3Builder> {
    let mut builder = AmazonS3Builder::new();
    let mut generic_endpoint = None;
    let mut generic_endpoint_url = None;
    let mut s3_endpoint = None;

    for (key, value) in environment {
        if !key.starts_with("AWS_") {
            continue;
        }
        let normalized = key.to_ascii_lowercase();
        let Ok(config_key) = normalized.parse::<AmazonS3ConfigKey>() else {
            continue;
        };
        match config_key {
            AmazonS3ConfigKey::Endpoint if normalized == "aws_endpoint_url" => {
                generic_endpoint_url = Some(value);
            }
            AmazonS3ConfigKey::Endpoint => generic_endpoint = Some(value),
            AmazonS3ConfigKey::S3Endpoint => s3_endpoint = Some(value),
            _ => builder = builder.with_config(config_key, value),
        }
    }

    let endpoint = explicit_endpoint
        .map(ToOwned::to_owned)
        .or(s3_endpoint)
        .or(generic_endpoint_url)
        .or(generic_endpoint);
    if let Some(endpoint) = endpoint {
        validate_endpoint(&endpoint, allow_http)?;
        builder = builder.with_endpoint(endpoint);
    }
    Ok(builder)
}

pub(crate) fn validate_endpoint(endpoint: &str, allow_http: bool) -> Result<()> {
    let url = Url::parse(endpoint)
        .map_err(|error| Error::InvalidArgument(format!("invalid S3 endpoint: {error}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidArgument(
            "S3 endpoint must not contain credentials; use a credential provider".to_owned(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::InvalidArgument(
            "S3 endpoint must not contain a query or fragment".to_owned(),
        ));
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if allow_http => Ok(()),
        "http" => Err(Error::InvalidArgument(
            "S3 endpoint uses HTTP but allow_http is false".to_owned(),
        )),
        _ => Err(Error::InvalidArgument(
            "unsupported S3 endpoint scheme".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use object_store::aws::AmazonS3ConfigKey;

    use super::{builder_from_env_snapshot, validate_endpoint};

    fn environment(values: &[(&str, &str)]) -> Vec<(String, String)> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn explicit_endpoint_overrides_all_environment_endpoints() {
        let env = environment(&[
            ("AWS_ENDPOINT", "https://generic.test?token=secret-one"),
            ("AWS_ENDPOINT_URL", "https://generic-url.test#secret-two"),
            (
                "AWS_ENDPOINT_URL_S3",
                "https://service.test?token=secret-three",
            ),
        ]);
        let builder = builder_from_env_snapshot(env, Some("https://explicit.test"), false).unwrap();
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("https://explicit.test".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::S3Endpoint),
            None
        );
    }

    #[test]
    fn explicit_endpoint_is_validated_after_overriding_environment() {
        let env = environment(&[("AWS_ENDPOINT_URL_S3", "https://service.test")]);
        let error =
            builder_from_env_snapshot(env, Some("https://explicit.test?token=super-secret"), false)
                .unwrap_err()
                .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn service_endpoint_precedes_generic_endpoints() {
        let env = environment(&[
            ("AWS_ENDPOINT", "https://generic.test"),
            ("AWS_ENDPOINT_URL", "https://generic-url.test"),
            (
                "AWS_ENDPOINT_URL_S3",
                "https://service.test?token=super-secret",
            ),
        ]);
        let error = builder_from_env_snapshot(env, None, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn generic_endpoint_url_precedes_legacy_generic_endpoint() {
        let env = environment(&[
            ("AWS_ENDPOINT", "https://legacy.test"),
            ("AWS_ENDPOINT_URL", "https://generic.test#super-secret"),
        ]);
        let error = builder_from_env_snapshot(env, None, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn validates_legal_environment_endpoint() {
        let env = environment(&[("AWS_ENDPOINT_URL_S3", "http://minio:9000")]);
        let builder = builder_from_env_snapshot(env, None, true).unwrap();
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Endpoint),
            Some("http://minio:9000".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::S3Endpoint),
            None
        );
    }

    #[test]
    fn preserves_region_precedence_and_static_credentials() {
        let env = environment(&[
            ("AWS_DEFAULT_REGION", "us-east-1"),
            ("AWS_REGION", "eu-west-1"),
            ("AWS_ACCESS_KEY_ID", "test-access-key"),
            ("AWS_SECRET_ACCESS_KEY", "test-secret-key"),
            ("AWS_SESSION_TOKEN", "test-session-token"),
        ]);
        let builder = builder_from_env_snapshot(env, None, false).unwrap();

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Region),
            Some("eu-west-1".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::AccessKeyId),
            Some("test-access-key".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey),
            Some("test-secret-key".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::Token),
            Some("test-session-token".to_owned())
        );

        let reversed = environment(&[
            ("AWS_REGION", "eu-west-1"),
            ("AWS_DEFAULT_REGION", "us-east-1"),
        ]);
        let builder = builder_from_env_snapshot(reversed, None, false).unwrap();
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::DefaultRegion),
            Some("eu-west-1".to_owned())
        );
    }

    #[test]
    fn preserves_external_credential_provider_settings() {
        let env = environment(&[
            ("AWS_WEB_IDENTITY_TOKEN_FILE", "/run/secrets/aws-token"),
            ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/rustdb"),
            (
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                "/v2/credentials/example",
            ),
        ]);
        let builder = builder_from_env_snapshot(env, None, false).unwrap();

        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::WebIdentityTokenFile),
            Some("/run/secrets/aws-token".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::RoleArn),
            Some("arn:aws:iam::123456789012:role/rustdb".to_owned())
        );
        assert_eq!(
            builder.get_config_value(&AmazonS3ConfigKey::ContainerCredentialsRelativeUri),
            Some("/v2/credentials/example".to_owned())
        );
    }

    #[test]
    fn mixed_case_endpoint_alias_cannot_bypass_validation() {
        let env = environment(&[(
            "AWS_endpoint_url_s3",
            "https://service.test?token=super-secret",
        )]);
        let error = builder_from_env_snapshot(env, None, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn rejects_and_redacts_endpoint_userinfo() {
        let error = validate_endpoint("https://alice:super-secret@example.test", false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain credentials"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn rejects_and_redacts_endpoint_query() {
        let error = validate_endpoint("https://example.test?token=super-secret", false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn rejects_and_redacts_endpoint_fragment() {
        let error = validate_endpoint("https://example.test#super-secret", false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("must not contain a query or fragment"));
        assert!(!error.contains("super-secret"));
    }

    #[test]
    fn accepts_endpoints_without_ambiguous_url_components() {
        validate_endpoint("https://example.test", false).unwrap();
        validate_endpoint("http://minio:9000", true).unwrap();
    }

    #[test]
    fn rejects_insecure_endpoint_by_default() {
        let error = validate_endpoint("http://minio:9000", false).unwrap_err();
        assert!(error.to_string().contains("allow_http is false"));
    }
}
