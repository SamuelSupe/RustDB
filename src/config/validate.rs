use crate::{EngineConfig, Error, Result};

impl EngineConfig {
    /// Validates the configuration without creating directories or starting runtimes.
    pub fn validate(&self) -> Result<()> {
        if self.memory_limit == 0 {
            return Err(Error::InvalidArgument(
                "memory_limit must be greater than zero".to_owned(),
            ));
        }
        if self.batch_size == 0 {
            return Err(Error::InvalidArgument(
                "batch_size must be greater than zero".to_owned(),
            ));
        }
        if self.compute_threads == 0 || self.io_concurrency == 0 {
            return Err(Error::InvalidArgument(
                "compute_threads and io_concurrency must be greater than zero".to_owned(),
            ));
        }
        if self.max_concurrent_queries == 0 {
            return Err(Error::InvalidArgument(
                "max_concurrent_queries must be greater than zero".to_owned(),
            ));
        }
        if self.s3.anonymous && self.s3.credential_provider.is_some() {
            return Err(Error::InvalidArgument(
                "S3 anonymous access and a credential provider are mutually exclusive".to_owned(),
            ));
        }
        if let Some(endpoint) = &self.s3.endpoint {
            crate::storage::validate_endpoint(endpoint, self.s3.allow_http)?;
        }
        self.csv_scan.validate()?;
        self.execution.validate()?;
        self.spill.validate()?;
        self.native_storage.validate()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{StaticCredentialProvider, aws::AwsCredential};

    use super::*;

    #[test]
    fn rejects_invalid_query_and_s3_settings() {
        let config = EngineConfig::builder().max_concurrent_queries(0).build();
        assert_error_contains(&config, "max_concurrent_queries");

        let s3 = crate::S3Config::default().endpoint("http://minio.example:9000");
        let mut config = EngineConfig::builder().s3(s3).build();
        assert_error_contains(&config, "allow_http is false");

        config.s3.allow_http = true;
        config.validate().unwrap();

        config.s3.anonymous = true;
        config.s3.credential_provider =
            Some(Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: "test-key".to_owned(),
                secret_key: "test-secret".to_owned(),
                token: None,
            })));
        assert_error_contains(&config, "mutually exclusive");
    }

    #[test]
    fn validation_does_not_create_the_spill_directory() {
        let root = tempfile::tempdir().unwrap();
        let spill = root.path().join("missing/parent/spill");
        let config = EngineConfig::builder().spill_directory(&spill).build();

        config.validate().unwrap();

        assert!(!spill.exists());
        assert!(!root.path().join("missing").exists());
    }

    fn assert_error_contains(config: &EngineConfig, expected: &str) {
        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}
