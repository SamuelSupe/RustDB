use url::Url;

use crate::{Error, Result};

#[derive(Debug)]
pub(super) struct S3Uri {
    bucket: String,
    path: String,
}

impl S3Uri {
    pub(super) fn parse(value: &str) -> Result<Self> {
        // Keep parse failures deliberately generic: the input can contain an
        // accidentally embedded access token or password.
        let url =
            Url::parse(value).map_err(|_| Error::InvalidArgument("invalid S3 URI".to_owned()))?;
        if url.scheme() != "s3" {
            return Err(Error::InvalidArgument(
                "S3 URI must use the s3:// scheme".to_owned(),
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            return Err(Error::InvalidArgument(
                "S3 URI must not contain user information; use a credential provider".to_owned(),
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(Error::InvalidArgument(
                "S3 URI must not contain a query or fragment".to_owned(),
            ));
        }
        let bucket = url
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| Error::InvalidArgument("S3 URI has no bucket".to_owned()))?;
        Ok(Self {
            bucket: bucket.to_owned(),
            path: url.path().to_owned(),
        })
    }

    pub(super) fn bucket(&self) -> &str {
        &self.bucket
    }

    pub(super) fn path(&self) -> &str {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::S3Uri;

    #[test]
    fn rejects_s3_uri_secrets_without_echoing_them() {
        for (uri, secret) in [
            ("s3://alice:password-123@bucket/key", "password-123"),
            ("s3://bucket/key?token=query-secret", "query-secret"),
            ("s3://bucket/key#fragment-secret", "fragment-secret"),
            ("s3://alice:malformed-secret@", "malformed-secret"),
        ] {
            let message = S3Uri::parse(uri).unwrap_err().to_string();
            assert!(!message.contains(secret), "{message}");
            assert!(!message.contains(uri), "{message}");
        }
    }

    #[test]
    fn returns_bucket_and_encoded_path() {
        let uri = S3Uri::parse("s3://bucket/folder/data%20file.parquet").unwrap();
        assert_eq!(uri.bucket(), "bucket");
        assert_eq!(uri.path(), "/folder/data%20file.parquet");
    }
}
