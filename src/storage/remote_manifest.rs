use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt, path::Path};

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PublicationState {
    Matches,
    Absent,
    Different,
    Unavailable(String),
}

pub(crate) async fn inspect(
    store: &Arc<dyn ObjectStore>,
    path: &Path,
    expected: &[u8],
) -> PublicationState {
    let response = match store.get(path).await {
        Ok(response) => response,
        Err(object_store::Error::NotFound { .. }) => return PublicationState::Absent,
        Err(error) => return PublicationState::Unavailable(error.to_string()),
    };
    if response.meta.size != u64::try_from(expected.len()).unwrap_or(u64::MAX) {
        return PublicationState::Different;
    }
    match response.bytes().await {
        Ok(actual) if actual.as_ref() == expected => PublicationState::Matches,
        Ok(_) => PublicationState::Different,
        Err(error) => PublicationState::Unavailable(error.to_string()),
    }
}

pub(crate) fn is_definitive_rejection(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::AlreadyExists { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::PermissionDenied { .. }
            | object_store::Error::InvalidPath { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use super::{PublicationState, inspect};

    #[tokio::test]
    async fn distinguishes_matching_absent_and_foreign_manifests() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("export/manifest.json");
        assert_eq!(
            inspect(&store, &path, b"ours").await,
            PublicationState::Absent
        );

        store
            .put(&path, PutPayload::from(Bytes::from_static(b"ours")))
            .await
            .unwrap();
        assert_eq!(
            inspect(&store, &path, b"ours").await,
            PublicationState::Matches
        );
        assert_eq!(
            inspect(&store, &path, b"theirs").await,
            PublicationState::Different
        );
    }
}
