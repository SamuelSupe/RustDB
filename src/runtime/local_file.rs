use std::{collections::HashMap, fs::File, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::OnceCell;

#[derive(Default)]
pub(crate) struct QueryLocalFileHandle {
    pub(crate) file: OnceCell<Arc<File>>,
}

#[derive(Default)]
pub(crate) struct QueryLocalFiles {
    handles: Mutex<HashMap<String, Arc<QueryLocalFileHandle>>>,
}

impl QueryLocalFiles {
    pub(crate) fn handle(&self, uri: &str) -> Arc<QueryLocalFileHandle> {
        let mut handles = self.handles.lock();
        Arc::clone(
            handles
                .entry(uri.to_owned())
                .or_insert_with(|| Arc::new(QueryLocalFileHandle::default())),
        )
    }

    pub(crate) fn clear(&self) {
        self.handles.lock().clear();
    }
}
