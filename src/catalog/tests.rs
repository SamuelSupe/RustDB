use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::stream;

use super::{Catalog, PersistentCatalog, TableEntry, normalize};
use crate::{
    Result,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    runtime::{QueryContext, RecordBatchStream, boxed_record_batch_stream},
};

#[test]
fn catalog_keys_are_ascii_case_insensitive() {
    assert_eq!(normalize("Orders"), "orders");
}

#[test]
fn dropping_unknown_view_does_not_remove_tables() {
    let catalog = Catalog::default();
    assert!(!catalog.drop_view("external"));
    assert!(!catalog.is_view("external"));
}

#[test]
fn sessions_share_persistent_tables_but_keep_local_entries_isolated() {
    let persistent = PersistentCatalog::new(7, [entry("shared", "persistent")]).unwrap();
    let first = Catalog::with_persistent(persistent.clone());
    let second = Catalog::with_persistent(persistent);

    first.register(entry("external", "first-only")).unwrap();
    first
        .register_view(entry("temp_view", "view"), "SELECT 1", false)
        .unwrap();

    assert_eq!(marker(&first, "shared"), "persistent");
    assert_eq!(marker(&second, "shared"), "persistent");
    assert_eq!(marker(&first, "external"), "first-only");
    assert!(second.table("external").is_none());
    assert!(first.is_view("temp_view"));
    assert!(!second.is_view("temp_view"));
    assert!(second.table("temp_view").is_none());
}

#[test]
fn local_entries_shadow_persistent_tables_and_names_are_deduplicated() {
    let persistent = PersistentCatalog::new(
        1,
        [entry("Orders", "persistent"), entry("lineitem", "lineitem")],
    )
    .unwrap();
    let catalog = Catalog::with_persistent(persistent);
    catalog.register(entry("orders", "local")).unwrap();

    assert_eq!(marker(&catalog, "ORDERS"), "local");
    assert_eq!(catalog.table_names(), vec!["lineitem", "orders"]);
}

#[test]
fn pinned_catalog_keeps_its_generation_while_live_sessions_advance() {
    let persistent = PersistentCatalog::new(3, [entry("items", "old")]).unwrap();
    let first = Catalog::with_persistent(persistent.clone());
    let second = Catalog::with_persistent(persistent.clone());
    let pinned = first.pin();

    assert_eq!(persistent.generation(), 3);
    assert_eq!(persistent.publish(3, [entry("items", "new")]).unwrap(), 4);
    assert_eq!(pinned.persistent_generation(), Some(3));
    assert_eq!(first.persistent_generation(), Some(4));
    assert_eq!(second.persistent_generation(), Some(4));
    assert_eq!(marker(&pinned, "items"), "old");
    assert_eq!(marker(&first, "items"), "new");
}

#[test]
fn pinned_catalog_keeps_a_stable_local_snapshot() {
    let catalog = Catalog::default();
    catalog.register(entry("before", "old")).unwrap();
    let pinned = catalog.pin();

    catalog.unregister("before");
    catalog.register(entry("after", "new")).unwrap();

    assert_eq!(marker(&pinned, "before"), "old");
    assert!(pinned.table("after").is_none());
    assert!(catalog.table("before").is_none());
    assert_eq!(marker(&catalog, "after"), "new");
}

#[test]
fn pinned_catalog_keeps_table_and_view_identity_together() {
    let catalog = Catalog::default();
    catalog
        .register_view(entry("item", "view"), "SELECT 1", false)
        .unwrap();
    let pinned = catalog.pin();

    catalog.register(entry("item", "table")).unwrap();

    assert!(pinned.is_view("item"));
    assert_eq!(marker(&pinned, "item"), "view");
    assert!(!catalog.is_view("item"));
    assert_eq!(marker(&catalog, "item"), "table");
}

#[test]
fn stale_persistent_publish_is_rejected_without_changing_visibility() {
    let persistent = PersistentCatalog::default();
    persistent.publish(0, [entry("first", "first")]).unwrap();

    let error = persistent
        .publish(0, [entry("stale", "stale")])
        .unwrap_err();
    assert!(error.to_string().contains("expected generation 0, found 1"));
    let catalog = Catalog::with_persistent(persistent);
    assert!(catalog.table("first").is_some());
    assert!(catalog.table("stale").is_none());
}

fn entry(name: &str, marker: &str) -> TableEntry {
    TableEntry::new(name, Arc::new(TestTable::new(marker)))
}

fn marker(catalog: &Catalog, name: &str) -> String {
    catalog
        .table(name)
        .unwrap()
        .provider()
        .schema()
        .field(0)
        .name()
        .to_owned()
}

struct TestTable {
    schema: SchemaRef,
}

impl TestTable {
    fn new(marker: &str) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![Field::new(
                marker,
                DataType::Int64,
                false,
            )])),
        }
    }
}

#[async_trait]
impl TableProvider for TestTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        Ok(boxed_record_batch_stream(stream::empty::<
            Result<RecordBatch>,
        >()))
    }
}
