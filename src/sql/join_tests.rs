use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::{
    Catalog, Error, Result, TableEntry,
    datasource::{ScanRequest, TableProvider, TableStatistics},
    runtime::{QueryContext, RecordBatchStream},
};

use super::{StatementPlan, plan_sql};

#[test]
fn using_preserves_side_types_and_selects_the_join_specific_merged_type() {
    let catalog = Catalog::default();
    register_schema(&catalog, "l", DataType::Int32);
    register_schema(&catalog, "r", DataType::Int64);
    for (join, merged) in [
        ("INNER JOIN", DataType::Int32),
        ("LEFT JOIN", DataType::Int32),
        ("RIGHT JOIN", DataType::Int64),
        ("FULL OUTER JOIN", DataType::Int64),
    ] {
        let sql = format!("SELECT id, l.id AS lid, r.id AS rid FROM l {join} r USING (id)");
        let StatementPlan::Query(plan) = plan_sql(&catalog, &sql).unwrap() else {
            panic!("expected query plan");
        };
        let fields = plan.schema().arrow().fields();
        assert_eq!(fields[0].data_type(), &merged, "{join} merged key");
        assert_eq!(fields[1].data_type(), &DataType::Int32, "{join} left key");
        assert_eq!(fields[2].data_type(), &DataType::Int64, "{join} right key");
    }
}

fn register_schema(catalog: &Catalog, name: &str, data_type: DataType) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", data_type, false)]));
    catalog
        .register(TableEntry::new(name, Arc::new(SchemaTable(schema))))
        .unwrap();
}

struct SchemaTable(SchemaRef);

#[async_trait]
impl TableProvider for SchemaTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn statistics(&self) -> TableStatistics {
        TableStatistics::default()
    }

    async fn scan(
        &self,
        _request: ScanRequest,
        _context: Arc<QueryContext>,
    ) -> Result<RecordBatchStream> {
        Err(Error::Internal(
            "schema-only test table was executed".into(),
        ))
    }
}
