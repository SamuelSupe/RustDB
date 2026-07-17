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

#[test]
fn exposes_left_semi_and_anti_joins_with_left_only_schema() {
    let catalog = Catalog::default();
    register_schema(&catalog, "l", DataType::Int64);
    register_schema(&catalog, "r", DataType::Int64);
    for (join, explain_name) in [
        ("LEFT SEMI JOIN", "SemiJoin"),
        ("LEFT ANTI JOIN", "AntiJoin"),
    ] {
        let sql = format!("SELECT l.id FROM l {join} r ON l.id = r.id");
        let StatementPlan::Query(plan) = plan_sql(&catalog, &sql).unwrap() else {
            panic!("expected query plan");
        };
        assert_eq!(plan.schema().arrow().fields().len(), 1, "{join}");
        assert!(
            plan.explain().contains(explain_name),
            "{join}: {}",
            plan.explain()
        );
    }
}

#[test]
fn keeps_an_unhashable_wide_decimal_equality_as_a_residual() {
    let catalog = wide_decimal_catalog(true);
    let StatementPlan::Query(plan) = plan_sql(
        &catalog,
        "SELECT l.id FROM l JOIN r ON l.id = r.id AND l.amount = r.amount",
    )
    .unwrap() else {
        panic!("expected query plan");
    };

    let explain = plan.explain();
    assert!(explain.contains("Join keys=1"), "{explain}");
    assert!(explain.contains("residual=true"), "{explain}");
}

#[test]
fn rejects_a_join_with_only_an_unhashable_wide_decimal_equality() {
    let catalog = wide_decimal_catalog(false);
    let error = plan_sql(
        &catalog,
        "SELECT l.amount FROM l JOIN r ON l.amount = r.amount",
    )
    .unwrap_err();

    assert!(matches!(&error, Error::InvalidArgument(_)));
    let message = error.to_string();
    assert!(message.contains("incompatible hash-key types"), "{message}");
    assert!(message.contains("explicit CAST"), "{message}");
}

#[test]
fn rejects_wide_decimal_join_using_without_a_common_type() {
    let catalog = wide_decimal_catalog(false);
    let error = plan_sql(&catalog, "SELECT amount FROM l JOIN r USING (amount)").unwrap_err();

    assert!(matches!(&error, Error::InvalidArgument(_)));
    let message = error.to_string();
    assert!(message.contains("JOIN ... USING"), "{message}");
    assert!(message.contains("explicit CAST"), "{message}");
}

#[test]
fn lossless_decimal_coercion_still_produces_a_hash_key() {
    let catalog = Catalog::default();
    register_fields(
        &catalog,
        "l",
        vec![Field::new("amount", DataType::Decimal128(10, 2), false)],
    );
    register_fields(
        &catalog,
        "r",
        vec![Field::new("amount", DataType::Decimal128(12, 4), false)],
    );
    let StatementPlan::Query(plan) = plan_sql(
        &catalog,
        "SELECT l.amount FROM l JOIN r ON l.amount = r.amount",
    )
    .unwrap() else {
        panic!("expected query plan");
    };

    let explain = plan.explain();
    assert!(explain.contains("Join keys=1"), "{explain}");
    assert!(explain.contains("residual=false"), "{explain}");
}

fn register_schema(catalog: &Catalog, name: &str, data_type: DataType) {
    register_fields(catalog, name, vec![Field::new("id", data_type, false)]);
}

fn register_fields(catalog: &Catalog, name: &str, fields: Vec<Field>) {
    let schema = Arc::new(Schema::new(fields));
    catalog
        .register(TableEntry::new(name, Arc::new(SchemaTable(schema))))
        .unwrap();
}

fn wide_decimal_catalog(with_id: bool) -> Catalog {
    let catalog = Catalog::default();
    let fields = |data_type| {
        let mut fields = Vec::new();
        if with_id {
            fields.push(Field::new("id", DataType::Int64, false));
        }
        fields.push(Field::new("amount", data_type, false));
        fields
    };
    register_fields(&catalog, "l", fields(DataType::Decimal128(35, 2)));
    register_fields(&catalog, "r", fields(DataType::Decimal128(38, 6)));
    catalog
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
