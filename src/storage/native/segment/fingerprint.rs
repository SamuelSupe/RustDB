use arrow::datatypes::Schema;

pub(super) fn schema_fingerprint(schema: &Schema) -> String {
    super::super::schema::sha256(&super::super::schema::encode(schema))
}
