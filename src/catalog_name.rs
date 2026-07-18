use sqlparser::ast::{ObjectName, ObjectNamePart};

use crate::{Error, Result};

pub(crate) const DEFAULT_SCHEMA: &str = "main";

pub(crate) fn object(name: &ObjectName, kind: &str) -> Result<String> {
    let parts = object_parts(name, kind)?;
    match parts.as_slice() {
        [name] => Ok(name.clone()),
        [schema, name] if schema == DEFAULT_SCHEMA => Ok(name.clone()),
        [schema, name] => Ok(format!("{schema}.{name}")),
        _ => Err(Error::Unsupported(format!(
            "{kind} names support at most schema.object"
        ))),
    }
}

/// Returns the SQL relation qualifier without collapsing the default schema.
/// Storage keys keep using [`object`], while binding needs `main.table` to stay
/// distinguishable from another schema containing a table with the same name.
pub(crate) fn qualifier(name: &ObjectName, kind: &str) -> Result<String> {
    let parts = object_parts(name, kind)?;
    match parts.as_slice() {
        [name] => Ok(name.clone()),
        [schema, name] => Ok(format!("{schema}.{name}")),
        _ => Err(Error::Unsupported(format!(
            "{kind} names support at most schema.object"
        ))),
    }
}

fn object_parts(name: &ObjectName, kind: &str) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => identifier_name(&identifier.value, kind),
            _ => Err(Error::Unsupported(format!(
                "{kind} names may contain identifiers only"
            ))),
        })
        .collect()
}

pub(crate) fn schema(name: &ObjectName) -> Result<String> {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return Err(Error::Unsupported(
            "schema names must be one identifier".to_owned(),
        ));
    };
    identifier_name(&identifier.value, "schema")
}

pub(crate) fn local(value: &str, kind: &str) -> Result<String> {
    let parts = value.split('.').collect::<Vec<_>>();
    for part in &parts {
        identifier_name(part, kind)?;
    }
    match parts.as_slice() {
        [name] => Ok((*name).to_owned()),
        [schema, name] if schema.eq_ignore_ascii_case(DEFAULT_SCHEMA) => Ok((*name).to_owned()),
        [schema, name] => Ok(format!("{}.{}", schema.to_ascii_lowercase(), name)),
        _ => Err(Error::InvalidArgument(format!(
            "{kind} names support at most schema.object"
        ))),
    }
}

pub(crate) fn schema_of(name: &str) -> &str {
    name.split_once('.')
        .map(|(schema, _)| schema)
        .unwrap_or(DEFAULT_SCHEMA)
}

pub(crate) fn object_of(name: &str) -> &str {
    name.split_once('.').map(|(_, name)| name).unwrap_or(name)
}

pub(crate) fn full_qualifier(name: &str) -> String {
    if name.contains('.') {
        name.to_owned()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

pub(crate) fn qualifier_matches(stored: &str, requested: &str) -> bool {
    stored.eq_ignore_ascii_case(requested)
        || (!requested.contains('.')
            && stored
                .rsplit('.')
                .next()
                .is_some_and(|short| short.eq_ignore_ascii_case(requested)))
}

pub(crate) fn quote(name: &str) -> String {
    match name.split_once('.') {
        Some((schema, object)) => {
            format!("{}.{}", quote_identifier(schema), quote_identifier(object))
        }
        None => quote_identifier(name),
    }
}

fn identifier_name(value: &str, kind: &str) -> Result<String> {
    if value.is_empty() || value.contains(['.', '\0']) {
        return Err(Error::InvalidArgument(format!(
            "{kind} identifiers must be non-empty and cannot contain '.' or NUL"
        )));
    }
    Ok(value.to_ascii_lowercase())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;

    use super::{DEFAULT_SCHEMA, full_qualifier, object, object_of, qualifier_matches, schema_of};

    #[test]
    fn default_schema_has_the_legacy_storage_key() {
        for (sql, expected) in [
            ("DESCRIBE events", "events"),
            ("DESCRIBE main.events", "events"),
            ("DESCRIBE analytics.events", "analytics.events"),
        ] {
            let Statement::ExplainTable { table_name, .. } =
                crate::sql::parse_statements(sql).unwrap().remove(0)
            else {
                panic!("expected DESCRIBE")
            };
            assert_eq!(object(&table_name, "table").unwrap(), expected);
        }
        assert_eq!(schema_of("events"), DEFAULT_SCHEMA);
        assert_eq!(schema_of("analytics.events"), "analytics");
        assert_eq!(object_of("analytics.events"), "events");
        assert_eq!(full_qualifier("events"), "main.events");
        assert_eq!(full_qualifier("analytics.events"), "analytics.events");
        assert!(qualifier_matches("analytics.events", "analytics.events"));
        assert!(qualifier_matches("analytics.events", "events"));
        assert!(!qualifier_matches("analytics.events", "sales.events"));
    }
}
