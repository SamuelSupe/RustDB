use sqlparser::ast::{ObjectType, SchemaName, Statement};

use crate::{Error, Result};

pub(crate) enum NativeSchemaCommand {
    Create { name: String, if_not_exists: bool },
    Drop { name: String, if_exists: bool },
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeSchemaCommand>> {
    match statement {
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            if with.is_some()
                || options.is_some()
                || default_collate_spec.is_some()
                || clone.is_some()
            {
                return Err(unsupported_create());
            }
            let SchemaName::Simple(name) = schema_name else {
                return Err(unsupported_create());
            };
            Ok(Some(NativeSchemaCommand::Create {
                name: crate::catalog_name::schema(name)?,
                if_not_exists: *if_not_exists,
            }))
        }
        Statement::Drop {
            object_type: ObjectType::Schema,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } => {
            if *cascade || *purge || *temporary || table.is_some() || names.len() != 1 {
                return Err(Error::Unsupported(
                    "DROP SCHEMA supports one empty schema, optional IF EXISTS, and optional RESTRICT"
                        .to_owned(),
                ));
            }
            let _ = restrict;
            Ok(Some(NativeSchemaCommand::Drop {
                name: crate::catalog_name::schema(&names[0])?,
                if_exists: *if_exists,
            }))
        }
        _ => Ok(None),
    }
}

fn unsupported_create() -> Error {
    Error::Unsupported("CREATE SCHEMA supports one name and optional IF NOT EXISTS only".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{NativeSchemaCommand, parse};

    #[test]
    fn parses_safe_schema_ddl() {
        for sql in [
            "CREATE SCHEMA analytics",
            "CREATE SCHEMA IF NOT EXISTS analytics",
            "DROP SCHEMA analytics",
            "DROP SCHEMA IF EXISTS analytics RESTRICT",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement).unwrap().is_some(), "{sql}");
        }
        let statement = crate::sql::parse_statements("DROP SCHEMA analytics CASCADE")
            .unwrap()
            .remove(0);
        assert!(parse(&statement).is_err());
        let statement = crate::sql::parse_statements("CREATE SCHEMA analytics")
            .unwrap()
            .remove(0);
        assert!(matches!(
            parse(&statement).unwrap(),
            Some(NativeSchemaCommand::Create { name, .. }) if name == "analytics"
        ));
    }
}
