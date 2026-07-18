use sqlparser::ast::{
    AlterTableOperation, ColumnOption, DataType, Expr, ObjectName, ObjectNamePart,
    RenameTableNameKind, Statement,
};

use crate::{Error, Result};

pub(crate) struct NativeAlterCommand {
    pub(crate) name: String,
    pub(crate) if_exists: bool,
    pub(crate) operation: NativeAlterOperation,
}

pub(crate) enum NativeAlterOperation {
    AddColumn {
        name: String,
        data_type: DataType,
        default: Option<Box<Expr>>,
        if_not_exists: bool,
    },
    DropColumns {
        names: Vec<String>,
        if_exists: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeAlterCommand>> {
    let Statement::AlterTable(alter) = statement else {
        return Ok(None);
    };
    if alter.only
        || alter.location.is_some()
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
        || alter.operations.len() != 1
    {
        return Err(unsupported());
    }
    let source_name = simple_name(&alter.name)?.to_ascii_lowercase();
    let operation = match &alter.operations[0] {
        AlterTableOperation::AddColumn {
            if_not_exists,
            column_def,
            column_position,
            ..
        } => {
            if column_position.is_some() || column_def.data_type == DataType::Unspecified {
                return Err(unsupported());
            }
            let mut default = None;
            for option in &column_def.options {
                if option.name.is_some() {
                    return Err(unsupported());
                }
                match &option.option {
                    ColumnOption::Null => {}
                    ColumnOption::Default(value) if default.is_none() => {
                        default = Some(Box::new(value.clone()));
                    }
                    _ => return Err(unsupported()),
                }
            }
            NativeAlterOperation::AddColumn {
                name: column_def.name.value.to_ascii_lowercase(),
                data_type: column_def.data_type.clone(),
                default,
                if_not_exists: *if_not_exists,
            }
        }
        AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            drop_behavior,
            ..
        } => {
            if drop_behavior.is_some() || column_names.is_empty() {
                return Err(unsupported());
            }
            NativeAlterOperation::DropColumns {
                names: column_names
                    .iter()
                    .map(|name| name.value.to_ascii_lowercase())
                    .collect(),
                if_exists: *if_exists,
            }
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => NativeAlterOperation::RenameColumn {
            old_name: old_column_name.value.to_ascii_lowercase(),
            new_name: new_column_name.value.to_ascii_lowercase(),
        },
        AlterTableOperation::RenameTable { table_name } => {
            let name = match table_name {
                RenameTableNameKind::As(name) | RenameTableNameKind::To(name) => name,
            };
            NativeAlterOperation::RenameTable {
                new_name: rename_target(&source_name, name)?,
            }
        }
        _ => return Err(unsupported()),
    };
    Ok(Some(NativeAlterCommand {
        name: source_name,
        if_exists: alter.if_exists,
        operation,
    }))
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
}

fn rename_target(source: &str, name: &ObjectName) -> Result<String> {
    if let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() {
        let object = crate::catalog_name::object(name, "native table")?;
        let schema = crate::catalog_name::schema_of(source);
        return Ok(if schema == crate::catalog_name::DEFAULT_SCHEMA {
            object
        } else {
            format!("{schema}.{}", identifier.value.to_ascii_lowercase())
        });
    }
    crate::catalog_name::object(name, "native table")
}

fn unsupported() -> Error {
    Error::Unsupported(
        "native ALTER TABLE supports one ADD/DROP/RENAME COLUMN or RENAME TO operation; constraints and placement modifiers are not supported"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::{NativeAlterOperation, parse};

    #[test]
    fn parses_safe_column_operations() {
        for sql in [
            "ALTER TABLE t ADD COLUMN label VARCHAR DEFAULT 'new'",
            "ALTER TABLE t DROP COLUMN label",
            "ALTER TABLE t RENAME COLUMN label TO name",
            "ALTER TABLE t RENAME TO renamed",
        ] {
            let statement = crate::sql::parse_statements(sql).unwrap().remove(0);
            assert!(parse(&statement).unwrap().is_some(), "{sql}");
        }
        let statement = crate::sql::parse_statements("ALTER TABLE t ADD COLUMN id BIGINT NOT NULL")
            .unwrap()
            .remove(0);
        assert!(parse(&statement).is_err());
        let statement = crate::sql::parse_statements("ALTER TABLE t DROP COLUMN a")
            .unwrap()
            .remove(0);
        assert!(matches!(
            parse(&statement).unwrap().unwrap().operation,
            NativeAlterOperation::DropColumns { .. }
        ));
    }
}
