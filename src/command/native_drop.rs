use sqlparser::ast::{ObjectName, ObjectType, Statement};

use crate::{Error, Result};

pub(crate) struct NativeDropTableCommand {
    pub(crate) name: String,
    pub(crate) if_exists: bool,
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeDropTableCommand>> {
    let Statement::Drop {
        object_type: ObjectType::Table,
        if_exists,
        names,
        cascade,
        restrict,
        purge,
        temporary,
        table,
    } = statement
    else {
        return Ok(None);
    };
    if *cascade || *restrict || *purge || *temporary || table.is_some() || names.len() != 1 {
        return Err(Error::Unsupported(
            "native DROP TABLE supports one name and optional IF EXISTS only".to_owned(),
        ));
    }
    Ok(Some(NativeDropTableCommand {
        name: simple_name(&names[0])?.to_ascii_lowercase(),
        if_exists: *if_exists,
    }))
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn accepts_one_plain_table() {
        let statement = crate::sql::parse_statements("DROP TABLE IF EXISTS events")
            .unwrap()
            .remove(0);
        let command = parse(&statement).unwrap().unwrap();
        assert_eq!(command.name, "events");
        assert!(command.if_exists);
    }
}
