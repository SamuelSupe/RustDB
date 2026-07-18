use sqlparser::ast::{ObjectName, Statement};

use crate::{Error, Result};

pub(crate) struct NativeTruncateCommand {
    pub(crate) name: String,
}

pub(super) fn parse(statement: &Statement) -> Result<Option<NativeTruncateCommand>> {
    let Statement::Truncate(truncate) = statement else {
        return Ok(None);
    };
    let [target] = truncate.table_names.as_slice() else {
        return Err(unsupported());
    };
    if target.only
        || target.has_asterisk
        || truncate.partitions.is_some()
        || truncate.if_exists
        || truncate.identity.is_some()
        || truncate.cascade.is_some()
        || truncate.on_cluster.is_some()
    {
        return Err(unsupported());
    }
    Ok(Some(NativeTruncateCommand {
        name: simple_name(&target.name)?.to_ascii_lowercase(),
    }))
}

fn simple_name(name: &ObjectName) -> Result<String> {
    crate::catalog_name::object(name, "native table")
}

fn unsupported() -> Error {
    Error::Unsupported(
        "native TRUNCATE supports one table without partitions, identity, or cascade modifiers"
            .to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn accepts_one_plain_target() {
        let statement = crate::sql::parse_statements("TRUNCATE TABLE events")
            .unwrap()
            .remove(0);
        assert_eq!(parse(&statement).unwrap().unwrap().name, "events");

        let statement = crate::sql::parse_statements("TRUNCATE TABLE a, b")
            .unwrap()
            .remove(0);
        assert!(parse(&statement).is_err());
    }
}
