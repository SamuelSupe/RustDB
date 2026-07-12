use rustdb::Result;

pub(crate) fn parse_statements(sql: &str) -> Result<Vec<String>> {
    rustdb::split_sql_statements(sql)
}

#[cfg(test)]
mod tests {
    use super::parse_statements;

    #[test]
    fn keeps_multiline_statement_text() {
        let sql = "SELECT id, count(*)\nFROM read_csv('data.csv')\nGROUP BY 3;";
        assert_eq!(parse_statements(sql).unwrap(), [sql.to_owned()]);
    }

    #[test]
    fn prefixes_later_statements_to_preserve_absolute_locations() {
        let sql = "SELECT 1;\n\nSELECT id, count(*)\nFROM data\nGROUP BY 3;";
        let statements = parse_statements(sql).unwrap();
        assert_eq!(statements.len(), 2);
        assert_eq!(
            statements[1],
            "\n\nSELECT id, count(*)\nFROM data\nGROUP BY 3;"
        );
    }

    #[test]
    fn preserves_trailing_order_direction_outside_statement_span() {
        let sql = "SELECT value\nFROM data\nORDER BY value DESC;";
        assert_eq!(parse_statements(sql).unwrap(), [sql.to_owned()]);
    }
}
