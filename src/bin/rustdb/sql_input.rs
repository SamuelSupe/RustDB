use rustdb::Result;
use sqlparser::{
    ast::{Spanned, Statement},
    dialect::DuckDbDialect,
    parser::Parser,
    tokenizer::{Location, Span},
};

pub(crate) fn parse_statements(sql: &str) -> Result<Vec<String>> {
    Parser::parse_sql(&DuckDbDialect {}, sql)
        .map(|statements| original_statements(sql, &statements))
        .map_err(Into::into)
}

fn original_statements(sql: &str, statements: &[Statement]) -> Vec<String> {
    statements
        .iter()
        .enumerate()
        .map(|(index, statement)| {
            let end = statements
                .get(index + 1)
                .and_then(|next| location_offset(sql, next.span().start))
                .unwrap_or(sql.len());
            original_statement(sql, statement, end)
        })
        .collect()
}

fn original_statement(sql: &str, statement: &Statement, end: usize) -> String {
    let span = statement.span();
    let Some(start) = location_offset(sql, span.start) else {
        return statement.to_string();
    };
    if span == Span::empty() || start > end {
        return statement.to_string();
    }

    let mut output = String::new();
    for _ in 1..span.start.line {
        output.push('\n');
    }
    for _ in 1..span.start.column {
        output.push(' ');
    }
    output.push_str(sql[start..end].trim_end());
    output
}

fn location_offset(sql: &str, target: Location) -> Option<usize> {
    let (mut line, mut column) = (1, 1);
    for (offset, character) in sql.char_indices() {
        if (line, column) == (target.line, target.column) {
            return Some(offset);
        }
        if character == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    ((line, column) == (target.line, target.column)).then_some(sql.len())
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
