use sqlparser::{
    ast::{Spanned, Statement},
    dialect::DuckDbDialect,
    parser::Parser,
    tokenizer::{Location, Token, Tokenizer},
};

use crate::{Error, Result};

/// sqlparser's statement dispatcher does not treat a leading parenthesis as a
/// query statement, even though its query parser supports parenthesized set
/// expressions. Keep the workaround at one boundary so every caller receives
/// the same AST and source locations.
pub(crate) fn parse_statements(sql: &str) -> Result<Vec<Statement>> {
    let dialect = DuckDbDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql)?;
    let mut statements = Vec::new();
    loop {
        while parser.consume_token(&Token::SemiColon) {}
        if parser.peek_token().token == Token::EOF {
            break;
        }
        let statement = if parser.peek_token().token == Token::LParen {
            Statement::Query(parser.parse_query()?)
        } else {
            parser.parse_statement()?
        };
        statements.push(statement);
        if parser.peek_token().token != Token::EOF && !parser.consume_token(&Token::SemiColon) {
            parser.expect_token(&Token::EOF)?;
        }
    }
    Ok(statements)
}

pub(crate) fn split_statement_text(sql: &str) -> Result<Vec<String>> {
    let statements = parse_statements(sql)?;
    if statements.len() <= 1 {
        return Ok((!statements.is_empty())
            .then(|| sql.to_owned())
            .into_iter()
            .collect());
    }

    let dialect = DuckDbDialect {};
    let semicolons = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|error| sqlparser::parser::ParserError::TokenizerError(error.to_string()))?
        .into_iter()
        .filter(|token| token.token == Token::SemiColon)
        .filter_map(|token| location_offset(sql, token.span.end))
        .collect::<Vec<_>>();
    let mut separators = Vec::with_capacity(statements.len() - 1);
    let mut previous = 0usize;
    for statement in statements.iter().skip(1) {
        let next = location_offset(sql, statement.span().start).ok_or_else(|| {
            Error::Internal("SQL statement location is outside the original input".into())
        })?;
        let separator = semicolons
            .iter()
            .copied()
            .rfind(|offset| *offset > previous && *offset <= next)
            .ok_or_else(|| Error::Internal("SQL statements have no delimiter".into()))?;
        separators.push(separator);
        previous = separator;
    }

    let mut output = Vec::with_capacity(statements.len());
    let mut start = 0usize;
    for end in separators.into_iter().chain(std::iter::once(sql.len())) {
        output.push(absolute_segment(sql, start, end));
        start = end;
    }
    Ok(output)
}

fn absolute_segment(sql: &str, start: usize, end: usize) -> String {
    let segment = sql[start..end].trim_end();
    let location = offset_location(sql, start);
    let mut output = String::new();
    for _ in 1..location.line {
        output.push('\n');
    }
    let first_line_has_content = segment
        .split_once('\n')
        .map_or(segment, |(first, _)| first)
        .chars()
        .any(|character| !character.is_whitespace());
    if first_line_has_content {
        for _ in 1..location.column {
            output.push(' ');
        }
    }
    output.push_str(segment);
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

fn offset_location(sql: &str, target: usize) -> Location {
    let (mut line, mut column) = (1, 1);
    for (offset, character) in sql.char_indices() {
        if offset == target {
            break;
        }
        if character == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    Location { line, column }
}

#[cfg(test)]
mod tests {
    use super::{parse_statements, split_statement_text};

    #[test]
    fn parses_root_parenthesized_set_expression() {
        let statements =
            parse_statements("(SELECT 1 UNION ALL SELECT 1) INTERSECT SELECT 1").unwrap();
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn splits_parenthesized_and_regular_statements_at_original_locations() {
        let sql = "(SELECT 1 UNION ALL SELECT 2);\n\nSELECT 3;";
        assert_eq!(
            split_statement_text(sql).unwrap(),
            [
                "(SELECT 1 UNION ALL SELECT 2);".to_owned(),
                "\n\nSELECT 3;".to_owned(),
            ]
        );
    }
}
