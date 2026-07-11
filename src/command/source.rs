use sqlparser::tokenizer::{Location, Span};

pub(super) fn suffix_with_location(sql: &str, span: Span) -> Option<String> {
    if span == Span::empty() {
        return None;
    }
    let start = location_offset(sql, span.start)?;
    let mut output = String::new();
    for _ in 1..span.start.line {
        output.push('\n');
    }
    for _ in 1..span.start.column {
        output.push(' ');
    }
    output.push_str(sql[start..].trim_end());
    Some(output)
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
    use sqlparser::tokenizer::{Location, Span};

    use super::suffix_with_location;

    #[test]
    fn preserves_suffix_and_absolute_start_location() {
        let sql = "CREATE TEMP VIEW v AS\n  SELECT 1\n  ORDER BY 1 DESC;";
        let span = Span::new(Location::new(2, 3), Location::new(3, 18));
        assert_eq!(
            suffix_with_location(sql, span).unwrap(),
            "\n  SELECT 1\n  ORDER BY 1 DESC;"
        );
    }
}
