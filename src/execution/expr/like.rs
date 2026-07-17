use std::mem::size_of;

use arrow::array::{Array, ArrayRef, BooleanArray, StringArray};

use crate::{Error, Result};

pub(super) fn evaluate(
    values: &ArrayRef,
    patterns: &ArrayRef,
    negated: bool,
    escape: Option<char>,
) -> Result<BooleanArray> {
    let values = strings(values, "input")?;
    let patterns = strings(patterns, "pattern")?;
    let output = (0..values.len())
        .map(|row| {
            if values.is_null(row) || patterns.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(
                    matches(values.value(row), patterns.value(row), escape)? ^ negated,
                ))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BooleanArray::from(output))
}

pub(super) fn evaluate_literal(
    values: &ArrayRef,
    pattern: &str,
    negated: bool,
    escape: Option<char>,
) -> Result<BooleanArray> {
    let values = strings(values, "input")?;
    if values.null_count() == values.len() {
        return Ok(BooleanArray::new_null(values.len()));
    }

    let compiled = CompiledLike::new(pattern, escape)?;
    let mut workspace = MatchWorkspace::new(compiled.wildcard_tokens());
    Ok(values
        .iter()
        .map(|value| value.map(|value| compiled.is_match(value, &mut workspace) ^ negated))
        .collect())
}

pub(super) fn literal_workspace_bytes(pattern: &str) -> usize {
    pattern
        .len()
        .saturating_mul(size_of::<LikeToken>().saturating_add(56))
        .saturating_add(4_096)
        .max(1)
}

fn strings<'a>(array: &'a ArrayRef, role: &str) -> Result<&'a StringArray> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Internal(format!("LIKE {role} has type {}", array.data_type())))
}

fn matches(value: &str, pattern: &str, escape: Option<char>) -> Result<bool> {
    let compiled = CompiledLike::new(pattern, escape)?;
    let mut workspace = MatchWorkspace::new(compiled.wildcard_tokens());
    Ok(compiled.is_match(value, &mut workspace))
}

enum CompiledLike {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Contains(String),
    Segments {
        parts: Box<[String]>,
        leading_any: bool,
        trailing_any: bool,
    },
    Wildcard(Box<[LikeToken]>),
}

impl CompiledLike {
    fn new(pattern: &str, escape: Option<char>) -> Result<Self> {
        let tokens = like_tokens(pattern, escape)?;
        if tokens.contains(&LikeToken::One) {
            return Ok(Self::Wildcard(tokens.into_boxed_slice()));
        }
        Ok(Self::from_literal_segments(tokens))
    }

    fn from_literal_segments(tokens: Vec<LikeToken>) -> Self {
        if tokens.is_empty() {
            return Self::Exact(String::new());
        }
        let leading_any = tokens.first() == Some(&LikeToken::Any);
        let trailing_any = tokens.last() == Some(&LikeToken::Any);
        let mut parts = Vec::new();
        let mut current = String::new();
        for token in tokens {
            match token {
                LikeToken::Literal(value) => current.push(value),
                LikeToken::Any => {
                    if !current.is_empty() {
                        parts.push(std::mem::take(&mut current));
                    }
                }
                LikeToken::One => unreachable!("single-character wildcard handled above"),
            }
        }
        if !current.is_empty() {
            parts.push(current);
        }

        if parts.is_empty() {
            return Self::Contains(String::new());
        }
        if parts.len() == 1 {
            let value = parts.pop().expect("one LIKE literal segment");
            return match (leading_any, trailing_any) {
                (false, false) => Self::Exact(value),
                (false, true) => Self::Prefix(value),
                (true, false) => Self::Suffix(value),
                (true, true) => Self::Contains(value),
            };
        }
        Self::Segments {
            parts: parts.into_boxed_slice(),
            leading_any,
            trailing_any,
        }
    }

    fn wildcard_tokens(&self) -> usize {
        match self {
            Self::Wildcard(tokens) => tokens.len(),
            _ => 0,
        }
    }

    fn is_match(&self, value: &str, workspace: &mut MatchWorkspace) -> bool {
        match self {
            Self::Exact(expected) => value == expected,
            Self::Prefix(expected) => value.starts_with(expected),
            Self::Suffix(expected) => value.ends_with(expected),
            Self::Contains(expected) => value.contains(expected),
            Self::Segments {
                parts,
                leading_any,
                trailing_any,
            } => segments_match(value, parts, *leading_any, *trailing_any),
            Self::Wildcard(tokens) => workspace.matches(value, tokens),
        }
    }
}

fn segments_match(value: &str, parts: &[String], leading_any: bool, trailing_any: bool) -> bool {
    let mut position = 0usize;
    let mut first_middle = 0usize;
    if !leading_any {
        let first = &parts[0];
        if !value.starts_with(first) {
            return false;
        }
        position = first.len();
        first_middle = 1;
    }

    let middle_end = parts.len() - usize::from(!trailing_any);
    for part in &parts[first_middle..middle_end] {
        let Some(found) = value.get(position..).and_then(|rest| rest.find(part)) else {
            return false;
        };
        position = position.saturating_add(found).saturating_add(part.len());
    }

    if trailing_any {
        return true;
    }
    let last = &parts[parts.len() - 1];
    value
        .len()
        .checked_sub(last.len())
        .is_some_and(|start| start >= position && value.ends_with(last))
}

struct MatchWorkspace {
    previous: Vec<u8>,
    current: Vec<u8>,
}

impl MatchWorkspace {
    fn new(tokens: usize) -> Self {
        Self {
            previous: vec![0; tokens.saturating_add(1)],
            current: vec![0; tokens.saturating_add(1)],
        }
    }

    fn matches(&mut self, value: &str, tokens: &[LikeToken]) -> bool {
        self.previous.fill(0);
        self.previous[0] = 1;
        for (index, token) in tokens.iter().enumerate() {
            self.previous[index + 1] =
                u8::from(*token == LikeToken::Any && self.previous[index] != 0);
        }

        for value in value.chars() {
            self.current.fill(0);
            for (index, token) in tokens.iter().enumerate() {
                self.current[index + 1] = u8::from(match token {
                    LikeToken::Any => self.previous[index + 1] != 0 || self.current[index] != 0,
                    LikeToken::One => self.previous[index] != 0,
                    LikeToken::Literal(expected) => self.previous[index] != 0 && value == *expected,
                });
            }
            std::mem::swap(&mut self.previous, &mut self.current);
        }
        self.previous[tokens.len()] != 0
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum LikeToken {
    Any,
    One,
    Literal(char),
}

fn like_tokens(pattern: &str, escape: Option<char>) -> Result<Vec<LikeToken>> {
    let mut characters = pattern.chars();
    let mut tokens = Vec::with_capacity(pattern.len());
    while let Some(character) = characters.next() {
        let token = if escape == Some(character) {
            LikeToken::Literal(characters.next().ok_or_else(|| {
                Error::InvalidArgument("LIKE pattern ends with its ESCAPE character".into())
            })?)
        } else {
            match character {
                '%' => LikeToken::Any,
                '_' => LikeToken::One,
                literal => LikeToken::Literal(literal),
            }
        };
        if token != LikeToken::Any || tokens.last() != Some(&LikeToken::Any) {
            tokens.push(token);
        }
    }
    Ok(tokens)
}

#[cfg(test)]
#[path = "like/tests.rs"]
mod tests;
