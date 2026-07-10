use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error{path}: {source}", path = display_path(.path))]
    Io {
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("SQL parse error: {0}")]
    SqlParse(#[from] sqlparser::parser::ParserError),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("query cancelled")]
    Cancelled,

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("internal error: {0}")]
    Internal(String),
}

fn display_path(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map(|path| format!(" at {}", path.display()))
        .unwrap_or_default()
}

impl Error {
    pub fn io(path: impl Into<Option<PathBuf>>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
