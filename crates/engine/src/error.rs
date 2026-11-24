use arrow::error::ArrowError;
use std::fmt;
use storage::error::StorageError;

#[derive(Debug)]
pub enum EngineError {
    Parse(sqlparser::parser::ParserError),
    /// Invalid SQL
    Bind(String),
    /// Unsupported SQL
    NotSupported(String),
    Execution(String),
    Storage(StorageError),
    Arrow(ArrowError),
}

pub type EngineResult<T> = Result<T, EngineError>;

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::Parse(msg) => write!(f, "parse error: {msg}"),
            EngineError::Bind(msg) => write!(f, "bind error: {msg}"),
            EngineError::NotSupported(msg) => write!(f, "not supported: {msg}"),
            EngineError::Execution(msg) => write!(f, "execution error: {msg}"),
            EngineError::Storage(msg) => write!(f, "storage error: {msg}"),
            EngineError::Arrow(msg) => write!(f, "arrow error: {msg}"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Parse(e) => Some(e),
            EngineError::Storage(e) => Some(e),
            EngineError::Arrow(e) => Some(e),
            EngineError::Bind(_) | EngineError::NotSupported(_) | EngineError::Execution(_) => None,
        }
    }
}

impl From<sqlparser::parser::ParserError> for EngineError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        EngineError::Parse(e)
    }
}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        EngineError::Storage(e)
    }
}

impl From<ArrowError> for EngineError {
    fn from(e: ArrowError) -> Self {
        match e {
            ArrowError::ArithmeticOverflow(msg) | ArrowError::CastError(msg) => {
                EngineError::Execution(msg)
            }
            ArrowError::DivideByZero => EngineError::Execution("division by zero".to_string()),
            other => EngineError::Arrow(other),
        }
    }
}
