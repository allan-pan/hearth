use std::fmt;

#[derive(Debug)]
pub enum StorageError {
    /// The request does not fit the table: unknown column, bad literal, and so on.
    Plan(String),
    Io(std::io::Error),
    Arrow(arrow::error::ArrowError),
    Vortex(vortex::error::VortexError),
    /// A broken invariant inside storage, such as a failed worker task.
    Internal(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Plan(msg) => write!(f, "plan error: {msg}"),
            StorageError::Io(e) => write!(f, "io error: {e}"),
            StorageError::Arrow(e) => write!(f, "arrow error: {e}"),
            StorageError::Vortex(e) => write!(f, "vortex error: {e}"),
            StorageError::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) => Some(e),
            StorageError::Arrow(e) => Some(e),
            StorageError::Vortex(e) => Some(e),
            StorageError::Plan(_) | StorageError::Internal(_) => None,
        }
    }
}

impl From<arrow::error::ArrowError> for StorageError {
    fn from(e: arrow::error::ArrowError) -> Self {
        StorageError::Arrow(e)
    }
}

impl From<vortex::error::VortexError> for StorageError {
    fn from(e: vortex::error::VortexError) -> Self {
        StorageError::Vortex(e)
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::error::ArrowError;
    use std::error::Error;

    #[test]
    fn display_plan() {
        let e = StorageError::Plan("unknown column x".into());
        assert_eq!(e.to_string(), "plan error: unknown column x");
    }

    #[test]
    fn display_io() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        assert_eq!(StorageError::Io(io).to_string(), "io error: file not found");
    }

    #[test]
    fn display_arrow() {
        let inner = ArrowError::ComputeError("bad".into());
        let expected = format!("arrow error: {inner}");
        assert_eq!(StorageError::Arrow(inner).to_string(), expected);
    }

    #[test]
    fn display_internal() {
        let e = StorageError::Internal("task failed".into());
        assert_eq!(e.to_string(), "internal error: task failed");
    }

    #[test]
    fn from_arrow_keeps_source() {
        let e: StorageError = ArrowError::ComputeError("bad".into()).into();
        assert!(matches!(e, StorageError::Arrow(_)));
        assert!(e.source().is_some());
    }

    #[test]
    fn from_io_keeps_source() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let e: StorageError = io.into();
        assert!(matches!(e, StorageError::Io(ref io) if io.kind() == std::io::ErrorKind::NotFound));
        assert!(e.source().is_some());
    }

    #[test]
    fn from_vortex_keeps_source() {
        let e: StorageError = vortex::error::vortex_err!("bad").into();
        assert!(matches!(e, StorageError::Vortex(_)));
        assert!(e.source().is_some());
    }
}
