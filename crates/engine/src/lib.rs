//! Query engine. Reads tables through the `storage` crate.
use crate::catalog::Catalog;
use crate::error::{EngineError, EngineResult};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use storage::runtime::StorageRuntime;

pub mod catalog;
pub mod error;
mod exec;
mod expr;
mod logical;
mod optimizer;
mod physical;
mod sql;
mod types;

pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[expect(dead_code, reason = "not yet implemented")]
pub struct Engine {
    runtime: Arc<StorageRuntime>,
    catalog: Arc<dyn Catalog>,
}

impl Engine {
    pub fn new(runtime: Arc<StorageRuntime>, catalog: Arc<dyn Catalog>) -> Self {
        Self { runtime, catalog }
    }

    pub fn sql(&self, sql: &str) -> EngineResult<QueryResult> {
        let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
        let _plan = sql::bind(statements, self.catalog.as_ref())?;
        Err(EngineError::NotSupported(
            "query execution not implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MemoryCatalog;

    fn engine() -> Engine {
        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        Engine::new(runtime, Arc::new(MemoryCatalog::new()))
    }

    #[test]
    fn valid_sql_is_not_supported_yet() {
        let result = engine().sql("select 1");
        assert!(matches!(result, Err(EngineError::NotSupported(_))));
    }

    #[test]
    fn invalid_sql_is_a_parse_error() {
        let result = engine().sql("selec 1");
        assert!(matches!(result, Err(EngineError::Parse(_))));
    }
}
