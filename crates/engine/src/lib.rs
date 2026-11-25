//! Query engine. Reads tables through the `storage` crate.
use crate::catalog::Catalog;
use crate::error::EngineResult;
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
        let logical = sql::bind(statements, self.catalog.as_ref())?;
        let physical = physical::plan(&logical, self.runtime.clone())?;
        exec::execute(physical)
    }
}
