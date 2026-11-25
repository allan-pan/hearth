//! Physical planner: LogicalPlan → PhysicalPlan.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use storage::runtime::StorageRuntime;

use crate::error::{EngineError, EngineResult};
use crate::exec::{
    CollectSink, FilterOp, LimitOp, PhysicalOperator, PhysicalSink, PhysicalSource, Pipeline,
    ProjectOp, ScanSource,
};
use crate::logical::{LogicalPlan, project_schema};

pub struct PhysicalPlan {
    pub schema: SchemaRef,
    pub pipelines: Vec<Pipeline>,
}

pub fn plan(logical: &LogicalPlan, runtime: Arc<StorageRuntime>) -> EngineResult<PhysicalPlan> {
    let (source, operators, schema) = build_pipeline(logical, &runtime)?;
    let pipeline = Pipeline {
        source,
        operators,
        sink: PhysicalSink::Collect(CollectSink),
    };
    Ok(PhysicalPlan {
        schema,
        pipelines: vec![pipeline],
    })
}

fn build_pipeline(
    logical: &LogicalPlan,
    runtime: &Arc<StorageRuntime>,
) -> EngineResult<(PhysicalSource, Vec<PhysicalOperator>, SchemaRef)> {
    match logical {
        LogicalPlan::Scan { table, snapshot } => {
            let schema = snapshot.schema.clone();
            let source = ScanSource::all_columns(table.as_str(), runtime.clone(), snapshot.clone());
            Ok((PhysicalSource::Scan(source), vec![], schema))
        }
        LogicalPlan::Filter { input, predicate } => {
            let (source, mut operators, schema) = build_pipeline(input, runtime)?;
            operators.push(PhysicalOperator::Filter(FilterOp {
                predicate: predicate.clone(),
            }));
            Ok((source, operators, schema))
        }
        LogicalPlan::Project { input, exprs } => {
            let (source, mut operators, input_schema) = build_pipeline(input, runtime)?;
            let schema = project_schema(&input_schema, exprs);
            operators.push(PhysicalOperator::Project(ProjectOp {
                exprs: exprs.iter().map(|(e, _)| e.clone()).collect(),
                schema: schema.clone(),
            }));
            Ok((source, operators, schema))
        }
        LogicalPlan::Limit { input, skip, fetch } => {
            if *fetch == Some(0) {
                ensure_supported(input)?;
                return Ok((PhysicalSource::Empty, vec![], input.schema()));
            }
            let (source, mut operators, schema) = build_pipeline(input, runtime)?;
            operators.push(PhysicalOperator::Limit(LimitOp {
                skip: *skip,
                fetch: *fetch,
            }));
            Ok((source, operators, schema))
        }
        LogicalPlan::Aggregate { .. } => Err(EngineError::NotSupported(
            "Aggregate operator not implemented yet".to_string(),
        )),
        LogicalPlan::Sort { .. } => Err(EngineError::NotSupported(
            "Sort operator not implemented yet".to_string(),
        )),
    }
}

fn ensure_supported(logical: &LogicalPlan) -> EngineResult<()> {
    match logical {
        LogicalPlan::Scan { .. } => Ok(()),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Limit { input, .. } => ensure_supported(input),
        LogicalPlan::Aggregate { .. } => Err(EngineError::NotSupported(
            "Aggregate operator not implemented yet".to_string(),
        )),
        LogicalPlan::Sort { .. } => Err(EngineError::NotSupported(
            "Sort operator not implemented yet".to_string(),
        )),
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, pipeline) in self.pipelines.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "P{}: {pipeline}", i + 1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use storage::table::Snapshot;

    use super::*;
    use crate::catalog::MemoryCatalog;
    use crate::sql;

    fn catalog() -> MemoryCatalog {
        let mut catalog = MemoryCatalog::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        catalog.register(
            "people",
            Snapshot {
                version: 0,
                files: vec![],
                schema,
            },
        );
        catalog
    }

    fn plan_sql(query: &str) -> EngineResult<PhysicalPlan> {
        let statements = Parser::parse_sql(&GenericDialect {}, query)?;
        let logical = sql::bind(statements, &catalog())?;
        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        plan(&logical, runtime)
    }

    #[test]
    fn plans_scan_filter_project_limit() {
        let physical =
            plan_sql("SELECT name AS label, id FROM people WHERE id > 1 LIMIT 10 OFFSET 2")
                .unwrap();
        assert_eq!(
            physical.to_string(),
            "P1: Scan(people) → Filter: #0 id > 1 → Project: #1 name AS label, #0 id → Limit: skip 2, fetch 10 → CollectSink"
        );
        assert_eq!(physical.schema.field(0).name(), "label");
        assert_eq!(physical.schema.field(1).name(), "id");
    }

    #[test]
    fn limit_zero_replaces_pipeline_with_empty_source_and_keeps_schema() {
        let physical = plan_sql("SELECT name AS label FROM people WHERE id > 1 LIMIT 0").unwrap();
        assert_eq!(physical.to_string(), "P1: Empty → CollectSink");
        assert_eq!(physical.schema.fields().len(), 1);
        assert_eq!(physical.schema.field(0).name(), "label");
        assert_eq!(physical.schema.field(0).data_type(), &DataType::Utf8View);
    }

    #[test]
    fn aggregate_and_sort_are_not_supported_yet_even_under_limit_zero() {
        assert!(matches!(
            plan_sql("SELECT COUNT(*) FROM people"),
            Err(EngineError::NotSupported(_))
        ));
        assert!(matches!(
            plan_sql("SELECT id FROM people ORDER BY id"),
            Err(EngineError::NotSupported(_))
        ));
        assert!(matches!(
            plan_sql("SELECT COUNT(*) FROM people LIMIT 0"),
            Err(EngineError::NotSupported(_))
        ));
        assert!(matches!(
            plan_sql("SELECT id FROM people ORDER BY id LIMIT 0"),
            Err(EngineError::NotSupported(_))
        ));
    }
}
