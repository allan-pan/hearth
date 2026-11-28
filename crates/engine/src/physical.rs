//! Physical planner: LogicalPlan → PhysicalPlan.

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use storage::runtime::StorageRuntime;

use crate::error::{EngineError, EngineResult};
use crate::exec::{
    AggregateSink, CollectSink, FilterOp, LimitOp, PhysicalOperator, PhysicalSink, PhysicalSource,
    Pipeline, ProjectOp, ScanSource, SlotId, SlotSource,
};
use crate::logical::{LogicalPlan, aggregate_schema, project_schema};

pub struct PhysicalPlan {
    pub schema: SchemaRef,
    pub pipelines: Vec<Pipeline>,
}

pub fn plan(logical: &LogicalPlan, runtime: Arc<StorageRuntime>) -> EngineResult<PhysicalPlan> {
    let mut builder = PipelineBuilder::new(runtime);
    let partial = builder.build(logical)?;
    let schema = partial.schema;
    builder.seal(partial.source, partial.operators, PhysicalSink::Collect(CollectSink));
    Ok(PhysicalPlan {
        schema,
        pipelines: builder.pipelines,
    })
}

struct PartialPipeline {
    source: PhysicalSource,
    operators: Vec<PhysicalOperator>,
    schema: SchemaRef,
}

struct PipelineBuilder {
    runtime: Arc<StorageRuntime>,
    pipelines: Vec<Pipeline>,
}

impl PipelineBuilder {
    fn new(runtime: Arc<StorageRuntime>) -> Self {
        Self {
            runtime,
            pipelines: Vec::new(),
        }
    }

    fn seal(
        &mut self,
        source: PhysicalSource,
        operators: Vec<PhysicalOperator>,
        sink: PhysicalSink,
    ) -> SlotId {
        let slot = SlotId(self.pipelines.len());
        self.pipelines.push(Pipeline {
            source,
            operators,
            sink,
        });
        slot
    }

    fn build(&mut self, logical: &LogicalPlan) -> EngineResult<PartialPipeline> {
        match logical {
            LogicalPlan::Scan { table, snapshot } => {
                let schema = snapshot.schema.clone();
                let source =
                    ScanSource::all_columns(table.as_str(), self.runtime.clone(), snapshot.clone());
                Ok(PartialPipeline {
                    source: PhysicalSource::Scan(source),
                    operators: vec![],
                    schema,
                })
            }
            LogicalPlan::Filter { input, predicate } => {
                let mut partial = self.build(input)?;
                partial.operators.push(PhysicalOperator::Filter(FilterOp {
                    predicate: predicate.clone(),
                }));
                Ok(partial)
            }
            LogicalPlan::Project { input, exprs } => {
                let mut partial = self.build(input)?;
                let schema = project_schema(&partial.schema, exprs);
                partial.operators.push(PhysicalOperator::Project(ProjectOp {
                    exprs: exprs.iter().map(|(e, _)| e.clone()).collect(),
                    schema: schema.clone(),
                }));
                partial.schema = schema;
                Ok(partial)
            }
            LogicalPlan::Limit { input, skip, fetch } => {
                if *fetch == Some(0) {
                    let schema = PipelineBuilder::new(self.runtime.clone())
                        .build(input)?
                        .schema;
                    return Ok(PartialPipeline {
                        source: PhysicalSource::Empty,
                        operators: vec![],
                        schema,
                    });
                }
                let mut partial = self.build(input)?;
                partial.operators.push(PhysicalOperator::Limit(LimitOp {
                    skip: *skip,
                    fetch: *fetch,
                }));
                Ok(partial)
            }
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                let partial = self.build(input)?;
                let schema = aggregate_schema(&partial.schema, group_by, aggregates);
                let sink = AggregateSink::new(
                    group_by.clone(),
                    aggregates.clone(),
                    partial.schema,
                    schema.clone(),
                )?;
                let slot = self.seal(
                    partial.source,
                    partial.operators,
                    PhysicalSink::Aggregate(sink),
                );
                Ok(PartialPipeline {
                    source: PhysicalSource::Slot(SlotSource::new(slot)),
                    operators: vec![],
                    schema,
                })
            }
            LogicalPlan::Sort { .. } => Err(EngineError::NotSupported(
                "Sort operator not implemented yet".to_string(),
            )),
        }
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
    fn plans_ungrouped_aggregate_as_two_pipelines() {
        let physical =
            plan_sql("SELECT COUNT(*), SUM(id) AS total FROM people WHERE id > 1").unwrap();
        assert_eq!(
            physical.to_string(),
            concat!(
                "P1: Scan(people) → Filter: #0 id > 1 → UngroupedAggregateSink: aggregates [COUNT(*), SUM(#0 id)]\n",
                "P2: SlotSource(P1) → Project: #0 COUNT(*), #1 SUM(#0 id) AS total → CollectSink"
            )
        );
        assert_eq!(physical.schema.fields().len(), 2);
        assert_eq!(physical.schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(physical.schema.field(1).name(), "total");
        assert_eq!(physical.schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn plans_grouped_aggregate_as_two_pipelines() {
        let physical =
            plan_sql("SELECT name, COUNT(*), SUM(id) FROM people GROUP BY name").unwrap();
        assert_eq!(
            physical.to_string(),
            concat!(
                "P1: Scan(people) → HashAggregateSink: group by [#1 name], aggregates [COUNT(*), SUM(#0 id)]\n",
                "P2: SlotSource(P1) → Project: #0 name, #1 COUNT(*), #2 SUM(#0 id) AS SUM(id) → CollectSink"
            )
        );
        assert_eq!(physical.schema.fields().len(), 3);
        assert_eq!(physical.schema.field(0).name(), "name");
        assert_eq!(physical.schema.field(0).data_type(), &DataType::Utf8View);
    }

    #[test]
    fn limit_zero_over_aggregate_collapses_to_empty_pipeline() {
        let physical = plan_sql("SELECT COUNT(*) AS cnt FROM people LIMIT 0").unwrap();
        assert_eq!(physical.to_string(), "P1: Empty → CollectSink");
        assert_eq!(physical.schema.fields().len(), 1);
        assert_eq!(physical.schema.field(0).name(), "cnt");
        assert_eq!(physical.schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn sort_is_not_supported_yet_even_under_limit_zero() {
        assert!(matches!(
            plan_sql("SELECT id FROM people ORDER BY id"),
            Err(EngineError::NotSupported(_))
        ));
        assert!(matches!(
            plan_sql("SELECT id FROM people ORDER BY id LIMIT 0"),
            Err(EngineError::NotSupported(_))
        ));
    }
}
