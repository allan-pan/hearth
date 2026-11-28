use std::fmt;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::{EngineError, EngineResult};
use crate::exec::eval::{self, ColumnarValue};
use crate::exec::{Sink, SinkResult};
use crate::expr::{AggExpr, Expr};

pub mod accumulator;
pub mod table;

use accumulator::Accumulator;
use table::{GroupConverter, GroupTable};

pub struct AggregateSink {
    grouping: Grouping,
    aggregates: Vec<AggExpr>,
    input_schema: SchemaRef,
    schema: SchemaRef,
}

enum Grouping {
    Ungrouped,
    Hash {
        group_by: Vec<Expr>,
        converter: GroupConverter,
    },
}

pub enum AggregateLocal {
    Ungrouped(Vec<Accumulator>),
    Hash {
        table: GroupTable,
        accumulators: Vec<Accumulator>,
        group_ids: Vec<u32>,
    },
}

pub enum AggregateGlobal {
    Ungrouped(Option<Vec<Accumulator>>),
    Hash(Option<(GroupTable, Vec<Accumulator>)>),
}

impl AggregateSink {
    pub fn new(
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
        input_schema: SchemaRef,
        schema: SchemaRef,
    ) -> EngineResult<Self> {
        if group_by.is_empty() && aggregates.is_empty() {
            return Err(EngineError::Execution(
                "aggregate requires at least one group key or aggregate expression".to_string(),
            ));
        }
        for agg in &aggregates {
            Accumulator::new(agg, &input_schema)?;
        }
        let grouping = if group_by.is_empty() {
            Grouping::Ungrouped
        } else {
            let key_types: Vec<_> = group_by
                .iter()
                .map(|expr| expr.data_type(&input_schema))
                .collect();
            let converter = table::build_converter(&key_types)?;
            Grouping::Hash {
                group_by,
                converter,
            }
        };
        Ok(Self {
            grouping,
            aggregates,
            input_schema,
            schema,
        })
    }

    fn init_accumulators(&self, initial_groups: usize) -> Vec<Accumulator> {
        self.aggregates
            .iter()
            .map(|agg| {
                let mut acc =
                    Accumulator::new(agg, &self.input_schema).expect("validated in constructor");
                if initial_groups > 0 {
                    acc.resize(initial_groups);
                }
                acc
            })
            .collect()
    }
}

impl Sink for AggregateSink {
    type Global = AggregateGlobal;
    type Local = AggregateLocal;
    type Output = Vec<RecordBatch>;

    fn init_global(&self) -> Self::Global {
        match &self.grouping {
            Grouping::Ungrouped => AggregateGlobal::Ungrouped(None),
            Grouping::Hash { .. } => AggregateGlobal::Hash(None),
        }
    }

    fn init_local(&self, _global: &Self::Global) -> Self::Local {
        match &self.grouping {
            Grouping::Ungrouped => AggregateLocal::Ungrouped(self.init_accumulators(1)),
            Grouping::Hash { converter, .. } => AggregateLocal::Hash {
                table: GroupTable::with_converter(Arc::clone(converter)),
                accumulators: self.init_accumulators(0),
                group_ids: Vec::new(),
            },
        }
    }

    fn sink(
        &self,
        _global: &Self::Global,
        local: &mut Self::Local,
        input: RecordBatch,
    ) -> EngineResult<SinkResult> {
        let num_rows = input.num_rows();
        if num_rows == 0 {
            return Ok(SinkResult::NeedMoreInput);
        }

        match (&self.grouping, local) {
            (Grouping::Ungrouped, AggregateLocal::Ungrouped(accumulators)) => {
                for (agg, acc) in self.aggregates.iter().zip(accumulators) {
                    let value = eval_agg_arg(agg, &input)?;
                    acc.update_single(value.as_ref(), num_rows)?;
                }
            }
            (
                Grouping::Hash { group_by, .. },
                AggregateLocal::Hash {
                    table,
                    accumulators,
                    group_ids,
                },
            ) => {
                let key_arrays: Vec<ArrayRef> = group_by
                    .iter()
                    .map(|expr| eval::evaluate(expr, &input)?.into_array(num_rows))
                    .collect::<EngineResult<_>>()?;
                table.group_ids(&key_arrays, group_ids)?;

                let num_groups = table.len();
                for acc in accumulators.iter_mut() {
                    acc.resize(num_groups);
                }
                for (agg, acc) in self.aggregates.iter().zip(accumulators) {
                    let value = eval_agg_arg(agg, &input)?;
                    acc.update_batch(value.as_ref(), group_ids)?;
                }
            }
            _ => unreachable!("aggregate grouping mode and local state always match"),
        }
        Ok(SinkResult::NeedMoreInput)
    }

    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()> {
        match (global, local) {
            (AggregateGlobal::Ungrouped(target), AggregateLocal::Ungrouped(source_accs)) => {
                match target {
                    None => *target = Some(source_accs),
                    Some(target_accs) => {
                        for (target_acc, source_acc) in target_accs.iter_mut().zip(source_accs) {
                            target_acc.merge_from(source_acc, &[0])?;
                        }
                    }
                }
            }
            (
                AggregateGlobal::Hash(target),
                AggregateLocal::Hash {
                    table: source_table,
                    accumulators: source_accs,
                    ..
                },
            ) => {
                if source_table.is_empty() {
                    return Ok(());
                }
                match target {
                    None => *target = Some((source_table, source_accs)),
                    Some((target_table, target_accs)) => {
                        let group_map = target_table.merge_from(source_table);
                        let num_groups = target_table.len();
                        for acc in target_accs.iter_mut() {
                            acc.resize(num_groups);
                        }
                        for (target_acc, source_acc) in target_accs.iter_mut().zip(source_accs) {
                            target_acc.merge_from(source_acc, &group_map)?;
                        }
                    }
                }
            }
            _ => unreachable!("aggregate global and local state always match"),
        }
        Ok(())
    }

    fn finalize(&self, global: Self::Global) -> EngineResult<Self::Output> {
        let columns = match global {
            AggregateGlobal::Ungrouped(accumulators) => {
                let accumulators = accumulators.unwrap_or_else(|| self.init_accumulators(1));
                accumulators
                    .into_iter()
                    .map(Accumulator::finalize)
                    .collect::<EngineResult<Vec<_>>>()?
            }
            AggregateGlobal::Hash(state) => {
                let Some((table, accumulators)) = state else {
                    return Ok(Vec::new());
                };
                debug_assert!(!table.is_empty());
                let mut columns = table.into_arrays()?;
                for acc in accumulators {
                    columns.push(acc.finalize()?);
                }
                columns
            }
        };
        Ok(vec![RecordBatch::try_new(self.schema.clone(), columns)?])
    }
}

impl fmt::Display for AggregateSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.grouping {
            Grouping::Ungrouped => {
                write!(f, "UngroupedAggregateSink: aggregates [")?;
                fmt_list(f, &self.aggregates)?;
                write!(f, "]")
            }
            Grouping::Hash { group_by, .. } => {
                write!(f, "HashAggregateSink: group by [")?;
                fmt_list(f, group_by)?;
                write!(f, "], aggregates [")?;
                fmt_list(f, &self.aggregates)?;
                write!(f, "]")
            }
        }
    }
}

fn fmt_list<T: fmt::Display>(f: &mut fmt::Formatter<'_>, items: &[T]) -> fmt::Result {
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{item}")?;
    }
    Ok(())
}

fn eval_agg_arg(agg: &AggExpr, batch: &RecordBatch) -> EngineResult<Option<ColumnarValue>> {
    match agg {
        AggExpr::CountAll => Ok(None),
        AggExpr::Call { expr, .. } => Ok(Some(eval::evaluate(expr, batch)?)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};

    use super::*;
    use crate::exec::{
        CollectSink, PhysicalSink, PhysicalSource, Pipeline, SlotId, SlotSource, SlotValue, Source,
        execute,
    };
    use crate::expr::AggFunc;
    use crate::physical::PhysicalPlan;

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("dept", DataType::Utf8View, true),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    fn make_batch(depts: &[Option<&str>], amounts: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(StringViewArray::from(depts.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn col(index: usize, name: &str) -> Expr {
        Expr::Column {
            index,
            name: name.into(),
        }
    }

    #[test]
    fn ungrouped_aggregate_on_empty_input_emits_one_row() {
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::Int64, false),
            Field::new("total", DataType::Int64, true),
        ]));
        let sink = AggregateSink::new(
            vec![],
            vec![
                AggExpr::CountAll,
                AggExpr::Call {
                    func: AggFunc::Sum,
                    expr: col(1, "amount"),
                },
            ],
            input_schema(),
            out_schema.clone(),
        )
        .unwrap();

        let plan = PhysicalPlan {
            schema: out_schema,
            pipelines: vec![
                Pipeline {
                    source: PhysicalSource::Empty,
                    operators: vec![],
                    sink: PhysicalSink::Aggregate(sink),
                },
                Pipeline {
                    source: PhysicalSource::Slot(SlotSource::new(SlotId(0))),
                    operators: vec![],
                    sink: PhysicalSink::Collect(CollectSink),
                },
            ],
        };

        let result = execute(plan).unwrap();
        assert_eq!(result.batches.len(), 1);
        let batch = &result.batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 0);
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn ungrouped_aggregate_combines_multiple_locals() {
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::Int64, false),
            Field::new("total", DataType::Int64, true),
        ]));
        let sink = AggregateSink::new(
            vec![],
            vec![
                AggExpr::CountAll,
                AggExpr::Call {
                    func: AggFunc::Sum,
                    expr: col(1, "amount"),
                },
            ],
            input_schema(),
            out_schema,
        )
        .unwrap();

        let mut global = sink.init_global();
        let mut local1 = sink.init_local(&global);
        let mut local2 = sink.init_local(&global);

        sink.sink(
            &global,
            &mut local1,
            make_batch(&[Some("a"), Some("b")], &[Some(10), None]),
        )
        .unwrap();
        sink.sink(&global, &mut local2, make_batch(&[Some("c")], &[Some(25)]))
            .unwrap();

        sink.combine(&mut global, local1).unwrap();
        sink.combine(&mut global, local2).unwrap();
        let batches = sink.finalize(global).unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_primitive::<Int64Type>().value(0), 3);
        assert_eq!(batch.column(1).as_primitive::<Int64Type>().value(0), 35);
    }

    #[test]
    fn hash_aggregate_on_empty_input_emits_zero_rows() {
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("dept", DataType::Utf8View, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let sink = AggregateSink::new(
            vec![col(0, "dept")],
            vec![AggExpr::CountAll],
            input_schema(),
            out_schema.clone(),
        )
        .unwrap();

        let plan = PhysicalPlan {
            schema: out_schema,
            pipelines: vec![
                Pipeline {
                    source: PhysicalSource::Empty,
                    operators: vec![],
                    sink: PhysicalSink::Aggregate(sink),
                },
                Pipeline {
                    source: PhysicalSource::Slot(SlotSource::new(SlotId(0))),
                    operators: vec![],
                    sink: PhysicalSink::Collect(CollectSink),
                },
            ],
        };

        let result = execute(plan).unwrap();
        assert!(result.batches.is_empty());
    }

    #[test]
    fn hash_aggregate_with_zero_aggregates_and_multi_local_combine() {
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "dept",
            DataType::Utf8View,
            true,
        )]));
        let sink =
            AggregateSink::new(vec![col(0, "dept")], vec![], input_schema(), out_schema).unwrap();

        let mut global = sink.init_global();
        let mut local1 = sink.init_local(&global);
        let mut local2 = sink.init_local(&global);

        sink.sink(
            &global,
            &mut local1,
            make_batch(
                &[Some("eng"), Some("sales"), None],
                &[Some(1), Some(2), Some(3)],
            ),
        )
        .unwrap();
        sink.sink(
            &global,
            &mut local2,
            make_batch(
                &[Some("sales"), Some("ops"), None],
                &[Some(4), Some(5), Some(6)],
            ),
        )
        .unwrap();

        sink.combine(&mut global, local1).unwrap();
        sink.combine(&mut global, local2).unwrap();
        let batches = sink.finalize(global).unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 1);
        let depts: Vec<Option<&str>> = batch.column(0).as_string_view().iter().collect();
        assert_eq!(depts, vec![Some("eng"), Some("sales"), None, Some("ops")]);
    }

    #[test]
    fn aggregate_rejects_empty_group_by_and_empty_aggregates() {
        let err = AggregateSink::new(vec![], vec![], input_schema(), Arc::new(Schema::empty()))
            .err()
            .expect("empty aggregate must fail");
        assert!(matches!(err, EngineError::Execution(_)));
    }

    #[test]
    fn slot_source_slices_aggregate_output_into_configured_batch_size() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let sink =
            AggregateSink::new(vec![col(0, "id")], vec![], schema.clone(), schema.clone()).unwrap();

        let mut global = sink.init_global();
        let mut local = sink.init_local(&global);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..10_i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        sink.sink(&global, &mut local, batch).unwrap();
        sink.combine(&mut global, local).unwrap();
        let output = sink.finalize(global).unwrap();

        let slots = vec![SlotValue::Batches(output)];
        let source = SlotSource::new(SlotId(0)).with_batch_size(3);
        let source_global = source.init_global(&slots);
        let mut source_local = source.init_local(&source_global).unwrap();

        let mut chunk_sizes = Vec::new();
        let mut collected = Vec::new();
        while let Some(chunk) = source.next(&source_global, &mut source_local).unwrap() {
            chunk_sizes.push(chunk.num_rows());
            collected.extend_from_slice(chunk.column(0).as_primitive::<Int64Type>().values());
        }

        assert_eq!(chunk_sizes, vec![3, 3, 3, 1]);
        assert_eq!(collected, (0..10_i64).collect::<Vec<_>>());
    }
}
