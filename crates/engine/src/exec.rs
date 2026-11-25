//! Vectorized push-based pipeline driver, operators, and expression evaluation.

use arrow::record_batch::RecordBatch;
use storage::stream::BatchStream;

use crate::error::EngineResult;

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod eval;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod filter;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod limit;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod project;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod result;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "execution operators not wired yet")
)]
mod scan;

pub use filter::FilterOp;
pub use limit::LimitOp;
pub use project::ProjectOp;
pub use result::CollectSink;
pub use scan::ScanSource;

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum PhysicalOperator {
    Filter(FilterOp),
    Project(ProjectOp),
    Limit(LimitOp),
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum OperatorLocal {
    Stateless,
    Limit(limit::LimitLocal),
}

impl Operator for PhysicalOperator {
    type Global = ();
    type Local = OperatorLocal;

    fn init_global(&self) -> Self::Global {}

    fn init_local(&self, _global: &Self::Global) -> Self::Local {
        match self {
            Self::Filter(_) | Self::Project(_) => OperatorLocal::Stateless,
            Self::Limit(op) => OperatorLocal::Limit(op.init_local()),
        }
    }

    fn execute(
        &self,
        _global: &Self::Global,
        local: &mut Self::Local,
        input: &RecordBatch,
    ) -> EngineResult<OperatorResult> {
        match (self, local) {
            (Self::Filter(op), OperatorLocal::Stateless) => op.execute(input),
            (Self::Project(op), OperatorLocal::Stateless) => op.execute(input),
            (Self::Limit(_), OperatorLocal::Limit(state)) => state.execute(input),
            _ => unreachable!("operator and local state always match"),
        }
    }
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum OperatorResult {
    NeedMoreInput(Option<RecordBatch>),
    HaveMoreOutput(RecordBatch),
    Finished(Option<RecordBatch>),
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum SinkResult {
    NeedMoreInput,
    Finished,
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub trait Source {
    type Global;
    type Local;

    fn init_global(&self) -> Self::Global;
    fn init_local(&self, global: &Self::Global) -> EngineResult<Self::Local>;
    fn next(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>>;
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub trait Operator {
    type Global;
    type Local;

    fn init_global(&self) -> Self::Global;
    fn init_local(&self, global: &Self::Global) -> Self::Local;
    fn execute(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
        input: &RecordBatch,
    ) -> EngineResult<OperatorResult>;
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub trait Sink {
    type Global;
    type Local;

    fn init_global(&self) -> Self::Global;
    fn init_local(&self, global: &Self::Global) -> Self::Local;
    fn sink(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
        input: RecordBatch,
    ) -> EngineResult<SinkResult>;
    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()>;
    fn finalize(&self, global: &mut Self::Global) -> EngineResult<()>;
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum PhysicalSource {
    Scan(ScanSource),
    Empty,
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum SourceLocal {
    Scan(BatchStream),
    Empty,
}

impl Source for PhysicalSource {
    type Global = ();
    type Local = SourceLocal;

    fn init_global(&self) -> Self::Global {}

    fn init_local(&self, global: &Self::Global) -> EngineResult<Self::Local> {
        match self {
            PhysicalSource::Scan(scan) => Ok(SourceLocal::Scan(scan.init_local(global)?)),
            PhysicalSource::Empty => Ok(SourceLocal::Empty),
        }
    }

    fn next(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>> {
        match (self, local) {
            (PhysicalSource::Scan(scan), SourceLocal::Scan(local)) => scan.next(global, local),
            (PhysicalSource::Empty, SourceLocal::Empty) => Ok(None),
            _ => unreachable!("source and local state always match"),
        }
    }
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum PhysicalSink {
    Collect(CollectSink),
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum SinkGlobal {
    Collect(Vec<RecordBatch>),
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub enum SinkLocal {
    Collect(Vec<RecordBatch>),
}

impl Sink for PhysicalSink {
    type Global = SinkGlobal;
    type Local = SinkLocal;

    fn init_global(&self) -> Self::Global {
        match self {
            PhysicalSink::Collect(sink) => SinkGlobal::Collect(sink.init_global()),
        }
    }
    fn init_local(&self, global: &Self::Global) -> Self::Local {
        match (self, global) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g)) => {
                SinkLocal::Collect(sink.init_local(g))
            }
        }
    }
    fn sink(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
        input: RecordBatch,
    ) -> EngineResult<SinkResult> {
        match (self, global, local) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g), SinkLocal::Collect(l)) => {
                sink.sink(g, l, input)
            }
        }
    }
    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()> {
        match (self, global, local) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g), SinkLocal::Collect(l)) => {
                sink.combine(g, l)
            }
        }
    }
    fn finalize(&self, global: &mut Self::Global) -> EngineResult<()> {
        match (self, global) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g)) => sink.finalize(g),
        }
    }
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub struct Pipeline {
    pub source: PhysicalSource,
    pub operators: Vec<PhysicalOperator>,
    pub sink: PhysicalSink,
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub fn run(pipeline: &Pipeline) -> EngineResult<SinkGlobal> {
    drive(&pipeline.source, &pipeline.operators, &pipeline.sink)
}

#[cfg_attr(not(test), expect(dead_code, reason = "pipeline not wired yet"))]
pub fn drive<S: Source, O: Operator, K: Sink>(
    source: &S,
    operators: &[O],
    sink: &K,
) -> EngineResult<K::Global> {
    let source_global = source.init_global();
    let mut source_local = source.init_local(&source_global)?;

    let op_globals: Vec<O::Global> = operators.iter().map(Operator::init_global).collect();
    let mut op_locals: Vec<O::Local> = operators
        .iter()
        .zip(&op_globals)
        .map(|(op, g)| op.init_local(g))
        .collect();

    let mut sink_global = sink.init_global();
    let mut sink_local = sink.init_local(&sink_global);

    while let Some(batch) = source.next(&source_global, &mut source_local)? {
        let finished = push_from(
            0,
            operators,
            &op_globals,
            &mut op_locals,
            sink,
            &sink_global,
            &mut sink_local,
            batch,
        )?;
        if finished {
            break;
        }
    }

    drop(source_local);
    drop(source_global);

    sink.combine(&mut sink_global, sink_local)?;
    sink.finalize(&mut sink_global)?;
    Ok(sink_global)
}

#[allow(clippy::too_many_arguments)]
fn push_from<O: Operator, K: Sink>(
    op_idx: usize,
    operators: &[O],
    op_globals: &[O::Global],
    op_locals: &mut [O::Local],
    sink: &K,
    sink_global: &K::Global,
    sink_local: &mut K::Local,
    batch: RecordBatch,
) -> EngineResult<bool> {
    if op_idx == operators.len() {
        return Ok(matches!(
            sink.sink(sink_global, sink_local, batch)?,
            SinkResult::Finished
        ));
    }

    loop {
        match operators[op_idx].execute(&op_globals[op_idx], &mut op_locals[op_idx], &batch)? {
            OperatorResult::NeedMoreInput(None) => return Ok(false),
            OperatorResult::NeedMoreInput(Some(out)) => {
                return push_from(
                    op_idx + 1,
                    operators,
                    op_globals,
                    op_locals,
                    sink,
                    sink_global,
                    sink_local,
                    out,
                );
            }
            OperatorResult::HaveMoreOutput(out) => {
                let finished = push_from(
                    op_idx + 1,
                    operators,
                    op_globals,
                    op_locals,
                    sink,
                    sink_global,
                    sink_local,
                    out,
                )?;
                if finished {
                    return Ok(true);
                }
            }
            OperatorResult::Finished(out) => {
                if let Some(out) = out {
                    push_from(
                        op_idx + 1,
                        operators,
                        op_globals,
                        op_locals,
                        sink,
                        sink_global,
                        sink_local,
                        out,
                    )?;
                }
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn int_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn collect_values(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    struct CountingSource {
        batches: Vec<RecordBatch>,
        pulls: Arc<AtomicUsize>,
    }

    impl CountingSource {
        fn new(batches: Vec<RecordBatch>) -> (Self, Arc<AtomicUsize>) {
            let pulls = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    batches,
                    pulls: pulls.clone(),
                },
                pulls,
            )
        }
    }

    impl Source for CountingSource {
        type Global = ();
        type Local = usize;

        fn init_global(&self) {}
        fn init_local(&self, _global: &()) -> EngineResult<usize> {
            Ok(0)
        }
        fn next(&self, _global: &(), index: &mut usize) -> EngineResult<Option<RecordBatch>> {
            self.pulls.fetch_add(1, Ordering::SeqCst);
            let batch = self.batches.get(*index).cloned();
            if batch.is_some() {
                *index += 1;
            }
            Ok(batch)
        }
    }

    enum TestOp {
        /// Emits `HaveMoreOutput` `n - 1` times, then `NeedMoreInput(Some)` on the `n`-th call.
        Repeat(usize),
        /// Emits `Finished(Some)` on the `n`-th batch (or `Finished(None)` if `n == 0`).
        StopAfter(usize),
        /// Drops every batch with `NeedMoreInput(None)`.
        DropAll,
    }

    enum TestOpLocal {
        Repeat { emitted: usize },
        StopAfter { seen: usize },
        DropAll,
    }

    impl Operator for TestOp {
        type Global = ();
        type Local = TestOpLocal;

        fn init_global(&self) {}
        fn init_local(&self, _global: &()) -> TestOpLocal {
            match self {
                Self::Repeat(_) => TestOpLocal::Repeat { emitted: 0 },
                Self::StopAfter(_) => TestOpLocal::StopAfter { seen: 0 },
                Self::DropAll => TestOpLocal::DropAll,
            }
        }
        fn execute(
            &self,
            _global: &(),
            local: &mut TestOpLocal,
            input: &RecordBatch,
        ) -> EngineResult<OperatorResult> {
            match (self, local) {
                (Self::Repeat(times), TestOpLocal::Repeat { emitted }) => {
                    *emitted += 1;
                    if *emitted < *times {
                        Ok(OperatorResult::HaveMoreOutput(input.clone()))
                    } else {
                        *emitted = 0;
                        Ok(OperatorResult::NeedMoreInput(Some(input.clone())))
                    }
                }
                (Self::StopAfter(limit), TestOpLocal::StopAfter { seen }) => {
                    if *limit == 0 {
                        return Ok(OperatorResult::Finished(None));
                    }
                    *seen += 1;
                    if *seen == *limit {
                        Ok(OperatorResult::Finished(Some(input.clone())))
                    } else {
                        Ok(OperatorResult::NeedMoreInput(Some(input.clone())))
                    }
                }
                (Self::DropAll, TestOpLocal::DropAll) => Ok(OperatorResult::NeedMoreInput(None)),
                _ => unreachable!(),
            }
        }
    }

    struct RecordingSink {
        stop_after: Option<usize>,
    }

    struct RecordingGlobal {
        batches: Vec<RecordBatch>,
        combined: bool,
        finalized: bool,
    }

    impl Sink for RecordingSink {
        type Global = RecordingGlobal;
        type Local = Vec<RecordBatch>;

        fn init_global(&self) -> RecordingGlobal {
            RecordingGlobal {
                batches: Vec::new(),
                combined: false,
                finalized: false,
            }
        }
        fn init_local(&self, _global: &RecordingGlobal) -> Vec<RecordBatch> {
            Vec::new()
        }
        fn sink(
            &self,
            _global: &RecordingGlobal,
            local: &mut Vec<RecordBatch>,
            input: RecordBatch,
        ) -> EngineResult<SinkResult> {
            local.push(input);
            if self.stop_after == Some(local.len()) {
                Ok(SinkResult::Finished)
            } else {
                Ok(SinkResult::NeedMoreInput)
            }
        }
        fn combine(
            &self,
            global: &mut RecordingGlobal,
            local: Vec<RecordBatch>,
        ) -> EngineResult<()> {
            assert!(!global.finalized, "combine must run before finalize");
            global.batches.extend(local);
            global.combined = true;
            Ok(())
        }
        fn finalize(&self, global: &mut RecordingGlobal) -> EngineResult<()> {
            assert!(global.combined, "finalize must run after combine");
            global.finalized = true;
            Ok(())
        }
    }

    #[test]
    fn empty_source_still_calls_combine_and_finalize() {
        let (source, pulls) = CountingSource::new(vec![]);
        let sink = RecordingSink { stop_after: None };
        let out = drive::<_, TestOp, _>(&source, &[], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        assert!(out.batches.is_empty());
        assert!(out.combined);
        assert!(out.finalized);
    }

    #[test]
    fn have_more_output_recalls_operator_with_same_input() {
        let (source, pulls) = CountingSource::new(vec![int_batch(&[1, 2]), int_batch(&[3])]);
        let sink = RecordingSink { stop_after: None };
        // Repeat(3) emits HaveMoreOutput twice, then NeedMoreInput(Some) on the 3rd call.
        let out = drive(&source, &[TestOp::Repeat(3)], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 3); // 2 batches + final None
        assert_eq!(
            collect_values(&out.batches),
            vec![1, 2, 1, 2, 1, 2, 3, 3, 3]
        );
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn finished_operator_stops_pulling_from_source() {
        let (source, pulls) = CountingSource::new(vec![
            int_batch(&[1]),
            int_batch(&[2]),
            int_batch(&[3]),
            int_batch(&[4]),
        ]);
        let sink = RecordingSink { stop_after: None };
        let out = drive(&source, &[TestOp::StopAfter(2)], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 2);
        assert_eq!(collect_values(&out.batches), vec![1, 2]);
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn finished_downstream_of_have_more_output_stops_immediately() {
        let (source, pulls) = CountingSource::new(vec![int_batch(&[10]), int_batch(&[20])]);
        let sink = RecordingSink { stop_after: None };
        // Repeat(3) wants to emit [10] three times, but StopAfter(2) finishes on the 2nd.
        let out = drive(&source, &[TestOp::Repeat(3), TestOp::StopAfter(2)], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        assert_eq!(collect_values(&out.batches), vec![10, 10]);
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn need_more_input_none_drops_batches_without_calling_sink() {
        let (source, pulls) = CountingSource::new(vec![int_batch(&[1]), int_batch(&[2])]);
        let sink = RecordingSink { stop_after: None };
        let out = drive(&source, &[TestOp::DropAll], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 3);
        assert!(out.batches.is_empty());
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn sink_finished_stops_pulling_from_source() {
        let (source, pulls) =
            CountingSource::new(vec![int_batch(&[1]), int_batch(&[2]), int_batch(&[3])]);
        let sink = RecordingSink {
            stop_after: Some(1),
        };
        let out = drive::<_, TestOp, _>(&source, &[], &sink).unwrap();

        assert_eq!(pulls.load(Ordering::SeqCst), 1);
        assert_eq!(collect_values(&out.batches), vec![1]);
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn physical_operator_pipeline_filters_projects_and_limits_early() {
        use crate::expr::{BinaryOp, Expr, ScalarValue};

        let (source, pulls) = CountingSource::new(vec![
            int_batch(&[1, 2, 3]),
            int_batch(&[4, 5, 6]),
            int_batch(&[7, 8, 9]),
        ]);
        let projected_schema = Arc::new(Schema::new(vec![Field::new(
            "doubled",
            DataType::Int64,
            false,
        )]));
        let x = || Expr::Column {
            index: 0,
            name: "x".into(),
        };
        let operators = [
            PhysicalOperator::Filter(FilterOp {
                predicate: Expr::Binary {
                    left: Box::new(x()),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                },
            }),
            PhysicalOperator::Project(ProjectOp {
                exprs: vec![Expr::Binary {
                    left: Box::new(x()),
                    op: BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                }],
                schema: projected_schema.clone(),
            }),
            PhysicalOperator::Limit(LimitOp {
                skip: 1,
                fetch: Some(2),
            }),
        ];
        let sink = RecordingSink { stop_after: None };
        let out = drive(&source, &operators, &sink).unwrap();

        // Batch 1 yields [3] (skipped), Batch 2 yields [4, 5, 6] -> emits [8, 10] and finishes.
        // Batch 3 is never pulled.
        assert_eq!(pulls.load(Ordering::SeqCst), 2);
        assert_eq!(collect_values(&out.batches), vec![8, 10]);
        assert_eq!(out.batches[0].schema(), projected_schema);
        assert!(out.combined && out.finalized);
    }

    #[test]
    fn run_empty_pipeline_returns_empty_batches() {
        let pipeline = Pipeline {
            source: PhysicalSource::Empty,
            operators: vec![],
            sink: PhysicalSink::Collect(CollectSink),
        };
        let SinkGlobal::Collect(batches) = run(&pipeline).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn run_scan_pipeline_filters_projects_and_collects() {
        use storage::runtime::StorageRuntime;
        use storage::table::Snapshot;
        use storage::write::write_tablefile;

        use crate::expr::{BinaryOp, Expr, ScalarValue};

        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        let dir = tempfile::tempdir().unwrap();
        let batch = int_batch(&[1, 2, 3, 4, 5]);
        let schema = batch.schema();
        let file = write_tablefile(&runtime, dir.path(), 1, batch).unwrap();
        let snapshot = Snapshot {
            version: 0,
            schema,
            files: vec![file],
        };

        let projected_schema = Arc::new(Schema::new(vec![Field::new(
            "doubled",
            DataType::Int64,
            false,
        )]));
        let x = || Expr::Column {
            index: 0,
            name: "x".into(),
        };
        let pipeline = Pipeline {
            source: PhysicalSource::Scan(ScanSource::all_columns(runtime, snapshot)),
            operators: vec![
                PhysicalOperator::Filter(FilterOp {
                    predicate: Expr::Binary {
                        left: Box::new(x()),
                        op: BinaryOp::Gt,
                        right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                    },
                }),
                PhysicalOperator::Project(ProjectOp {
                    exprs: vec![Expr::Binary {
                        left: Box::new(x()),
                        op: BinaryOp::Multiply,
                        right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
                    }],
                    schema: projected_schema.clone(),
                }),
                PhysicalOperator::Limit(LimitOp {
                    skip: 0,
                    fetch: Some(2),
                }),
            ],
            sink: PhysicalSink::Collect(CollectSink),
        };

        let SinkGlobal::Collect(batches) = run(&pipeline).unwrap();
        assert_eq!(collect_values(&batches), vec![6, 8]);
        assert_eq!(batches[0].schema(), projected_schema);
    }
}
