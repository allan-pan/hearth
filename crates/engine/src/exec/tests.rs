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

    fn init_global(&self, _slots: &[SlotValue]) {}
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
    type Output = RecordingGlobal;

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
    fn finalize(&self, mut global: RecordingGlobal) -> EngineResult<RecordingGlobal> {
        assert!(global.combined, "finalize must run after combine");
        global.finalized = true;
        Ok(global)
    }
}

#[test]
fn empty_source_still_calls_combine_and_finalize() {
    let (source, pulls) = CountingSource::new(vec![]);
    let sink = RecordingSink { stop_after: None };
    let out = drive::<_, TestOp, _>(&source, &[], &sink, &[]).unwrap();

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
    let out = drive(&source, &[TestOp::Repeat(3)], &sink, &[]).unwrap();

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
    let out = drive(&source, &[TestOp::StopAfter(2)], &sink, &[]).unwrap();

    assert_eq!(pulls.load(Ordering::SeqCst), 2);
    assert_eq!(collect_values(&out.batches), vec![1, 2]);
    assert!(out.combined && out.finalized);
}

#[test]
fn finished_downstream_of_have_more_output_stops_immediately() {
    let (source, pulls) = CountingSource::new(vec![int_batch(&[10]), int_batch(&[20])]);
    let sink = RecordingSink { stop_after: None };
    // Repeat(3) wants to emit [10] three times, but StopAfter(2) finishes on the 2nd.
    let out = drive(
        &source,
        &[TestOp::Repeat(3), TestOp::StopAfter(2)],
        &sink,
        &[],
    )
    .unwrap();

    assert_eq!(pulls.load(Ordering::SeqCst), 1);
    assert_eq!(collect_values(&out.batches), vec![10, 10]);
    assert!(out.combined && out.finalized);
}

#[test]
fn need_more_input_none_drops_batches_without_calling_sink() {
    let (source, pulls) = CountingSource::new(vec![int_batch(&[1]), int_batch(&[2])]);
    let sink = RecordingSink { stop_after: None };
    let out = drive(&source, &[TestOp::DropAll], &sink, &[]).unwrap();

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
    let out = drive::<_, TestOp, _>(&source, &[], &sink, &[]).unwrap();

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
    let out = drive(&source, &operators, &sink, &[]).unwrap();

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
    let SlotValue::Batches(batches) = run(&pipeline, &[]).unwrap();
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
        source: PhysicalSource::Scan(ScanSource::all_columns("t", runtime, snapshot)),
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

    let SlotValue::Batches(batches) = run(&pipeline, &[]).unwrap();
    assert_eq!(collect_values(&batches), vec![6, 8]);
    assert_eq!(batches[0].schema(), projected_schema);
}

#[test]
fn pipeline_handoff_via_slot_source_and_batch_slicing() {
    let (source1, _) = CountingSource::new(vec![int_batch(&[1, 2, 3, 4]), int_batch(&[5, 6, 7])]);
    let slot0 =
        drive::<_, TestOp, _>(&source1, &[], &PhysicalSink::Collect(CollectSink), &[]).unwrap();
    let slots = vec![slot0];

    let source2 = SlotSource::new(SlotId(0)).with_batch_size(3);
    let SlotValue::Batches(batches) =
        drive::<_, TestOp, _>(&source2, &[], &PhysicalSink::Collect(CollectSink), &slots).unwrap();

    let chunk_sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(chunk_sizes, vec![3, 1, 3]);
    assert_eq!(collect_values(&batches), vec![1, 2, 3, 4, 5, 6, 7]);
}
