//! Vectorized push-based pipeline driver, operators, and expression evaluation.

use std::fmt;

use arrow::record_batch::RecordBatch;
use storage::stream::BatchStream;

use crate::QueryResult;
use crate::error::{EngineError, EngineResult};
use crate::physical::PhysicalPlan;

mod aggregate;
mod eval;
mod filter;
mod limit;
mod project;
mod result;
mod scan;
#[cfg(test)]
mod tests;

pub use aggregate::AggregateSink;
pub use filter::FilterOp;
pub use limit::LimitOp;
pub use project::ProjectOp;
pub use result::CollectSink;
pub use scan::ScanSource;

const DEFAULT_BATCH_SIZE: usize = 8_192;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotId(pub usize);

pub enum SlotValue {
    Batches(Vec<RecordBatch>),
}

pub struct SlotSource {
    slot: SlotId,
    batch_size: usize,
}

impl SlotSource {
    pub fn new(slot: SlotId) -> Self {
        Self {
            slot,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    #[cfg(test)]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(batch_size > 0, "batch_size must be positive");
        self.batch_size = batch_size;
        self
    }
}

pub struct SlotSourceLocal {
    batch_idx: usize,
    row_offset: usize,
}

impl Source for SlotSource {
    type Global = Vec<RecordBatch>;
    type Local = SlotSourceLocal;

    fn init_global(&self, slots: &[SlotValue]) -> Self::Global {
        let SlotValue::Batches(batches) = &slots[self.slot.0];
        batches.clone()
    }

    fn init_local(&self, _global: &Self::Global) -> EngineResult<Self::Local> {
        Ok(SlotSourceLocal {
            batch_idx: 0,
            row_offset: 0,
        })
    }

    fn next(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>> {
        while let Some(batch) = global.get(local.batch_idx) {
            if local.row_offset >= batch.num_rows() {
                local.batch_idx += 1;
                local.row_offset = 0;
                continue;
            }
            let start = local.row_offset;
            let len = self.batch_size.min(batch.num_rows() - start);
            local.row_offset += len;
            return Ok(Some(batch.slice(start, len)));
        }
        Ok(None)
    }
}

pub enum PhysicalOperator {
    Filter(FilterOp),
    Project(ProjectOp),
    Limit(LimitOp),
}

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

pub enum OperatorResult {
    NeedMoreInput(Option<RecordBatch>),
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "no multi-output operator implemented yet")
    )]
    HaveMoreOutput(RecordBatch),
    Finished(Option<RecordBatch>),
}

pub enum SinkResult {
    NeedMoreInput,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "no early-finishing sink implemented yet")
    )]
    Finished,
}

pub trait Source {
    type Global;
    type Local;

    fn init_global(&self, slots: &[SlotValue]) -> Self::Global;
    fn init_local(&self, global: &Self::Global) -> EngineResult<Self::Local>;
    fn next(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>>;
}

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

pub trait Sink {
    type Global;
    type Local;
    type Output;

    fn init_global(&self) -> Self::Global;
    fn init_local(&self, global: &Self::Global) -> Self::Local;
    fn sink(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
        input: RecordBatch,
    ) -> EngineResult<SinkResult>;
    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()>;
    fn finalize(&self, global: Self::Global) -> EngineResult<Self::Output>;
}

pub enum PhysicalSource {
    Scan(ScanSource),
    Empty,
    Slot(SlotSource),
}

pub enum SourceGlobal {
    Scan,
    Empty,
    Slot(Vec<RecordBatch>),
}

pub enum SourceLocal {
    Scan(BatchStream),
    Empty,
    Slot(SlotSourceLocal),
}

impl Source for PhysicalSource {
    type Global = SourceGlobal;
    type Local = SourceLocal;

    fn init_global(&self, slots: &[SlotValue]) -> Self::Global {
        match self {
            PhysicalSource::Scan(_) => SourceGlobal::Scan,
            PhysicalSource::Empty => SourceGlobal::Empty,
            PhysicalSource::Slot(source) => SourceGlobal::Slot(source.init_global(slots)),
        }
    }

    fn init_local(&self, global: &Self::Global) -> EngineResult<Self::Local> {
        match (self, global) {
            (PhysicalSource::Scan(scan), SourceGlobal::Scan) => {
                Ok(SourceLocal::Scan(scan.init_local(&())?))
            }
            (PhysicalSource::Empty, SourceGlobal::Empty) => Ok(SourceLocal::Empty),
            (PhysicalSource::Slot(source), SourceGlobal::Slot(batches)) => {
                Ok(SourceLocal::Slot(source.init_local(batches)?))
            }
            _ => unreachable!("source and global state always match"),
        }
    }

    fn next(
        &self,
        global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>> {
        match (self, global, local) {
            (PhysicalSource::Scan(scan), SourceGlobal::Scan, SourceLocal::Scan(local)) => {
                scan.next(&(), local)
            }
            (PhysicalSource::Empty, SourceGlobal::Empty, SourceLocal::Empty) => Ok(None),
            (PhysicalSource::Slot(source), SourceGlobal::Slot(batches), SourceLocal::Slot(l)) => {
                source.next(batches, l)
            }
            _ => unreachable!("source, global, and local state always match"),
        }
    }
}

pub enum PhysicalSink {
    Collect(CollectSink),
    Aggregate(AggregateSink),
}

pub enum SinkGlobal {
    Collect(Vec<RecordBatch>),
    Aggregate(aggregate::AggregateGlobal),
}

pub enum SinkLocal {
    Collect(Vec<RecordBatch>),
    Aggregate(aggregate::AggregateLocal),
}

impl Sink for PhysicalSink {
    type Global = SinkGlobal;
    type Local = SinkLocal;
    type Output = SlotValue;

    fn init_global(&self) -> Self::Global {
        match self {
            PhysicalSink::Collect(sink) => SinkGlobal::Collect(sink.init_global()),
            PhysicalSink::Aggregate(sink) => SinkGlobal::Aggregate(sink.init_global()),
        }
    }

    fn init_local(&self, global: &Self::Global) -> Self::Local {
        match (self, global) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g)) => {
                SinkLocal::Collect(sink.init_local(g))
            }
            (PhysicalSink::Aggregate(sink), SinkGlobal::Aggregate(g)) => {
                SinkLocal::Aggregate(sink.init_local(g))
            }
            _ => unreachable!("sink and global state always match"),
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
            (PhysicalSink::Aggregate(sink), SinkGlobal::Aggregate(g), SinkLocal::Aggregate(l)) => {
                sink.sink(g, l, input)
            }
            _ => unreachable!("sink, global, and local state always match"),
        }
    }

    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()> {
        match (self, global, local) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g), SinkLocal::Collect(l)) => {
                sink.combine(g, l)
            }
            (PhysicalSink::Aggregate(sink), SinkGlobal::Aggregate(g), SinkLocal::Aggregate(l)) => {
                sink.combine(g, l)
            }
            _ => unreachable!("sink, global, and local state always match"),
        }
    }

    fn finalize(&self, global: Self::Global) -> EngineResult<Self::Output> {
        match (self, global) {
            (PhysicalSink::Collect(sink), SinkGlobal::Collect(g)) => {
                Ok(SlotValue::Batches(sink.finalize(g)?))
            }
            (PhysicalSink::Aggregate(sink), SinkGlobal::Aggregate(g)) => {
                Ok(SlotValue::Batches(sink.finalize(g)?))
            }
            _ => unreachable!("sink and global state always match"),
        }
    }
}

pub struct Pipeline {
    pub source: PhysicalSource,
    pub operators: Vec<PhysicalOperator>,
    pub sink: PhysicalSink,
}

impl fmt::Display for PhysicalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scan(scan) => write!(f, "Scan({})", scan.table),
            Self::Empty => write!(f, "Empty"),
            Self::Slot(source) => write!(f, "SlotSource(P{})", source.slot.0 + 1),
        }
    }
}

impl fmt::Display for PhysicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filter(filter) => write!(f, "Filter: {}", filter.predicate),
            Self::Project(project) => {
                write!(f, "Project: ")?;
                for (i, (expr, field)) in project
                    .exprs
                    .iter()
                    .zip(project.schema.fields())
                    .enumerate()
                {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    expr.fmt_aliased(f, field.name())?;
                }
                Ok(())
            }
            Self::Limit(limit) => {
                write!(f, "Limit: skip {}, fetch ", limit.skip)?;
                match limit.fetch {
                    Some(n) => write!(f, "{n}"),
                    None => write!(f, "all"),
                }
            }
        }
    }
}

impl fmt::Display for PhysicalSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Collect(_) => write!(f, "CollectSink"),
            Self::Aggregate(sink) => write!(f, "{sink}"),
        }
    }
}

impl fmt::Display for Pipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)?;
        for op in &self.operators {
            write!(f, " → {op}")?;
        }
        write!(f, " → {}", self.sink)
    }
}

pub fn execute(plan: PhysicalPlan) -> EngineResult<QueryResult> {
    let Some((last, rest)) = plan.pipelines.split_last() else {
        return Err(EngineError::Execution(
            "physical plan has no pipelines".to_string(),
        ));
    };
    if !matches!(last.sink, PhysicalSink::Collect(_)) {
        return Err(EngineError::Execution(
            "final pipeline must end in CollectSink".to_string(),
        ));
    }
    let mut completed = Vec::with_capacity(rest.len());
    for pipeline in rest {
        completed.push(run(pipeline, &completed)?);
    }
    let SlotValue::Batches(batches) = run(last, &completed)?;
    Ok(QueryResult {
        schema: plan.schema,
        batches,
    })
}

fn run(pipeline: &Pipeline, slots: &[SlotValue]) -> EngineResult<SlotValue> {
    drive(&pipeline.source, &pipeline.operators, &pipeline.sink, slots)
}

pub fn drive<S: Source, O: Operator, K: Sink>(
    source: &S,
    operators: &[O],
    sink: &K,
    slots: &[SlotValue],
) -> EngineResult<K::Output> {
    let source_global = source.init_global(slots);
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
    sink.finalize(sink_global)
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
