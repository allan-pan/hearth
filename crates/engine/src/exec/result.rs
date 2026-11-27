use arrow::record_batch::RecordBatch;

use crate::error::EngineResult;
use crate::exec::{Sink, SinkResult};

pub struct CollectSink;

impl Sink for CollectSink {
    type Global = Vec<RecordBatch>;
    type Local = Vec<RecordBatch>;
    type Output = Vec<RecordBatch>;

    fn init_global(&self) -> Self::Global {
        Vec::new()
    }

    fn init_local(&self, _global: &Self::Global) -> Self::Local {
        Vec::new()
    }

    fn sink(
        &self,
        _global: &Self::Global,
        local: &mut Self::Local,
        input: RecordBatch,
    ) -> EngineResult<SinkResult> {
        local.push(input);
        Ok(SinkResult::NeedMoreInput)
    }

    fn combine(&self, global: &mut Self::Global, local: Self::Local) -> EngineResult<()> {
        global.extend(local);
        Ok(())
    }

    fn finalize(&self, global: Self::Global) -> EngineResult<Self::Output> {
        Ok(global)
    }
}
