use arrow::record_batch::RecordBatch;

use crate::error::EngineResult;
use crate::exec::OperatorResult;

pub struct LimitOp {
    pub skip: usize,
    pub fetch: Option<usize>,
}

pub struct LimitLocal {
    remaining_skip: usize,
    remaining_fetch: Option<usize>,
}

impl LimitOp {
    pub fn init_local(&self) -> LimitLocal {
        LimitLocal {
            remaining_skip: self.skip,
            remaining_fetch: self.fetch,
        }
    }
}

impl LimitLocal {
    pub fn execute(&mut self, input: &RecordBatch) -> EngineResult<OperatorResult> {
        if self.remaining_fetch == Some(0) {
            return Ok(OperatorResult::Finished(None));
        }
        let num_rows = input.num_rows();
        if self.remaining_skip >= num_rows {
            self.remaining_skip -= num_rows;
            return Ok(OperatorResult::NeedMoreInput(None));
        }

        let offset = std::mem::take(&mut self.remaining_skip);
        let avail = num_rows - offset;

        match &mut self.remaining_fetch {
            None => Ok(OperatorResult::NeedMoreInput(Some(
                input.slice(offset, avail),
            ))),
            Some(rem) => {
                let take = avail.min(*rem);
                *rem -= take;
                let sliced = input.slice(offset, take);
                if *rem == 0 {
                    Ok(OperatorResult::Finished(Some(sliced)))
                } else {
                    Ok(OperatorResult::NeedMoreInput(Some(sliced)))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn int_batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn values(batches: &[RecordBatch]) -> Vec<i64> {
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

    fn run_limit(skip: usize, fetch: Option<usize>, inputs: &[&[i64]]) -> (Vec<i64>, bool) {
        let mut state = LimitOp { skip, fetch }.init_local();
        let mut out = Vec::new();
        let mut finished = false;
        for rows in inputs {
            let batch = int_batch(rows);
            match state.execute(&batch).unwrap() {
                OperatorResult::NeedMoreInput(Some(b)) => out.push(b),
                OperatorResult::NeedMoreInput(None) => {}
                OperatorResult::Finished(opt) => {
                    if let Some(b) = opt {
                        out.push(b);
                    }
                    finished = true;
                    break;
                }
                OperatorResult::HaveMoreOutput(_) => panic!("LimitOp never returns HaveMoreOutput"),
            }
        }
        (values(&out), finished)
    }

    #[test]
    fn fetch_zero_finishes_immediately_without_output() {
        let (rows, finished) = run_limit(0, Some(0), &[&[1, 2, 3]]);
        assert!(rows.is_empty());
        assert!(finished);
    }

    #[test]
    fn offset_without_limit_passes_all_remaining_rows() {
        let (rows, finished) = run_limit(2, None, &[&[0, 1, 2], &[3, 4]]);
        assert_eq!(rows, vec![2, 3, 4]);
        assert!(!finished);
    }

    #[test]
    fn offset_consumes_whole_batches_and_exceeding_all_rows_emits_nothing() {
        let (rows, finished) = run_limit(5, Some(4), &[&[0, 1, 2], &[3, 4, 5, 6, 7], &[8, 9, 10]]);
        assert_eq!(rows, vec![5, 6, 7, 8]);
        assert!(finished);

        let (rows, finished) = run_limit(100, Some(5), &[&[0, 1, 2], &[3, 4]]);
        assert!(rows.is_empty());
        assert!(!finished);
    }

    #[test]
    fn limit_ending_at_exact_batch_boundary_returns_finished() {
        let (rows, finished) = run_limit(0, Some(4), &[&[1, 2], &[3, 4], &[5, 6]]);
        assert_eq!(rows, vec![1, 2, 3, 4]);
        assert!(finished);
    }

    #[test]
    fn limit_ending_inside_a_batch_slices_and_finishes() {
        let (rows, finished) = run_limit(1, Some(2), &[&[10, 20, 30, 40]]);
        assert_eq!(rows, vec![20, 30]);
        assert!(finished);
    }

    #[test]
    fn execute_after_finished_returns_finished_none() {
        let mut state = LimitOp {
            skip: 0,
            fetch: Some(1),
        }
        .init_local();
        assert!(matches!(
            state.execute(&int_batch(&[1, 2])).unwrap(),
            OperatorResult::Finished(Some(_))
        ));
        assert!(matches!(
            state.execute(&int_batch(&[3, 4])).unwrap(),
            OperatorResult::Finished(None)
        ));
    }
}