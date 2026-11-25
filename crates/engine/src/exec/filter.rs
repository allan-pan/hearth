use arrow::array::{Array, AsArray};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;

use crate::error::EngineResult;
use crate::exec::OperatorResult;
use crate::exec::eval::{ColumnarValue, evaluate};
use crate::expr::Expr;

pub struct FilterOp {
    pub predicate: Expr,
}

impl FilterOp {
    pub fn execute(&self, input: &RecordBatch) -> EngineResult<OperatorResult> {
        match evaluate(&self.predicate, input)? {
            ColumnarValue::Scalar(a) => {
                let b = a.as_boolean();
                if b.is_valid(0) && b.value(0) && input.num_rows() > 0 {
                    Ok(OperatorResult::NeedMoreInput(Some(input.clone())))
                } else {
                    Ok(OperatorResult::NeedMoreInput(None))
                }
            }
            ColumnarValue::Array(mask) => {
                let filtered = filter_record_batch(input, mask.as_boolean())?;
                if filtered.num_rows() == 0 {
                    Ok(OperatorResult::NeedMoreInput(None))
                } else {
                    Ok(OperatorResult::NeedMoreInput(Some(filtered)))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::expr::{BinaryOp, ScalarValue};

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8View, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(StringViewArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    fn run_filter(predicate: Expr, batch: &RecordBatch) -> OperatorResult {
        FilterOp { predicate }.execute(batch).unwrap()
    }

    #[test]
    fn filters_rows_and_treats_null_as_false() {
        let batch = sample_batch();
        let pred = Expr::Binary {
            left: Box::new(Expr::Column {
                index: 0,
                name: "id".into(),
            }),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        };
        let OperatorResult::NeedMoreInput(Some(out)) = run_filter(pred, &batch) else {
            panic!("expected NeedMoreInput(Some)");
        };
        let ids: Vec<Option<i64>> = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(ids, vec![Some(2), Some(4)]);
    }

    #[test]
    fn all_rows_dropped_returns_need_more_input_none() {
        let batch = sample_batch();
        let pred = Expr::Literal(ScalarValue::Boolean(false));
        assert!(matches!(
            run_filter(pred, &batch),
            OperatorResult::NeedMoreInput(None)
        ));
    }

    #[test]
    fn scalar_true_keeps_all_rows_including_single_row() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![42]))]).unwrap();
        let pred = Expr::Literal(ScalarValue::Boolean(true));
        let OperatorResult::NeedMoreInput(Some(out)) = run_filter(pred, &batch) else {
            panic!("expected NeedMoreInput(Some)");
        };
        assert_eq!(out.num_rows(), 1);
    }
}
