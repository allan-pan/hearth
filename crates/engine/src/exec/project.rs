use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::EngineResult;
use crate::exec::OperatorResult;
use crate::exec::eval::evaluate;
use crate::expr::Expr;

pub struct ProjectOp {
    pub exprs: Vec<Expr>,
    pub schema: SchemaRef,
}

impl ProjectOp {
    pub fn execute(&self, input: &RecordBatch) -> EngineResult<OperatorResult> {
        let columns = self
            .exprs
            .iter()
            .map(|expr| evaluate(expr, input)?.into_array(input.num_rows()))
            .collect::<EngineResult<_>>()?;
        let projected = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(OperatorResult::NeedMoreInput(Some(projected)))
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
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                Arc::new(StringViewArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap()
    }

    fn run_project(exprs: Vec<Expr>, schema: SchemaRef, batch: &RecordBatch) -> RecordBatch {
        let OperatorResult::NeedMoreInput(Some(out)) =
            ProjectOp { exprs, schema }.execute(batch).unwrap()
        else {
            panic!("expected NeedMoreInput(Some)");
        };
        out
    }

    #[test]
    fn reorders_columns_and_preserves_aliases() {
        let batch = sample_batch();
        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8View, true),
            Field::new("key", DataType::Int64, true),
        ]));
        let exprs = vec![
            Expr::Column {
                index: 1,
                name: "name".into(),
            },
            Expr::Column {
                index: 0,
                name: "id".into(),
            },
        ];

        let out = run_project(exprs, schema.clone(), &batch);
        assert_eq!(out.schema(), schema);
        let labels: Vec<Option<&str>> = out
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect();
        let keys: Vec<Option<i64>> = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(labels, vec![Some("a"), Some("b"), None]);
        assert_eq!(keys, vec![Some(1), None, Some(3)]);
    }

    #[test]
    fn evaluates_expressions_and_broadcasts_scalars() {
        let batch = sample_batch();
        let schema = Arc::new(Schema::new(vec![
            Field::new("shifted", DataType::Int64, true),
            Field::new("constant", DataType::Int64, false),
        ]));
        let exprs = vec![
            Expr::Binary {
                left: Box::new(Expr::Column {
                    index: 0,
                    name: "id".into(),
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(ScalarValue::Int64(10))),
            },
            Expr::Literal(ScalarValue::Int64(99)),
        ];

        let out = run_project(exprs, schema.clone(), &batch);
        assert_eq!(out.schema(), schema);
        let shifted: Vec<Option<i64>> = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect();
        let constant: Vec<i64> = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(shifted, vec![Some(11), None, Some(13)]);
        assert_eq!(constant, vec![99, 99, 99]);
    }
}
