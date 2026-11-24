use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, AsArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float64Array,
        Int32Array, Int64Array, StringViewArray, UInt32Array,
    },
    compute::{
        CastOptions,
        kernels::{boolean, cast, cmp, numeric},
    },
    datatypes::DataType,
    error::ArrowError,
    record_batch::RecordBatch,
};

use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};

pub enum ColumnarValue {
    Array(ArrayRef),
    Scalar(ArrayRef),
}

impl Datum for ColumnarValue {
    fn get(&self) -> (&dyn Array, bool) {
        match self {
            Self::Array(a) => (a.as_ref(), false),
            Self::Scalar(a) => (a.as_ref(), true),
        }
    }
}

impl ColumnarValue {
    pub fn from_scalar(value: &ScalarValue) -> EngineResult<Self> {
        let array: ArrayRef = match value {
            ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v])),
            ScalarValue::Int32(v) => Arc::new(Int32Array::from(vec![*v])),
            ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v])),
            ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![*v])),
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => Arc::new(
                Decimal128Array::from(vec![*value]).with_precision_and_scale(*precision, *scale)?,
            ),
            ScalarValue::Date32(v) => Arc::new(Date32Array::from(vec![*v])),
            ScalarValue::Utf8(v) => Arc::new(StringViewArray::from(vec![v.as_str()])),
        };
        Ok(Self::from_array(array, true))
    }

    pub fn data_type(&self) -> &DataType {
        self.get().0.data_type()
    }

    pub fn into_array(self, num_rows: usize) -> EngineResult<ArrayRef> {
        match self {
            Self::Array(a) => Ok(a),
            Self::Scalar(a) => {
                let indices = UInt32Array::from(vec![0; num_rows]);
                Ok(arrow::compute::take(a.as_ref(), &indices, None)?)
            }
        }
    }

    fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }

    fn from_array(array: ArrayRef, is_scalar: bool) -> Self {
        if is_scalar {
            debug_assert_eq!(array.len(), 1);
            Self::Scalar(array)
        } else {
            Self::Array(array)
        }
    }

    fn into_inner(self) -> ArrayRef {
        match self {
            Self::Array(a) | Self::Scalar(a) => a,
        }
    }

    fn map_array(
        self,
        f: impl FnOnce(&dyn Array) -> Result<ArrayRef, ArrowError>,
    ) -> EngineResult<Self> {
        let is_scalar = self.is_scalar();
        let array = f(self.get().0)?;
        Ok(Self::from_array(array, is_scalar))
    }
}

pub fn evaluate(expr: &Expr, batch: &RecordBatch) -> EngineResult<ColumnarValue> {
    let value = match expr {
        Expr::Column { index, .. } => ColumnarValue::Array(batch.column(*index).clone()),
        Expr::Literal(v) => ColumnarValue::from_scalar(v)?,
        Expr::Binary { left, op, right } => {
            let left = evaluate(left, batch)?;
            let right = evaluate(right, batch)?;
            eval_binary(left, *op, right, batch.num_rows())?
        }
        Expr::Not(inner) => eval_not(evaluate(inner, batch)?)?,
        Expr::IsNull { expr, negated } => evaluate(expr, batch)?.map_array(|a| {
            Ok(Arc::new(if *negated {
                boolean::is_not_null(a)?
            } else {
                boolean::is_null(a)?
            }))
        })?,
        Expr::Cast { expr, to } => evaluate(expr, batch)?.map_array(|a| {
            cast::cast_with_options(
                a,
                to,
                &CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
        })?,
        Expr::InList {
            expr,
            list,
            negated,
        } => match list.split_first() {
            None => ColumnarValue::from_scalar(&ScalarValue::Boolean(*negated))?,
            Some((first, rest)) => {
                let lhs = evaluate(expr, batch)?;
                let mut acc = eval_eq(&lhs, &evaluate(first, batch)?)?;
                for item in rest {
                    let eq = eval_eq(&lhs, &evaluate(item, batch)?)?;
                    acc = eval_bool_binary(acc, eq, batch.num_rows(), boolean::or_kleene)?;
                }
                if *negated { eval_not(acc)? } else { acc }
            }
        },
        Expr::Aggregate(agg) => {
            return Err(EngineError::Execution(format!(
                "unexpected aggregate expression {agg} during scalar evaluation"
            )));
        }
    };
    debug_assert_eq!(value.data_type(), &expr.data_type(batch.schema_ref()));
    Ok(value)
}

fn eval_binary(
    left: ColumnarValue,
    op: BinaryOp,
    right: ColumnarValue,
    num_rows: usize,
) -> EngineResult<ColumnarValue> {
    let array: ArrayRef = match op {
        BinaryOp::Add => numeric::add(&left, &right)?,
        BinaryOp::Subtract => numeric::sub(&left, &right)?,
        BinaryOp::Multiply => numeric::mul(&left, &right)?,
        BinaryOp::Eq => Arc::new(cmp::eq(&left, &right)?),
        BinaryOp::NotEq => Arc::new(cmp::neq(&left, &right)?),
        BinaryOp::Lt => Arc::new(cmp::lt(&left, &right)?),
        BinaryOp::LtEq => Arc::new(cmp::lt_eq(&left, &right)?),
        BinaryOp::Gt => Arc::new(cmp::gt(&left, &right)?),
        BinaryOp::GtEq => Arc::new(cmp::gt_eq(&left, &right)?),
        BinaryOp::And => return eval_bool_binary(left, right, num_rows, boolean::and_kleene),
        BinaryOp::Or => return eval_bool_binary(left, right, num_rows, boolean::or_kleene),
    };
    Ok(ColumnarValue::from_array(
        array,
        left.is_scalar() && right.is_scalar(),
    ))
}

fn eval_eq(left: &ColumnarValue, right: &ColumnarValue) -> EngineResult<ColumnarValue> {
    Ok(ColumnarValue::from_array(
        Arc::new(cmp::eq(left, right)?),
        left.is_scalar() && right.is_scalar(),
    ))
}

fn eval_not(value: ColumnarValue) -> EngineResult<ColumnarValue> {
    value.map_array(|a| Ok(Arc::new(boolean::not(a.as_boolean())?)))
}

fn eval_bool_binary(
    left: ColumnarValue,
    right: ColumnarValue,
    num_rows: usize,
    f: impl FnOnce(&BooleanArray, &BooleanArray) -> Result<BooleanArray, ArrowError>,
) -> EngineResult<ColumnarValue> {
    let is_scalar = left.is_scalar() && right.is_scalar();
    let (l, r) = if is_scalar {
        (left.into_inner(), right.into_inner())
    } else {
        (left.into_array(num_rows)?, right.into_array(num_rows)?)
    };
    Ok(ColumnarValue::from_array(
        Arc::new(f(l.as_boolean(), r.as_boolean())?),
        is_scalar,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        AsArray, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
        StringViewArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::expr::{AggExpr, BinaryOp, Expr, ScalarValue};

    fn col(index: usize, name: &str) -> Expr {
        Expr::Column {
            index,
            name: name.into(),
        }
    }

    fn lit(v: ScalarValue) -> Expr {
        Expr::Literal(v)
    }

    fn bin(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn eval_bools(expr: &Expr, batch: &RecordBatch) -> Vec<Option<bool>> {
        let arr = evaluate(expr, batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        arr.as_boolean().iter().collect()
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("b1", DataType::Boolean, true),
            Field::new("b2", DataType::Boolean, true),
            Field::new("s", DataType::Utf8View, true),
            Field::new("d", DataType::Date32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30), Some(40)])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(false),
                    None,
                    Some(true),
                    None,
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("123"),
                ])),
                Arc::new(Date32Array::from(vec![
                    Some(100),
                    Some(200),
                    None,
                    Some(300),
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn column_and_literal_evaluation() {
        let batch = sample_batch();
        let c = evaluate(&col(0, "i32"), &batch).unwrap();
        assert!(matches!(c, ColumnarValue::Array(_)));
        assert_eq!(
            c.into_array(4)
                .unwrap()
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), None, Some(4)]
        );

        let l = evaluate(&lit(ScalarValue::Int64(7)), &batch).unwrap();
        assert!(matches!(l, ColumnarValue::Scalar(_)));
        assert_eq!(
            l.into_array(4)
                .unwrap()
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(7), Some(7), Some(7), Some(7)]
        );
    }

    #[test]
    fn scalar_op_scalar_stays_scalar() {
        let batch = sample_batch();
        let expr = bin(
            lit(ScalarValue::Decimal128 {
                value: 6,
                precision: 3,
                scale: 2,
            }),
            BinaryOp::Subtract,
            lit(ScalarValue::Decimal128 {
                value: 1,
                precision: 3,
                scale: 2,
            }),
        );
        let val = evaluate(&expr, &batch).unwrap();
        assert!(matches!(val, ColumnarValue::Scalar(_)));
        assert_eq!(val.data_type(), &DataType::Decimal128(4, 2));
        let arr = val.into_array(1).unwrap();
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            5
        );
    }

    #[test]
    fn comparisons_with_nulls_and_scalars() {
        let batch = sample_batch();
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::Eq, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(false), Some(true), None, Some(false)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::NotEq, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(true), Some(false), None, Some(true)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::Lt, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(true), Some(false), None, Some(false)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::LtEq, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(true), Some(true), None, Some(false)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::Gt, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(false), Some(false), None, Some(true)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(0, "i32"), BinaryOp::GtEq, lit(ScalarValue::Int32(2))),
                &batch
            ),
            vec![Some(false), Some(true), None, Some(true)]
        );
        assert_eq!(
            eval_bools(
                &bin(col(5, "d"), BinaryOp::Gt, lit(ScalarValue::Date32(150))),
                &batch
            ),
            vec![Some(false), Some(true), None, Some(true)]
        );
    }

    #[test]
    fn arithmetic_array_and_scalar_with_nulls() {
        let batch = sample_batch();
        let add_expr = bin(col(0, "i32"), BinaryOp::Add, lit(ScalarValue::Int32(10)));
        let res = evaluate(&add_expr, &batch).unwrap();
        assert!(matches!(res, ColumnarValue::Array(_)));
        assert_eq!(
            res.into_array(4)
                .unwrap()
                .as_primitive::<Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(11), Some(12), None, Some(14)]
        );

        let mul_expr = bin(
            col(1, "i64"),
            BinaryOp::Multiply,
            lit(ScalarValue::Int64(2)),
        );
        assert_eq!(
            evaluate(&mul_expr, &batch)
                .unwrap()
                .into_array(4)
                .unwrap()
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(20), None, Some(60), Some(80)]
        );
    }

    #[test]
    fn decimal_arithmetic_with_different_scales() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Decimal128(15, 2), true),
            Field::new("b", DataType::Decimal128(6, 4), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal128Array::from(vec![Some(1250), None]) // 12.50
                        .with_precision_and_scale(15, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(2500), Some(1000)]) // 0.2500, 0.1000
                        .with_precision_and_scale(6, 4)
                        .unwrap(),
                ),
            ],
        )
        .unwrap();

        let sum = evaluate(&bin(col(0, "a"), BinaryOp::Add, col(1, "b")), &batch).unwrap();
        assert_eq!(sum.data_type(), &DataType::Decimal128(18, 4));
        let sum_arr = sum.into_array(2).unwrap();
        let dec = sum_arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.iter().collect::<Vec<_>>(), vec![Some(127500), None]);

        let prod = evaluate(&bin(col(0, "a"), BinaryOp::Multiply, col(1, "b")), &batch).unwrap();
        assert_eq!(prod.data_type(), &DataType::Decimal128(22, 6));
        let prod_arr = prod.into_array(2).unwrap();
        let dec = prod_arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(dec.iter().collect::<Vec<_>>(), vec![Some(3125000), None]);
    }

    #[test]
    fn arithmetic_overflow_maps_to_execution_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![i32::MAX]))]).unwrap();
        let expr = bin(col(0, "x"), BinaryOp::Add, lit(ScalarValue::Int32(1)));
        assert!(matches!(
            evaluate(&expr, &batch),
            Err(EngineError::Execution(_))
        ));
    }

    #[test]
    fn kleene_boolean_logic_and_not() {
        let batch = sample_batch();
        // b1: [T, F, N, T]
        // b2: [F, N, T, N]
        // AND: [F, F, N, N]
        assert_eq!(
            eval_bools(&bin(col(2, "b1"), BinaryOp::And, col(3, "b2")), &batch),
            vec![Some(false), Some(false), None, None]
        );
        // OR:  [T, N, T, T]
        assert_eq!(
            eval_bools(&bin(col(2, "b1"), BinaryOp::Or, col(3, "b2")), &batch),
            vec![Some(true), None, Some(true), Some(true)]
        );
        // Mixed scalar + array: false AND b1 -> [F, F, F, F]
        assert_eq!(
            eval_bools(
                &bin(
                    lit(ScalarValue::Boolean(false)),
                    BinaryOp::And,
                    col(2, "b1")
                ),
                &batch
            ),
            vec![Some(false), Some(false), Some(false), Some(false)]
        );
        // Scalar op Scalar stays Scalar
        let scalar_or = evaluate(
            &bin(
                lit(ScalarValue::Boolean(true)),
                BinaryOp::Or,
                lit(ScalarValue::Boolean(false)),
            ),
            &batch,
        )
        .unwrap();
        assert!(matches!(scalar_or, ColumnarValue::Scalar(_)));

        // NOT b1: [F, T, N, F]
        assert_eq!(
            eval_bools(&Expr::Not(Box::new(col(2, "b1"))), &batch),
            vec![Some(false), Some(true), None, Some(false)]
        );
    }

    #[test]
    fn is_null_and_is_not_null() {
        let batch = sample_batch();
        assert_eq!(
            eval_bools(
                &Expr::IsNull {
                    expr: Box::new(col(0, "i32")),
                    negated: false,
                },
                &batch
            ),
            vec![Some(false), Some(false), Some(true), Some(false)]
        );
        assert_eq!(
            eval_bools(
                &Expr::IsNull {
                    expr: Box::new(col(0, "i32")),
                    negated: true,
                },
                &batch
            ),
            vec![Some(true), Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn cast_succeeds_and_fails_on_invalid_input() {
        let batch = sample_batch();
        let widened = Expr::Cast {
            expr: Box::new(col(0, "i32")),
            to: DataType::Int64,
        };
        assert_eq!(
            evaluate(&widened, &batch)
                .unwrap()
                .into_array(4)
                .unwrap()
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), None, Some(4)]
        );

        let bad_cast = Expr::Cast {
            expr: Box::new(col(4, "s")), // contains "a", "b"
            to: DataType::Int64,
        };
        assert!(matches!(
            evaluate(&bad_cast, &batch),
            Err(EngineError::Execution(_))
        ));
    }

    #[test]
    fn in_list_and_not_in_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
                Arc::new(Int32Array::from(vec![None, None, Some(5)])),
            ],
        )
        .unwrap();

        // x IN (1, y):
        // row 0: 1 IN (1, NULL) -> true
        // row 1: 2 IN (1, NULL) -> NULL
        // row 2: NULL IN (1, 5) -> NULL
        let in_expr = Expr::InList {
            expr: Box::new(col(0, "x")),
            list: vec![lit(ScalarValue::Int32(1)), col(1, "y")],
            negated: false,
        };
        assert_eq!(eval_bools(&in_expr, &batch), vec![Some(true), None, None]);

        let not_in = Expr::InList {
            expr: Box::new(col(0, "x")),
            list: vec![lit(ScalarValue::Int32(1)), col(1, "y")],
            negated: true,
        };
        assert_eq!(eval_bools(&not_in, &batch), vec![Some(false), None, None]);

        // Empty list: x IN () -> false, x NOT IN () -> true
        let empty_in = Expr::InList {
            expr: Box::new(col(0, "x")),
            list: vec![],
            negated: false,
        };
        assert_eq!(
            eval_bools(&empty_in, &batch),
            vec![Some(false), Some(false), Some(false)]
        );
    }

    #[test]
    fn aggregate_expr_is_an_execution_error() {
        let batch = sample_batch();
        let err = evaluate(&Expr::Aggregate(Box::new(AggExpr::CountAll)), &batch);
        assert!(matches!(err, Err(EngineError::Execution(_))));
    }

    #[test]
    fn from_scalar_covers_all_variants() {
        for variant in [
            ScalarValue::Boolean(true),
            ScalarValue::Int32(42),
            ScalarValue::Int64(1234567890),
            ScalarValue::Float64(1.23),
            ScalarValue::Decimal128 {
                value: 12345678901234567890,
                precision: 20,
                scale: 10,
            },
            ScalarValue::Date32(18628),
            ScalarValue::Utf8("Hello, world!".to_string()),
        ] {
            let val = ColumnarValue::from_scalar(&variant).unwrap();
            let (array, is_scalar) = val.get();
            assert!(is_scalar);
            assert_eq!(array.len(), 1);
            assert_eq!(array.data_type(), &variant.data_type());
        }
    }
}
