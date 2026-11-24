use std::sync::Arc;

use crate::error::EngineResult;
use crate::expr::ScalarValue;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
    Scalar, StringViewArray,
};

pub fn to_arrow_scalar(value: &ScalarValue) -> EngineResult<Scalar<ArrayRef>> {
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
    Ok(Scalar::new(array))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::ScalarValue;
    use arrow::array::Datum;

    #[test]
    fn test_to_arrow_scalar() {
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
            ScalarValue::Date32(18628), // Represents 2021-01-01
            ScalarValue::Utf8("Hello, world!".to_string()),
        ] {
            let arrow_scalar = to_arrow_scalar(&variant).unwrap();
            assert!(arrow_scalar.get().1);
            assert_eq!(arrow_scalar.get().0.len(), 1);
            assert_eq!(arrow_scalar.get().0.data_type(), &variant.data_type());
        }
    }
}
