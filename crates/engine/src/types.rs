//! Type rules: which types an operation accepts and which type it produces.
use arrow::datatypes::DataType;

use crate::expr::{AggFunc, BinaryOp};

const MAX_DECIMAL_PRECISION: i16 = 38;

pub fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::Decimal128(..)
    )
}

pub fn as_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Int32 => Some((10, 0)),
        DataType::Int64 => Some((19, 0)),
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

pub fn common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    match (left, right) {
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Some(DataType::Int64)
        }
        _ => decimal_union(as_decimal(left)?, as_decimal(right)?, 0),
    }
}

pub fn arithmetic_type(op: BinaryOp, left: &DataType, right: &DataType) -> Option<DataType> {
    match (left, right) {
        (DataType::Int32, DataType::Int32) => Some(DataType::Int32),
        (DataType::Int64, DataType::Int64) => Some(DataType::Int64),
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => match op {
            BinaryOp::Multiply => decimal(
                i16::from(*p1) + i16::from(*p2) + 1,
                i16::from(*s1) + i16::from(*s2),
            ),
            // One more integer digit for the carry.
            _ => decimal_union((*p1, *s1), (*p2, *s2), 1),
        },
        _ => None,
    }
}

pub fn aggregate_type(func: AggFunc, input: &DataType) -> Option<DataType> {
    match func {
        AggFunc::Count => Some(DataType::Int64),
        AggFunc::Min | AggFunc::Max => Some(input.clone()),
        AggFunc::Avg => is_numeric(input).then_some(DataType::Float64),
        AggFunc::Sum => match input {
            DataType::Int32 | DataType::Int64 => Some(DataType::Int64),
            DataType::Decimal128(_, scale) => decimal(MAX_DECIMAL_PRECISION, i16::from(*scale)),
            _ => None,
        },
    }
}

fn decimal_union(left: (u8, i8), right: (u8, i8), extra_digits: i16) -> Option<DataType> {
    let integer_digits = |(precision, scale): (u8, i8)| i16::from(precision) - i16::from(scale);
    let scale = left.1.max(right.1);
    let integer = integer_digits(left).max(integer_digits(right));
    decimal(integer + extra_digits + i16::from(scale), i16::from(scale))
}

fn decimal(precision: i16, scale: i16) -> Option<DataType> {
    let precision = u8::try_from(precision.min(MAX_DECIMAL_PRECISION)).ok()?;
    Some(DataType::Decimal128(precision, i8::try_from(scale).ok()?))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType::{
        self, Boolean, Date32, Decimal128, Float64, Int32, Int64, Utf8View,
    };

    use super::*;

    fn check(left: DataType, right: DataType, expected: Option<DataType>) {
        assert_eq!(common_type(&left, &right), expected, "{left} with {right}");
        assert_eq!(common_type(&right, &left), expected, "{right} with {left}");
    }

    #[test]
    fn same_type_is_its_own_common_type() {
        for t in [Int32, Int64, Boolean, Utf8View, Date32, Decimal128(15, 2)] {
            check(t.clone(), t.clone(), Some(t));
        }
    }

    #[test]
    fn integers_widen_to_int64() {
        check(Int32, Int64, Some(Int64));
    }

    #[test]
    fn integer_and_decimal_meet_at_a_decimal_holding_both() {
        check(Int32, Decimal128(15, 2), Some(Decimal128(15, 2)));
        check(Int32, Decimal128(5, 2), Some(Decimal128(12, 2)));
        check(Int64, Decimal128(15, 2), Some(Decimal128(21, 2)));
    }

    #[test]
    fn decimals_keep_the_larger_scale_and_the_larger_integer_part() {
        check(Decimal128(15, 2), Decimal128(6, 4), Some(Decimal128(17, 4)));
        check(
            Decimal128(15, 2),
            Decimal128(10, 2),
            Some(Decimal128(15, 2)),
        );
    }

    #[test]
    fn decimal_precision_is_capped_at_38() {
        check(
            Decimal128(38, 0),
            Decimal128(10, 5),
            Some(Decimal128(38, 5)),
        );
    }

    #[test]
    fn unrelated_types_have_no_common_type() {
        let cases = [
            (Utf8View, Int64),
            (Boolean, Int32),
            (Date32, Int32),
            (Date32, Utf8View),
            (Float64, Int64),
            (Boolean, Decimal128(15, 2)),
        ];
        for (left, right) in cases {
            check(left, right, None);
        }
    }

    #[test]
    fn arithmetic_refuses_operands_the_kernels_do_not_take() {
        for (left, right) in [
            (Int32, Int64),
            (Int64, Decimal128(15, 2)),
            (Utf8View, Utf8View),
        ] {
            assert_eq!(arithmetic_type(BinaryOp::Add, &left, &right), None);
        }
    }

    #[test]
    fn aggregates_accept_and_return() {
        let cases = [
            (AggFunc::Sum, Int32, Some(Int64)),
            (AggFunc::Sum, Decimal128(15, 2), Some(Decimal128(38, 2))),
            (AggFunc::Sum, Utf8View, None),
            (AggFunc::Avg, Decimal128(15, 2), Some(Float64)),
            (AggFunc::Avg, Boolean, None),
            (AggFunc::Min, Utf8View, Some(Utf8View)),
            (AggFunc::Count, Boolean, Some(Int64)),
        ];
        for (func, input, expected) in cases {
            assert_eq!(
                aggregate_type(func, &input),
                expected,
                "{func} over {input}"
            );
        }
    }
}
