//! Type coercion: converting the two sides of a comparison to one type.
use arrow::datatypes::{DataType, Schema};

use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};

pub(super) fn common_type(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }
    match (left, right) {
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Some(DataType::Int64)
        }
        _ => {
            let (p1, s1) = as_decimal(left)?;
            let (p2, s2) = as_decimal(right)?;
            let scale = s1.max(s2);
            let integer_digits = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2));
            let precision = (integer_digits + i16::from(scale)).min(38);
            Some(DataType::Decimal128(u8::try_from(precision).ok()?, scale))
        }
    }
}

fn as_decimal(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Int32 => Some((10, 0)),
        DataType::Int64 => Some((19, 0)),
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

pub(super) fn coerce_pair(left: Expr, right: Expr, input: &Schema) -> EngineResult<(Expr, Expr)> {
    let (l, r) = (left.data_type(input), right.data_type(input));
    if l == r {
        return Ok((left, right));
    }
    if let Some(right) = literal_as(&right, &l) {
        return Ok((left, right));
    }
    if let Some(left) = literal_as(&left, &r) {
        return Ok((left, right));
    }
    let common = common_type(&l, &r).ok_or_else(|| cannot_compare(&left, &l, &right, &r))?;
    Ok((convert(left, &l, &common), convert(right, &r, &common)))
}

pub(super) fn coerce_list(
    expr: Expr,
    items: Vec<Expr>,
    input: &Schema,
) -> EngineResult<(Expr, Vec<Expr>)> {
    let target = expr.data_type(input);
    let fitted: Option<Vec<Expr>> = items
        .iter()
        .map(|item| {
            if item.data_type(input) == target {
                Some(item.clone())
            } else {
                literal_as(item, &target)
            }
        })
        .collect();
    if let Some(items) = fitted {
        return Ok((expr, items));
    }

    let mut common = target.clone();
    for item in &items {
        let item_type = item.data_type(input);
        common = common_type(&common, &item_type)
            .ok_or_else(|| cannot_compare(&expr, &common, item, &item_type))?;
    }
    let items = items
        .into_iter()
        .map(|item| {
            let from = item.data_type(input);
            convert(item, &from, &common)
        })
        .collect();
    Ok((convert(expr, &target, &common), items))
}

fn literal_as(expr: &Expr, to: &DataType) -> Option<Expr> {
    match expr {
        Expr::Literal(value) => value.cast_to(to).map(Expr::Literal),
        _ => None,
    }
}

fn convert(expr: Expr, from: &DataType, to: &DataType) -> Expr {
    if from == to {
        return expr;
    }
    literal_as(&expr, to).unwrap_or_else(|| Expr::Cast {
        expr: Box::new(expr),
        to: to.clone(),
    })
}

fn cannot_compare(left: &Expr, l: &DataType, right: &Expr, r: &DataType) -> EngineError {
    EngineError::Bind(format!(
        "cannot compare {left} of type {l} with {right} of type {r}"
    ))
}

/// Prepares the operands of `+`, `-` or `*` for Arrow's kernels, which need two integers of
/// one type or two decimals. An integer next to a decimal becomes a decimal, as narrow as
/// possible when it is a literal, since its precision feeds the result's.
pub(super) fn coerce_arithmetic(
    op: BinaryOp,
    left: Expr,
    right: Expr,
    input: &Schema,
) -> EngineResult<(Expr, Expr)> {
    let (l, r) = (left.data_type(input), right.data_type(input));
    if !is_numeric(&l) || !is_numeric(&r) {
        return Err(EngineError::Bind(format!(
            "{op} needs numbers, got {left} of type {l} and {right} of type {r}"
        )));
    }
    let is_decimal = |t: &DataType| matches!(t, DataType::Decimal128(..));
    match (is_decimal(&l), is_decimal(&r)) {
        (false, false) => coerce_pair(left, right, input),
        (true, true) => Ok((left, right)),
        (false, true) => Ok((decimal_operand(left, &l), right)),
        (true, false) => Ok((left, decimal_operand(right, &r))),
    }
}

fn is_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::Decimal128(..)
    )
}

fn decimal_operand(expr: Expr, integer_type: &DataType) -> Expr {
    if let Expr::Literal(value) = &expr
        && let Some(value) = narrowest_decimal(value)
    {
        return Expr::Literal(value);
    }
    let (precision, scale) = as_decimal(integer_type).expect("an integer type");
    Expr::Cast {
        expr: Box::new(expr),
        to: DataType::Decimal128(precision, scale),
    }
}

fn narrowest_decimal(value: &ScalarValue) -> Option<ScalarValue> {
    let n = match value {
        ScalarValue::Int32(n) => i128::from(*n),
        ScalarValue::Int64(n) => i128::from(*n),
        _ => return None,
    };
    let digits = n.unsigned_abs().checked_ilog10().map_or(1, |d| d + 1);
    value.cast_to(&DataType::Decimal128(u8::try_from(digits).ok()?, 0))
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
}
