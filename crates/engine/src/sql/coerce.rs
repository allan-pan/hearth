//! Type coercion: converting the two sides of a comparison to one type.
use arrow::datatypes::{DataType, Schema};

use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};
use crate::types::{as_decimal, common_type, is_numeric};

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
