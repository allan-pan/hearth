//! Type coercion: converting the operands of a comparison or arithmetic to types that fit.
use arrow::datatypes::{DataType, Schema};

use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};
use crate::types::{as_decimal, common_type, is_numeric};

pub(super) fn coerce_pair(left: Expr, right: Expr, input: &Schema) -> EngineResult<(Expr, Expr)> {
    let [left, right] = <[Expr; 2]>::try_from(coerce_operands(vec![left, right], input)?)
        .expect("coerce_operands keeps the expression count");
    Ok((left, right))
}

pub(super) fn coerce_operands(exprs: Vec<Expr>, input: &Schema) -> EngineResult<Vec<Expr>> {
    let types: Vec<DataType> = exprs.iter().map(|expr| expr.data_type(input)).collect();
    for target in &types {
        let fitted: Option<Vec<Expr>> = exprs
            .iter()
            .zip(&types)
            .map(|(expr, from)| {
                if from == target {
                    Some(expr.clone())
                } else {
                    literal_as(expr, target)
                }
            })
            .collect();
        if let Some(fitted) = fitted {
            return Ok(fitted);
        }
    }

    let mut common = types[0].clone();
    for (expr, from) in exprs.iter().zip(&types).skip(1) {
        common = common_type(&common, from)
            .ok_or_else(|| cannot_compare(&exprs[0], &common, expr, from))?;
    }
    Ok(exprs
        .into_iter()
        .zip(&types)
        .map(|(expr, from)| convert(expr, from, &common))
        .collect())
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
