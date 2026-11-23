use arrow::datatypes::{DataType, Schema};
use sqlparser::ast::{self, Ident};

use super::aggregate::{bind_aggregate, is_aggregate};
use super::coerce::{coerce_arithmetic, coerce_operands, coerce_pair};
use super::dates::fold_interval;
use super::{normalize, not_supported};
use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};

pub(super) fn bind_expr(expr: ast::Expr, input: &Schema) -> EngineResult<Expr> {
    match expr {
        ast::Expr::Identifier(ident) => bind_column(&ident, input),
        ast::Expr::Value(value) => bind_literal(value.into()),
        ast::Expr::BinaryOp { left, op, right } => bind_binary(*left, op, *right, input),
        ast::Expr::Nested(inner) => bind_expr(*inner, input),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr,
        } => {
            let expr = bind_expr(*expr, input)?;
            require_boolean(&expr, input, "NOT")?;
            Ok(Expr::Not(Box::new(expr)))
        }
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Minus,
            expr,
        } => {
            let expr = bind_expr(*expr, input)?;
            let expr = match expr {
                Expr::Literal(ScalarValue::Int64(n)) => {
                    Expr::Literal(ScalarValue::Int64(n.checked_neg().ok_or_else(|| {
                        EngineError::Bind(format!("integer literal {n} is too small to negate"))
                    })?))
                }
                Expr::Literal(ScalarValue::Decimal128 {
                    value,
                    precision,
                    scale,
                }) => Expr::Literal(ScalarValue::Decimal128 {
                    value: value.checked_neg().ok_or_else(|| {
                        EngineError::Bind(format!("decimal literal {value} is too small to negate"))
                    })?,
                    precision,
                    scale,
                }),
                Expr::Literal(other) => {
                    return Err(EngineError::Bind(format!(
                        "unary minus is only supported for numeric literals, got {other}"
                    )));
                }
                other => {
                    return Err(not_supported(&format!(
                        "unary minus not supported yet for {other}"
                    )));
                }
            };
            Ok(expr)
        }
        ast::Expr::IsNull(e) => {
            let e = bind_expr(*e, input)?;
            Ok(Expr::IsNull {
                expr: Box::new(e),
                negated: false,
            })
        }
        ast::Expr::IsNotNull(e) => {
            let e = bind_expr(*e, input)?;
            Ok(Expr::IsNull {
                expr: Box::new(e),
                negated: true,
            })
        }
        ast::Expr::In {
            expr,
            target: ast::InTarget::List(items),
            negated,
            global: false,
        } => {
            let mut operands = vec![bind_expr(*expr, input)?];
            for item in items {
                operands.push(bind_expr(item, input)?);
            }
            let mut operands = coerce_operands(operands, input)?;
            let expr = operands.remove(0);
            Ok(Expr::InList {
                expr: Box::new(expr),
                list: operands,
                negated,
            })
        }
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => bind_between(*expr, negated, *low, *high, input),
        ast::Expr::TypedString(ast::TypedString {
            data_type,
            value,
            uses_odbc_syntax: _,
        }) => match (data_type, value.value) {
            (ast::DataType::Date, ast::Value::SingleQuotedString(text)) => {
                let value = ScalarValue::parse_date(&text)
                    .ok_or_else(|| EngineError::Bind(format!("invalid date literal '{text}'")))?;
                Ok(Expr::Literal(value))
            }
            (other, _) => Err(not_supported(&format!("the typed string literal {other}"))),
        },
        ast::Expr::Function(function) if is_aggregate(&function) => {
            let aggregate = bind_aggregate(function, input)?;
            Ok(Expr::Aggregate(Box::new(aggregate)))
        }
        other => Err(not_supported(&format!("the expression {other}"))),
    }
}

fn bind_column(ident: &Ident, input: &Schema) -> EngineResult<Expr> {
    let name = normalize(ident);
    let index = input
        .fields()
        .iter()
        .position(|f| *f.name() == name)
        .ok_or_else(|| EngineError::Bind(format!("column {name} does not exist")))?;
    Ok(Expr::Column {
        index,
        name: input.field(index).name().clone(),
    })
}

fn bind_literal(value: ast::Value) -> EngineResult<Expr> {
    match value {
        ast::Value::Number(s, _) => {
            let value = match s.parse::<i64>() {
                Ok(n) => ScalarValue::Int64(n),
                Err(_) => parse_decimal(&s)
                    .ok_or_else(|| not_supported(&format!("the numeric literal {s}")))?,
            };
            Ok(Expr::Literal(value))
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(ScalarValue::Utf8(s))),
        ast::Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(b))),
        other => Err(not_supported(&format!("the literal {other}"))),
    }
}

fn parse_decimal(text: &str) -> Option<ScalarValue> {
    let (integer, fraction) = text.split_once('.')?;
    let is_digits = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if !is_digits(integer) || !is_digits(fraction) || integer.len() + fraction.len() == 0 {
        return None;
    }
    let integer_digits = integer.trim_start_matches('0').len().max(1);
    let precision = u8::try_from(integer_digits + fraction.len()).ok()?;
    if precision > 38 {
        return None;
    }
    Some(ScalarValue::Decimal128 {
        value: format!("{integer}{fraction}").parse().ok()?,
        precision,
        scale: i8::try_from(fraction.len()).ok()?,
    })
}

fn bind_binary(
    left: ast::Expr,
    op: ast::BinaryOperator,
    right: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
    use ast::BinaryOperator::{Minus, Plus};
    let (left, op, right) = match (left, op, right) {
        (date, op @ (Plus | Minus), ast::Expr::Interval(interval)) => {
            return fold_interval(date, op == Minus, interval, input);
        }
        (ast::Expr::Interval(interval), Plus, date) => {
            return fold_interval(date, false, interval, input);
        }
        (ast::Expr::Interval(_), Minus, _) => {
            return Err(EngineError::Bind(
                "cannot subtract from an interval".to_string(),
            ));
        }
        other => other,
    };
    let left = bind_expr(left, input)?;
    let right = bind_expr(right, input)?;
    let op = match op {
        ast::BinaryOperator::Eq => BinaryOp::Eq,
        ast::BinaryOperator::NotEq => BinaryOp::NotEq,
        ast::BinaryOperator::Lt => BinaryOp::Lt,
        ast::BinaryOperator::LtEq => BinaryOp::LtEq,
        ast::BinaryOperator::Gt => BinaryOp::Gt,
        ast::BinaryOperator::GtEq => BinaryOp::GtEq,
        ast::BinaryOperator::And => BinaryOp::And,
        ast::BinaryOperator::Or => BinaryOp::Or,
        ast::BinaryOperator::Plus => BinaryOp::Add,
        ast::BinaryOperator::Minus => BinaryOp::Subtract,
        ast::BinaryOperator::Multiply => BinaryOp::Multiply,
        other => return Err(not_supported(&format!("the binary operator {other}"))),
    };
    let (left, right) = match op {
        BinaryOp::And | BinaryOp::Or => {
            require_boolean(&left, input, &op.to_string())?;
            require_boolean(&right, input, &op.to_string())?;
            (left, right)
        }
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply => {
            coerce_arithmetic(op, left, right, input)?
        }
        _ => coerce_pair(left, right, input)?,
    };
    Ok(Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn bind_between(
    expr: ast::Expr,
    negated: bool,
    low: ast::Expr,
    high: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
    let operands = vec![
        bind_expr(expr, input)?,
        bind_expr(low, input)?,
        bind_expr(high, input)?,
    ];
    let [expr, low, high] = <[Expr; 3]>::try_from(coerce_operands(operands, input)?)
        .expect("coerce_operands keeps the expression count");
    let (low_op, join_op, high_op) = if negated {
        (BinaryOp::Lt, BinaryOp::Or, BinaryOp::Gt)
    } else {
        (BinaryOp::GtEq, BinaryOp::And, BinaryOp::LtEq)
    };

    Ok(Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(expr.clone()),
            op: low_op,
            right: Box::new(low),
        }),
        op: join_op,
        right: Box::new(Expr::Binary {
            left: Box::new(expr),
            op: high_op,
            right: Box::new(high),
        }),
    })
}

pub(super) fn require_boolean(expr: &Expr, schema: &Schema, clause: &str) -> EngineResult<()> {
    let datatype = expr.data_type(schema);
    if datatype != DataType::Boolean {
        Err(EngineError::Bind(format!(
            "{clause} needs boolean expression, got {expr} of type {datatype}",
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(value: i128, precision: u8, scale: i8) -> Option<ScalarValue> {
        Some(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        })
    }

    #[test]
    fn parses_decimal_literals() {
        let cases = [
            ("0.05", decimal(5, 3, 2)),
            ("12.34", decimal(1234, 4, 2)),
            ("1.50", decimal(150, 3, 2)),
            (".5", decimal(5, 2, 1)),
            ("5.", decimal(5, 1, 0)),
            ("007.5", decimal(75, 2, 1)),
            ("0.0", decimal(0, 2, 1)),
        ];
        for (text, expected) in cases {
            assert_eq!(parse_decimal(text), expected, "{text}");
        }
    }

    #[test]
    fn decimal_literal_precision_is_limited_to_38_digits() {
        let fraction = "1".repeat(37);
        assert_eq!(
            parse_decimal(&format!("1.{fraction}")).map(|v| v.data_type()),
            Some(DataType::Decimal128(38, 37))
        );
        assert_eq!(parse_decimal(&format!("12.{fraction}")), None);
    }

    #[test]
    fn rejects_text_that_is_not_a_plain_decimal() {
        for text in ["1", "1e3", "1.5e2", "1.2.3", ".", "-1.5", "1.a", ""] {
            assert_eq!(parse_decimal(text), None, "{text:?}");
        }
    }
}
