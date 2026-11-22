use arrow::datatypes::{DataType, Schema};
use sqlparser::ast::{self, Ident};

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
            let expr = bind_expr(*expr, input)?;
            let items: Vec<Expr> = items
                .into_iter()
                .map(|item| bind_expr(item, input))
                .collect::<EngineResult<Vec<_>>>()?;
            for item in &items {
                require_same_type(&expr, item, input, "IN")?;
            }
            Ok(Expr::InList {
                expr: Box::new(expr),
                list: items,
                negated,
            })
        }
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => bind_between(*expr, negated, *low, *high, input),
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
            let n = s.parse::<i64>().map_err(|_| {
                not_supported(&format!("the literal {s} (only integers are supported)"))
            })?;
            Ok(Expr::Literal(ScalarValue::Int64(n)))
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(ScalarValue::Utf8(s))),
        ast::Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(b))),
        other => Err(not_supported(&format!("the literal {other}"))),
    }
}

fn bind_binary(
    left: ast::Expr,
    op: ast::BinaryOperator,
    right: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
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
        other => return Err(not_supported(&format!("the binary operator {other}"))),
    };
    match op {
        BinaryOp::And | BinaryOp::Or => {
            require_boolean(&left, input, &op.to_string())?;
            require_boolean(&right, input, &op.to_string())?;
        }
        _ => {
            require_same_type(&left, &right, input, &op.to_string())?;
        }
    }
    Ok(Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn require_same_type(left: &Expr, right: &Expr, input: &Schema, context: &str) -> EngineResult<()> {
    let l = left.data_type(input);
    let r = right.data_type(input);
    if l != r {
        Err(not_supported(&format!(
            "the {context} comparison between two different types unsupported, got {left} of type {l} and {right} of type {r}"
        )))
    } else {
        Ok(())
    }
}

fn bind_between(
    expr: ast::Expr,
    negated: bool,
    low: ast::Expr,
    high: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
    let expr = bind_expr(expr, input)?;
    let low = bind_expr(low, input)?;
    let high = bind_expr(high, input)?;
    require_same_type(&expr, &low, input, "BETWEEN")?;
    require_same_type(&expr, &high, input, "BETWEEN")?;
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
