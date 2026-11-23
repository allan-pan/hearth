use crate::error::{EngineError, EngineResult};
use crate::expr::Expr;
use crate::logical::{LogicalPlan, SortKey};
use crate::sql::{bind_expr, not_supported, reject_aggregates};
use arrow::datatypes::Schema;
use sqlparser::ast;

pub(super) fn bind_order_by(
    order_by: ast::OrderBy,
    plan: &LogicalPlan,
) -> EngineResult<Vec<SortKey>> {
    let ast::OrderBy { kind, interpolate } = order_by;
    if interpolate.is_some() {
        return Err(not_supported("ORDER BY INTERPOLATE"));
    }
    let items = match kind {
        ast::OrderByKind::Expressions(items) => items,
        ast::OrderByKind::All(_) => return Err(not_supported("ORDER BY ALL")),
    };
    let output = plan.schema();
    items
        .into_iter()
        .map(|item| sort_key(item, plan, &output))
        .collect()
}

fn sort_key(item: ast::OrderByExpr, plan: &LogicalPlan, output: &Schema) -> EngineResult<SortKey> {
    let ast::OrderByExpr {
        expr,
        options: ast::OrderByOptions { sort, nulls_first },
        with_fill,
    } = item;
    if with_fill.is_some() {
        return Err(not_supported("ORDER BY ... WITH FILL"));
    }
    if nulls_first.is_some() {
        return Err(not_supported("ORDER BY ... NULLS FIRST/LAST"));
    }
    let ascending = match sort {
        None | Some(ast::OrderBySort::Asc) => true,
        Some(ast::OrderBySort::Desc) => false,
        Some(ast::OrderBySort::Using(_)) => return Err(not_supported("ORDER BY ... USING")),
    };
    let expr = order_expr(expr, plan, output)?;
    Ok(SortKey {
        expr,
        ascending,
        nulls_first: !ascending,
    })
}

fn order_expr(expr: ast::Expr, plan: &LogicalPlan, output: &Schema) -> EngineResult<Expr> {
    match expr {
        ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(n, _),
            ..
        }) => match n.parse::<usize>() {
            Ok(pos) if (1..=output.fields().len()).contains(&pos) => Ok(Expr::Column {
                index: pos - 1,
                name: output.field(pos - 1).name().clone(),
            }),
            _ => Err(EngineError::Bind(format!(
                "ORDER BY position must be a positive integer within range, got {n}"
            ))),
        },
        _ => match bind_expr(expr.clone(), output) {
            Ok(bound_expr) => {
                reject_aggregates(&bound_expr, "ORDER BY")?;
                Ok(bound_expr)
            }
            Err(error) => {
                let not_selected = match plan {
                    LogicalPlan::Project { input, .. } => bind_expr(expr, &input.schema()).is_ok(),
                    _ => false,
                };
                if not_selected {
                    Err(not_supported("ORDER BY a column that is not selected"))
                } else {
                    Err(error)
                }
            }
        },
    }
}

pub(super) fn bind_limit(limit_clause: ast::LimitClause) -> EngineResult<(usize, Option<usize>)> {
    match limit_clause {
        ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(not_supported("LIMIT BY is not supported"));
            };
            let fetch = limit.map(|limit| row_count(limit, "LIMIT")).transpose()?;
            let skip = match offset {
                Some(offset) => row_count(offset.value, "OFFSET")?,
                None => 0,
            };
            Ok((skip, fetch))
        }
        ast::LimitClause::OffsetCommaLimit { .. } => Err(not_supported("LIMIT with a comma")),
    }
}

fn row_count(expr: ast::Expr, clause: &str) -> EngineResult<usize> {
    match expr {
        ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::Number(text, _),
            ..
        }) => text.parse::<usize>().map_err(|_| {
            EngineError::Bind(format!(
                "the {clause} clause must be a non-negative integer, got {text}"
            ))
        }),
        other => Err(EngineError::Bind(format!(
            "the {clause} clause must be a non-negative integer, got {other}"
        ))),
    }
}
