use arrow::datatypes::Schema;
use sqlparser::ast;

use super::bind_expr::bind_expr;
use super::{normalize, not_supported, reject_present};
use crate::error::{EngineError, EngineResult};
use crate::expr::{AggExpr, AggFunc, Expr};
use crate::logical::{LogicalPlan, aggregate_key_name};
use crate::types::aggregate_type;

pub(super) fn is_aggregate(function: &ast::Function) -> bool {
    aggregate_func(&function.name).is_some()
}

pub(super) fn reject_aggregates(expr: &Expr, clause: &str) -> EngineResult<()> {
    if expr.contains_aggregate() {
        return Err(EngineError::Bind(format!(
            "aggregate functions are not allowed in {clause}"
        )));
    }
    Ok(())
}
pub(super) fn bind_aggregation(
    input: LogicalPlan,
    group_by: Vec<ast::Expr>,
    items: Vec<(Expr, String)>,
) -> EngineResult<LogicalPlan> {
    let input_schema = input.schema();
    let keys = group_by
        .into_iter()
        .map(|key| match bind_expr(key, &input_schema)? {
            Expr::Literal(value) => Err(not_supported(&format!("GROUP BY {value}"))),
            key => {
                reject_aggregates(&key, "GROUP BY")?;
                Ok(key)
            }
        })
        .collect::<EngineResult<Vec<_>>>()?;

    let mut grouping = Grouping {
        keys: &keys,
        aggregates: Vec::new(),
    };
    let exprs = items
        .into_iter()
        .map(|(expr, name)| Ok((grouping.rewrite(expr)?, name)))
        .collect::<EngineResult<Vec<_>>>()?;
    let aggregates = grouping.aggregates;

    Ok(LogicalPlan::Project {
        input: Box::new(LogicalPlan::Aggregate {
            input: Box::new(input),
            group_by: keys,
            aggregates,
        }),
        exprs,
    })
}

/// Rewrites bound select items so group keys and aggregate calls become columns of the
/// Aggregate, whose output is the keys followed by the aggregates.
struct Grouping<'a> {
    keys: &'a [Expr],
    aggregates: Vec<AggExpr>,
}

impl Grouping<'_> {
    fn rewrite(&mut self, expr: Expr) -> EngineResult<Expr> {
        if let Some(index) = self.keys.iter().position(|key| *key == expr) {
            return Ok(Expr::Column {
                index,
                name: aggregate_key_name(&self.keys[index]),
            });
        }
        match expr {
            Expr::Aggregate(aggregate) => {
                let position = self.aggregates.iter().position(|a| *a == *aggregate);
                let position = position.unwrap_or_else(|| {
                    self.aggregates.push(*aggregate);
                    self.aggregates.len() - 1
                });
                Ok(Expr::Column {
                    index: self.keys.len() + position,
                    name: self.aggregates[position].to_string(),
                })
            }
            Expr::Column { name, .. } => Err(EngineError::Bind(format!(
                "column {name} must appear in GROUP BY or be used in an aggregate function"
            ))),
            other => other.try_map_children(|child| self.rewrite(child)),
        }
    }
}

fn aggregate_func(name: &ast::ObjectName) -> Option<AggFunc> {
    let [ast::ObjectNamePart::Identifier(ident)] = name.0.as_slice() else {
        return None;
    };
    match normalize(ident).as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "avg" => Some(AggFunc::Avg),
        _ => None,
    }
}

pub(super) fn bind_aggregate(function: ast::Function, input: &Schema) -> EngineResult<AggExpr> {
    let ast::Function {
        name,
        uses_odbc_syntax: _,
        parameters,
        args,
        within_group,
        filter,
        null_treatment,
        over,
    } = function;
    let func =
        aggregate_func(&name).ok_or_else(|| not_supported(&format!("the function {name}")))?;

    let unsupported = [
        ("window functions (OVER)", over.is_some()),
        ("FILTER", filter.is_some()),
        ("WITHIN GROUP", !within_group.is_empty()),
        ("IGNORE/RESPECT NULLS", null_treatment.is_some()),
        (
            "function parameters",
            !matches!(parameters, ast::FunctionArguments::None),
        ),
    ];
    reject_present(&unsupported)?;

    let ast::FunctionArguments::List(ast::FunctionArgumentList {
        duplicate_treatment,
        args,
        clauses,
    }) = args
    else {
        return Err(EngineError::Bind(format!("{name} needs an argument")));
    };
    let unsupported = [
        (
            "DISTINCT in aggregate functions",
            duplicate_treatment.is_some(),
        ),
        ("clauses in aggregate functions", !clauses.is_empty()),
    ];
    reject_present(&unsupported)?;

    let Ok([arg]) = <[ast::FunctionArg; 1]>::try_from(args) else {
        return Err(EngineError::Bind(format!(
            "{name} takes exactly one argument"
        )));
    };
    match (func, arg) {
        (AggFunc::Count, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)) => {
            Ok(AggExpr::CountAll)
        }
        (func, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))) => {
            let expr = bind_expr(arg, input)?;
            reject_aggregates(&expr, "the argument of an aggregate function")?;
            let data_type = expr.data_type(input);
            if aggregate_type(func, &data_type).is_none() {
                return Err(EngineError::Bind(format!(
                    "{name} needs a number, got {expr} of type {data_type}"
                )));
            }
            Ok(AggExpr::Call { func, expr })
        }
        _ => Err(EngineError::Bind(format!("invalid argument to {name}"))),
    }
}
