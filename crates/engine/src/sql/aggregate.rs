use std::ops::ControlFlow;

use arrow::datatypes::{DataType, Schema};
use sqlparser::ast::{self, SelectItem, VisitMut, VisitorMut};

use super::bind_expr::bind_expr;
use super::{normalize, not_supported, reject_present};
use crate::error::{EngineError, EngineResult};
use crate::expr::{AggExpr, AggFunc, Expr};
use crate::logical::{LogicalPlan, aggregate_key_name};

pub(super) fn is_aggregate(function: &ast::Function) -> bool {
    aggregate_func(&function.name).is_some()
}

pub(super) fn contains_aggregate(item: &SelectItem) -> bool {
    let expr = match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => return false,
    };
    ast::visit_expressions(expr, |e| match e {
        ast::Expr::Function(function) if is_aggregate(function) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// Binds a query with GROUP BY or aggregates as a Project over an Aggregate over `input`.
pub(super) fn bind_aggregation(
    input: LogicalPlan,
    group_by: Vec<ast::Expr>,
    projection: Vec<SelectItem>,
) -> EngineResult<LogicalPlan> {
    let input_schema = input.schema();
    let keys = group_by
        .into_iter()
        .map(|key| match bind_expr(key, &input_schema)? {
            Expr::Literal(value) => Err(not_supported(&format!("GROUP BY {value}"))),
            key => Ok(key),
        })
        .collect::<EngineResult<Vec<_>>>()?;

    let mut grouping = Grouping {
        input: &input_schema,
        keys: &keys,
        aggregates: Vec::new(),
    };
    let mut items = Vec::with_capacity(projection.len());
    for item in projection {
        let (mut expr, name) = match item {
            SelectItem::UnnamedExpr(expr) => {
                let name = match &expr {
                    ast::Expr::Identifier(ident) => normalize(ident),
                    other => other.to_string(),
                };
                (expr, name)
            }
            SelectItem::ExprWithAlias { expr, alias } => (expr, normalize(&alias)),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                return Err(EngineError::Bind(
                    "* cannot be used with GROUP BY or aggregate functions".to_string(),
                ));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(not_supported("multiple aliases for one expression"));
            }
        };
        if let ControlFlow::Break(error) = expr.visit(&mut grouping) {
            return Err(error);
        }
        items.push((expr, name));
    }
    let aggregates = grouping.aggregates;

    let aggregate = LogicalPlan::Aggregate {
        input: Box::new(input),
        group_by: keys,
        aggregates,
    };
    let schema = aggregate.schema();
    let exprs = items
        .into_iter()
        .map(|(expr, name)| Ok((bind_expr(expr, &schema)?, name)))
        .collect::<EngineResult<Vec<_>>>()?;
    Ok(LogicalPlan::Project {
        input: Box::new(aggregate),
        exprs,
    })
}

/// Rewrites a select item so aggregate calls and group keys become columns of the Aggregate.
struct Grouping<'a> {
    input: &'a Schema,
    keys: &'a [Expr],
    aggregates: Vec<AggExpr>,
}

impl VisitorMut for Grouping<'_> {
    type Break = EngineError;

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<EngineError> {
        match self.replacement(expr) {
            Ok(Some(column)) => {
                *expr = ast::Expr::Identifier(ast::Ident::with_quote('"', column));
                ControlFlow::Continue(())
            }
            Ok(None) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }
}

impl Grouping<'_> {
    /// The Aggregate output column that replaces `expr`, if any.
    fn replacement(&mut self, expr: &ast::Expr) -> EngineResult<Option<String>> {
        if let ast::Expr::Function(function) = expr
            && is_aggregate(function)
        {
            let aggregate = bind_aggregate(function.clone(), self.input)?;
            let column = aggregate.to_string();
            if !self.aggregates.contains(&aggregate) {
                self.aggregates.push(aggregate);
            }
            return Ok(Some(column));
        }
        match bind_expr(expr.clone(), self.input) {
            Ok(bound) => {
                if let Some(key) = self.keys.iter().find(|key| **key == bound) {
                    return Ok(Some(aggregate_key_name(key)));
                }
                match bound {
                    Expr::Column { name, .. } => Err(EngineError::Bind(format!(
                        "column {name} must appear in GROUP BY or be used in an aggregate function"
                    ))),
                    _ => Ok(None),
                }
            }
            Err(error) if matches!(expr, ast::Expr::Identifier(_)) => Err(error),
            Err(_) => Ok(None),
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

fn bind_aggregate(function: ast::Function, input: &Schema) -> EngineResult<AggExpr> {
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
            let data_type = expr.data_type(input);
            let numeric = matches!(
                data_type,
                DataType::Int32 | DataType::Int64 | DataType::Decimal128(..)
            );
            if matches!(func, AggFunc::Sum | AggFunc::Avg) && !numeric {
                return Err(EngineError::Bind(format!(
                    "{name} needs a number, got {expr} of type {data_type}"
                )));
            }
            Ok(AggExpr::Call { func, expr })
        }
        _ => Err(EngineError::Bind(format!("invalid argument to {name}"))),
    }
}
