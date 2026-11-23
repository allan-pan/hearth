//! Binds sqlparser's AST into a `LogicalPlan`.
use sqlparser::ast::{GroupByExpr, Ident, Query, Select, SelectFlavor, SetExpr, Statement};

use crate::catalog::Catalog;
use crate::error::{EngineError, EngineResult};
use crate::logical::LogicalPlan;

mod aggregate;
mod bind_expr;
mod coerce;
mod dates;
mod from;
mod projection;
#[cfg(test)]
mod tests;

use aggregate::{bind_aggregation, contains_aggregate};
use bind_expr::{bind_expr, require_boolean};
use from::bind_from;
use projection::bind_projection;

pub fn bind(statements: Vec<Statement>, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let count = statements.len();
    let Ok([statement]) = <[Statement; 1]>::try_from(statements) else {
        return Err(not_supported(&format!(
            "running {count} statements at once (exactly one is supported)"
        )));
    };
    match statement {
        Statement::Query(query) => bind_query(*query, catalog),
        _ => Err(not_supported("statements other than SELECT")),
    }
}

fn bind_query(query: Query, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    let unsupported = [
        ("WITH", with.is_some()),
        ("ORDER BY", order_by.is_some()),
        ("LIMIT", limit_clause.is_some()),
        ("FETCH", fetch.is_some()),
        ("FOR UPDATE/SHARE", !locks.is_empty()),
        ("FOR XML/JSON", for_clause.is_some()),
        ("SETTINGS", settings.is_some()),
        ("FORMAT", format_clause.is_some()),
        ("pipe operators", !pipe_operators.is_empty()),
    ];
    reject_present(&unsupported)?;

    match *body {
        SetExpr::Select(select) => bind_select(*select, catalog),
        SetExpr::SetOperation { op, .. } => Err(not_supported(&op.to_string())),
        _ => Err(not_supported("query bodies other than SELECT")),
    }
}

fn bind_select(select: Select, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        top_before_distinct: _,
        projection,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        flavor,
    } = select;

    let group_by = match group_by {
        GroupByExpr::Expressions(exprs, modifiers) if modifiers.is_empty() => exprs,
        GroupByExpr::Expressions(..) => return Err(not_supported("GROUP BY modifiers")),
        GroupByExpr::All(_) => return Err(not_supported("GROUP BY ALL")),
    };
    let unsupported = [
        ("optimizer hints", !optimizer_hints.is_empty()),
        ("DISTINCT", distinct.is_some()),
        ("SELECT modifiers", select_modifiers.is_some()),
        ("TOP", top.is_some()),
        ("SELECT * EXCLUDE", exclude.is_some()),
        ("SELECT INTO", into.is_some()),
        ("LATERAL VIEW", !lateral_views.is_empty()),
        ("PREWHERE", prewhere.is_some()),
        ("CONNECT BY", !connect_by.is_empty()),
        ("CLUSTER BY", !cluster_by.is_empty()),
        ("DISTRIBUTE BY", !distribute_by.is_empty()),
        ("SORT BY", !sort_by.is_empty()),
        ("HAVING", having.is_some()),
        ("WINDOW", !named_window.is_empty()),
        ("QUALIFY", qualify.is_some()),
        ("SELECT AS VALUE/STRUCT", value_table_mode.is_some()),
        (
            "FROM-first syntax",
            !matches!(flavor, SelectFlavor::Standard),
        ),
    ];
    reject_present(&unsupported)?;

    let scan = bind_from(from, catalog)?;
    let input = match selection {
        Some(predicate) => {
            let schema = scan.schema();
            let predicate = bind_expr(predicate, &schema)?;
            require_boolean(&predicate, &schema, "WHERE")?;
            LogicalPlan::Filter {
                input: Box::new(scan),
                predicate,
            }
        }
        None => scan,
    };
    if !group_by.is_empty() || projection.iter().any(contains_aggregate) {
        return bind_aggregation(input, group_by, projection);
    }
    let exprs = bind_projection(projection, &input.schema())?;

    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs,
    })
}

fn normalize(ident: &Ident) -> String {
    match ident.quote_style {
        None => ident.value.to_lowercase(),
        Some(_) => ident.value.clone(),
    }
}

fn reject_present(clauses: &[(&str, bool)]) -> EngineResult<()> {
    match clauses.iter().find(|(_, present)| *present) {
        Some((clause, _)) => Err(not_supported(clause)),
        None => Ok(()),
    }
}

fn not_supported(what: &str) -> EngineError {
    EngineError::NotSupported(format!("{what} is not supported yet"))
}
