use arrow::datatypes::Schema;
use sqlparser::ast::{SelectItem, WildcardAdditionalOptions};

use super::bind_expr::bind_expr;
use super::{normalize, not_supported, reject_present};
use crate::error::EngineResult;
use crate::expr::Expr;

pub(super) fn bind_projection(
    projection: Vec<SelectItem>,
    schema: &Schema,
) -> EngineResult<Vec<(Expr, String)>> {
    let mut exprs = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let text = expr.to_string();
                let expr = bind_expr(expr, schema)?;
                let name = match &expr {
                    Expr::Column { name, .. } => name.clone(),
                    _ => text,
                };
                exprs.push((expr, name));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push((bind_expr(expr, schema)?, normalize(&alias)));
            }
            SelectItem::Wildcard(options) => {
                check_wildcard_options(options)?;
                for (index, field) in schema.fields().iter().enumerate() {
                    let column = Expr::Column {
                        index,
                        name: field.name().clone(),
                    };
                    exprs.push((column, field.name().clone()));
                }
            }
            SelectItem::QualifiedWildcard(..) => return Err(not_supported("table.*")),
            SelectItem::ExprWithAliases { .. } => {
                return Err(not_supported("multiple aliases for one expression"));
            }
        }
    }
    Ok(exprs)
}

fn check_wildcard_options(options: WildcardAdditionalOptions) -> EngineResult<()> {
    let WildcardAdditionalOptions {
        wildcard_token: _,
        opt_ilike,
        opt_exclude,
        opt_except,
        opt_replace,
        opt_rename,
        opt_alias,
    } = options;
    let unsupported = [
        ("* ILIKE", opt_ilike.is_some()),
        ("* EXCLUDE", opt_exclude.is_some()),
        ("* EXCEPT", opt_except.is_some()),
        ("* REPLACE", opt_replace.is_some()),
        ("* RENAME", opt_rename.is_some()),
        ("* AS", opt_alias.is_some()),
    ];
    reject_present(&unsupported)
}
