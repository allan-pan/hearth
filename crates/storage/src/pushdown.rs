//! Turns a [`Predicate`] and a projection into Vortex expressions, so the Vortex scan does the
//! filtering and reads only the columns it needs.
//!
//! The predicate must already be coerced to the column types (see `Predicate::coerce`), so each
//! literal has the same type as its column. Vortex applies the same SQL NULL rules as
//! [`Predicate`] documents. The scan tests check this on NULL data.

use vortex::dtype::FieldName;
use vortex::expr::{
    Expression, and, col, eq, gt, gt_eq, is_null, lit, lt, lt_eq, not, or, root, select,
};

use crate::scan::{CmpOp, Predicate};

pub(crate) fn predicate_expr(predicate: &Predicate) -> Expression {
    match predicate {
        Predicate::Compare { column, op, value } => {
            let compare = match op {
                CmpOp::Eq => eq,
                CmpOp::Lt => lt,
                CmpOp::Le => lt_eq,
                CmpOp::Gt => gt,
                CmpOp::Ge => gt_eq,
            };
            compare(col(column.as_str()), lit(value.to_scalar()))
        }
        // `IN (a, b, c)` is `col = a OR col = b OR col = c`. An empty list matches nothing.
        Predicate::In { column, values } => values
            .iter()
            .map(|v| eq(col(column.as_str()), lit(v.to_scalar())))
            .reduce(or)
            .unwrap_or_else(|| lit(false)),
        Predicate::IsNull { column } => is_null(col(column.as_str())),
        Predicate::And(parts) => parts
            .iter()
            .map(predicate_expr)
            .reduce(and)
            .unwrap_or_else(|| lit(true)),
        Predicate::Or(parts) => parts
            .iter()
            .map(predicate_expr)
            .reduce(or)
            .unwrap_or_else(|| lit(false)),
        Predicate::Not(inner) => not(predicate_expr(inner)),
        Predicate::True => lit(true),
    }
}

pub(crate) fn projection_expr(columns: &[String]) -> Expression {
    let names: Vec<FieldName> = columns
        .iter()
        .map(|c| FieldName::from(c.as_str()))
        .collect();
    select(names, root())
}
