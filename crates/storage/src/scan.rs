//! Scan requests: which columns to read ([`Projection`]) and which rows to keep ([`Predicate`]).

use arrow::datatypes::{Field, Schema};

use crate::error::{StorageError, StorageResult};
use crate::value::Literal;

#[derive(Debug, Clone)]
pub struct Projection {
    pub columns: Vec<String>,
}

impl Projection {
    pub fn new<S: Into<String>>(columns: impl IntoIterator<Item = S>) -> Self {
        Self {
            columns: columns.into_iter().map(Into::into).collect(),
        }
    }

    /// Errors when a column is not in `schema`.
    pub(crate) fn validate(&self, schema: &Schema) -> StorageResult<()> {
        self.columns
            .iter()
            .try_for_each(|name| field(schema, name).map(|_| ()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A row filter. Comparisons follow SQL NULL rules: a comparison with a NULL value is not true,
/// and neither is its negation, so `NOT` drops NULL rows too.
#[derive(Debug, Clone)]
pub enum Predicate {
    Compare {
        column: String,
        op: CmpOp,
        value: Literal,
    },
    In {
        column: String,
        values: Vec<Literal>,
    },
    IsNull {
        column: String,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    True,
}

impl Predicate {
    /// `column op value`, for example `Predicate::compare("id", CmpOp::Gt, 5i64)`.
    pub fn compare(column: impl Into<String>, op: CmpOp, value: impl Into<Literal>) -> Self {
        Predicate::Compare {
            column: column.into(),
            op,
            value: value.into(),
        }
    }

    /// Returns a copy with every literal converted to its column's type.
    ///
    /// Errors when a column is unknown, when a literal can't be converted without changing its
    /// value (see [`Literal`]), and for `IS NULL` on a non-nullable column.
    pub fn coerce(&self, schema: &Schema) -> StorageResult<Predicate> {
        let all = |parts: &[Predicate]| -> StorageResult<Vec<Predicate>> {
            parts.iter().map(|p| p.coerce(schema)).collect()
        };
        Ok(match self {
            Predicate::Compare { column, op, value } => Predicate::Compare {
                column: column.clone(),
                op: *op,
                value: value.coerce_to(field(schema, column)?)?,
            },
            Predicate::In { column, values } => {
                let field = field(schema, column)?;
                Predicate::In {
                    column: column.clone(),
                    values: values
                        .iter()
                        .map(|v| v.coerce_to(field))
                        .collect::<StorageResult<_>>()?,
                }
            }
            Predicate::IsNull { column } => {
                if !field(schema, column)?.is_nullable() {
                    return Err(StorageError::Plan(format!(
                        "column {column} is not nullable"
                    )));
                }
                self.clone()
            }
            Predicate::And(parts) => Predicate::And(all(parts)?),
            Predicate::Or(parts) => Predicate::Or(all(parts)?),
            Predicate::Not(inner) => Predicate::Not(Box::new(inner.coerce(schema)?)),
            Predicate::True => Predicate::True,
        })
    }
}

fn field<'a>(schema: &'a Schema, column: &str) -> StorageResult<&'a Field> {
    schema
        .field_with_name(column)
        .map_err(|_| StorageError::Plan(format!("unknown column {column}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::sample_batch;

    fn schema() -> std::sync::Arc<Schema> {
        sample_batch().schema()
    }

    #[test]
    fn known_projection_column_is_valid() {
        assert!(Projection::new(["name"]).validate(&schema()).is_ok());
    }

    #[test]
    fn unknown_projection_column_is_error() {
        assert!(Projection::new(["missing"]).validate(&schema()).is_err());
    }

    #[test]
    fn coerce_converts_comparison_literal() {
        let coerced = Predicate::compare("id", CmpOp::Eq, "5")
            .coerce(&schema())
            .unwrap();
        assert!(matches!(
            coerced,
            Predicate::Compare {
                op: CmpOp::Eq,
                value: Literal::Int64(5),
                ..
            }
        ));
    }

    #[test]
    fn coerce_converts_every_in_value() {
        let p = Predicate::In {
            column: "id".to_string(),
            values: vec!["1".into(), 2i64.into()],
        };
        let Predicate::In { values, .. } = p.coerce(&schema()).unwrap() else {
            panic!("expected IN");
        };
        assert!(matches!(values[..], [Literal::Int64(1), Literal::Int64(2)]));
    }

    #[test]
    fn coerce_rejects_type_mismatch() {
        let p = Predicate::compare("id", CmpOp::Eq, "two");
        assert!(p.coerce(&schema()).is_err());
    }

    #[test]
    fn coerce_rejects_missing_column() {
        let p = Predicate::compare("missing", CmpOp::Eq, 1i64);
        assert!(p.coerce(&schema()).is_err());
    }

    #[test]
    fn coerce_fails_if_any_part_fails() {
        let p = Predicate::And(vec![
            Predicate::compare("id", CmpOp::Eq, 1i64),
            Predicate::compare("missing", CmpOp::Eq, 1i64),
        ]);
        assert!(p.coerce(&schema()).is_err());
    }

    #[test]
    fn is_null_on_nullable_column_is_valid() {
        let p = Predicate::IsNull {
            column: "name".to_string(),
        };
        assert!(p.coerce(&schema()).is_ok());
    }

    #[test]
    fn is_null_on_non_nullable_column_is_error() {
        let p = Predicate::IsNull {
            column: "id".to_string(),
        };
        assert!(p.coerce(&schema()).is_err());
    }
}
