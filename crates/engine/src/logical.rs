//! Logical plans: trees of operators over tables, with every name resolved and every
//! expression typed. The binder produces them, and every later stage reads them.
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use storage::table::Snapshot;

use crate::expr::Expr;

#[derive(Debug, Clone)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "built by the binder in step 2.1b")
)]
pub enum LogicalPlan {
    /// Reads every column of `table` at the version in `snapshot`.
    Scan { table: String, snapshot: Snapshot },
    /// Computes each expression over its input and outputs it under the paired name.
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, String)>,
    },
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the binder in step 2.1b")
)]
impl LogicalPlan {
    /// The columns this node outputs. Column indexes in the expressions of the node above
    /// refer to positions in this schema.
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { snapshot, .. } => snapshot.schema.clone(),
            LogicalPlan::Project { input, exprs } => {
                let input = input.schema();
                let fields: Vec<Field> = exprs
                    .iter()
                    .map(|(expr, name)| {
                        Field::new(name, expr.data_type(&input), expr.nullable(&input))
                    })
                    .collect();
                Arc::new(Schema::new(fields))
            }
        }
    }
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "reached only through Display, until the binder builds plans in step 2.1b"
    )
)]
impl LogicalPlan {
    /// Writes this node on one line, then its input indented two more spaces.
    fn fmt_indented(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = depth * 2)?;
        match self {
            LogicalPlan::Scan { table, .. } => writeln!(f, "Scan: {table}"),
            LogicalPlan::Project { input, exprs } => {
                write!(f, "Project: ")?;
                for (i, (expr, alias)) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{expr}")?;
                    // Show the output name only when it differs from the column's own name.
                    let same_name = matches!(expr, Expr::Column { name, .. } if name == alias);
                    if !same_name {
                        write!(f, " AS {alias}")?;
                    }
                }
                writeln!(f)?;
                input.fmt_indented(f, depth + 1)
            }
        }
    }
}

/// An indented tree, one node per line, read bottom-up. Plan tests compare against this text.
impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indented(f, 0)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn people() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        LogicalPlan::Scan {
            table: "people".into(),
            snapshot: Snapshot {
                version: 0,
                files: vec![],
                schema,
            },
        }
    }

    fn column(index: usize, name: &str) -> Expr {
        Expr::Column {
            index,
            name: name.into(),
        }
    }

    /// `select name, id from people`
    fn name_then_id() -> LogicalPlan {
        LogicalPlan::Project {
            input: Box::new(people()),
            exprs: vec![
                (column(1, "name"), "name".into()),
                (column(0, "id"), "id".into()),
            ],
        }
    }

    #[test]
    fn display_indents_input_under_project() {
        assert_eq!(
            name_then_id().to_string(),
            "Project: #1 name, #0 id\n  Scan: people\n"
        );
    }

    #[test]
    fn display_shows_alias_only_when_renamed() {
        let plan = LogicalPlan::Project {
            input: Box::new(people()),
            exprs: vec![(column(0, "id"), "key".into())],
        };
        assert_eq!(plan.to_string(), "Project: #0 id AS key\n  Scan: people\n");
    }

    #[test]
    fn scan_schema_is_the_table_schema() {
        let schema = people().schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["id", "name"]);
    }

    #[test]
    fn project_schema_follows_expression_order_types_and_nullability() {
        let schema = name_then_id().schema();
        assert_eq!(
            schema.fields().to_vec(),
            [
                Arc::new(Field::new("name", DataType::Utf8View, true)),
                Arc::new(Field::new("id", DataType::Int64, false)),
            ]
        );
    }
}
