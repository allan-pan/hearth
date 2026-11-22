//! Logical plans.
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use storage::table::Snapshot;

use crate::expr::Expr;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table: String,
        snapshot: Snapshot,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<(Expr, String)>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { snapshot, .. } => snapshot.schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
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

impl LogicalPlan {
    fn fmt_indented(&self, f: &mut fmt::Formatter<'_>, depth: usize) -> fmt::Result {
        write!(f, "{:indent$}", "", indent = depth * 2)?;
        match self {
            LogicalPlan::Scan { table, .. } => writeln!(f, "Scan: {table}"),
            LogicalPlan::Filter { input, predicate } => {
                writeln!(f, "Filter: {predicate}")?;
                input.fmt_indented(f, depth + 1)
            }
            LogicalPlan::Project { input, exprs } => {
                write!(f, "Project: ")?;
                for (i, (expr, alias)) in exprs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{expr}")?;
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

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indented(f, 0)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::expr::{BinaryOp, ScalarValue};

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

    fn id_is_one() -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(people()),
            predicate: Expr::Binary {
                left: Box::new(column(0, "id")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
            },
        }
    }

    #[test]
    fn display_puts_each_node_above_its_input() {
        let plan = LogicalPlan::Project {
            input: Box::new(id_is_one()),
            exprs: vec![(column(1, "name"), "name".into())],
        };
        assert_eq!(
            plan.to_string(),
            "Project: #1 name\n  Filter: #0 id = 1\n    Scan: people\n"
        );
    }

    #[test]
    fn filter_schema_is_its_input_schema() {
        assert_eq!(id_is_one().schema(), people().schema());
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
