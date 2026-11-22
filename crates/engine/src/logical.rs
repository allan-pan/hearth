//! Logical plans.
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use storage::table::Snapshot;

use crate::expr::{AggExpr, Expr};

#[derive(Debug, Clone)]
#[cfg_attr(not(test), expect(dead_code, reason = "aggregates are not bound yet"))]
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
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
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
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                input,
            } => {
                let input = input.schema();
                let keys = group_by.iter().map(|expr| {
                    let name = match expr {
                        Expr::Column { name, .. } => name.clone(),
                        _ => expr.to_string(),
                    };
                    Field::new(name, expr.data_type(&input), expr.nullable(&input))
                });
                let results = aggregates
                    .iter()
                    .map(|agg| Field::new(agg.to_string(), agg.data_type(&input), agg.nullable()));
                Arc::new(Schema::new(keys.chain(results).collect::<Vec<_>>()))
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
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => {
                write!(f, "Aggregate: group by [")?;
                for (i, expr) in group_by.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{expr}")?;
                }
                write!(f, "], aggregates [")?;
                for (i, agg) in aggregates.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{agg}")?;
                }
                writeln!(f, "]")?;
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
    use crate::expr::{AggExpr, AggFunc, BinaryOp, ScalarValue};

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

    fn sum_of_id() -> AggExpr {
        AggExpr::Call {
            func: AggFunc::Sum,
            expr: column(0, "id"),
        }
    }

    /// `select name, count(*), sum(id) from people group by name`, without the Project.
    fn count_by_name() -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(people()),
            group_by: vec![column(1, "name")],
            aggregates: vec![AggExpr::CountAll, sum_of_id()],
        }
    }

    #[test]
    fn aggregate_display_lists_keys_then_aggregates() {
        assert_eq!(
            count_by_name().to_string(),
            "Aggregate: group by [#1 name], aggregates [COUNT(*), SUM(#0 id)]\n  Scan: people\n"
        );
    }

    #[test]
    fn aggregate_without_keys_shows_an_empty_group_by() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(people()),
            group_by: vec![],
            aggregates: vec![sum_of_id()],
        };
        assert_eq!(
            plan.to_string(),
            "Aggregate: group by [], aggregates [SUM(#0 id)]\n  Scan: people\n"
        );
        assert_eq!(
            plan.schema().fields().to_vec(),
            [Arc::new(Field::new("SUM(#0 id)", DataType::Int64, true))]
        );
    }

    #[test]
    fn aggregate_schema_is_keys_then_aggregates() {
        assert_eq!(
            count_by_name().schema().fields().to_vec(),
            [
                Arc::new(Field::new("name", DataType::Utf8View, true)),
                Arc::new(Field::new("COUNT(*)", DataType::Int64, false)),
                Arc::new(Field::new("SUM(#0 id)", DataType::Int64, true)),
            ]
        );
    }

    #[test]
    fn aggregate_key_expression_is_named_by_its_text() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(people()),
            group_by: vec![Expr::Binary {
                left: Box::new(column(0, "id")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
            }],
            aggregates: vec![AggExpr::CountAll],
        };
        assert_eq!(
            plan.schema().field(0),
            &Field::new("#0 id = 1", DataType::Boolean, false)
        );
    }

    #[test]
    fn project_over_aggregate_refers_to_its_outputs() {
        let plan = LogicalPlan::Project {
            input: Box::new(count_by_name()),
            exprs: vec![
                (column(0, "name"), "name".into()),
                (column(2, "SUM(#0 id)"), "total".into()),
            ],
        };
        assert_eq!(
            plan.to_string(),
            "Project: #0 name, #2 SUM(#0 id) AS total\n  \
             Aggregate: group by [#1 name], aggregates [COUNT(*), SUM(#0 id)]\n    \
             Scan: people\n"
        );
        assert_eq!(
            plan.schema().fields().to_vec(),
            [
                Arc::new(Field::new("name", DataType::Utf8View, true)),
                Arc::new(Field::new("total", DataType::Int64, true)),
            ]
        );
    }
}
