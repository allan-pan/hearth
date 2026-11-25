//! Logical plans.
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use storage::table::Snapshot;

use crate::expr::{AggExpr, Expr};

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
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalPlan>,
        skip: usize,
        fetch: Option<usize>,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            LogicalPlan::Scan { snapshot, .. } => snapshot.schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { input, exprs } => project_schema(&input.schema(), exprs),
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                input,
            } => {
                let input = input.schema();
                let keys = group_by.iter().map(|expr| {
                    Field::new(
                        aggregate_key_name(expr),
                        expr.data_type(&input),
                        expr.nullable(&input),
                    )
                });
                let results = aggregates
                    .iter()
                    .map(|agg| Field::new(agg.to_string(), agg.data_type(&input), agg.nullable()));
                Arc::new(Schema::new(keys.chain(results).collect::<Vec<_>>()))
            }
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
        }
    }

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
                    expr.fmt_aliased(f, alias)?;
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
            LogicalPlan::Sort { input, keys } => {
                write!(f, "Sort: ")?;
                for (i, key) in keys.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{key}")?;
                }
                writeln!(f)?;
                input.fmt_indented(f, depth + 1)
            }
            LogicalPlan::Limit { input, skip, fetch } => {
                write!(f, "Limit: skip {skip}, fetch ")?;
                match fetch {
                    Some(n) => writeln!(f, "{n}")?,
                    None => writeln!(f, "all")?,
                }
                input.fmt_indented(f, depth + 1)
            }
        }
    }
}

pub fn project_schema(input: &Schema, exprs: &[(Expr, String)]) -> SchemaRef {
    let fields: Vec<Field> = exprs
        .iter()
        .map(|(expr, name)| Field::new(name, expr.data_type(input), expr.nullable(input)))
        .collect();
    Arc::new(Schema::new(fields))
}

pub fn aggregate_key_name(key: &Expr) -> String {
    match key {
        Expr::Column { name, .. } => name.clone(),
        _ => key.to_string(),
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_indented(f, 0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl fmt::Display for SortKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.expr,
            if self.ascending { "ASC" } else { "DESC" },
            if self.nulls_first {
                "NULLS FIRST"
            } else {
                "NULLS LAST"
            }
        )
    }
}
#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::expr::{AggExpr, AggFunc, BinaryOp, ScalarValue};
    use crate::logical::SortKey;

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

    fn key(expr: Expr, ascending: bool, nulls_first: bool) -> SortKey {
        SortKey {
            expr,
            ascending,
            nulls_first,
        }
    }

    #[test]
    fn sort_key_display() {
        assert_eq!(
            key(column(0, "id"), true, false).to_string(),
            "#0 id ASC NULLS LAST"
        );
        assert_eq!(
            key(column(1, "name"), false, true).to_string(),
            "#1 name DESC NULLS FIRST"
        );
    }

    #[test]
    fn sort_keeps_its_input_schema() {
        let plan = LogicalPlan::Sort {
            input: Box::new(name_then_id()),
            keys: vec![
                key(column(1, "id"), false, true),
                key(column(0, "name"), true, false),
            ],
        };
        assert_eq!(plan.schema(), name_then_id().schema());
        assert_eq!(
            plan.to_string(),
            "Sort: #1 id DESC NULLS FIRST, #0 name ASC NULLS LAST\n  \
             Project: #1 name, #0 id\n    \
             Scan: people\n"
        );
    }

    #[test]
    fn limit_keeps_its_input_schema() {
        let limit = |skip, fetch| LogicalPlan::Limit {
            input: Box::new(people()),
            skip,
            fetch,
        };
        assert_eq!(limit(0, Some(10)).schema(), people().schema());
        assert_eq!(
            limit(0, Some(10)).to_string(),
            "Limit: skip 0, fetch 10\n  Scan: people\n"
        );
        assert_eq!(
            limit(5, None).to_string(),
            "Limit: skip 5, fetch all\n  Scan: people\n"
        );
    }
}
