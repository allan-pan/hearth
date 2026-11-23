use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use storage::table::Snapshot;

use super::*;
use crate::catalog::MemoryCatalog;
use crate::expr::{BinaryOp, Expr, ScalarValue};

mod aggregate;
mod arithmetic;
mod coerce;
mod dates;
mod order;
mod predicates;
mod select;

fn table(fields: Vec<Field>) -> Snapshot {
    Snapshot {
        version: 0,
        files: vec![],
        schema: Arc::new(Schema::new(fields)),
    }
}

/// Tables: `people(id, name)`, `items(id, qty Int32, price Decimal128(15, 2))` and
/// `orders(id, shipdate Date32)`.
fn bind_sql(sql: &str) -> EngineResult<LogicalPlan> {
    let mut catalog = MemoryCatalog::new();
    catalog.register(
        "people",
        table(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]),
    );
    catalog.register(
        "orders",
        table(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("shipdate", DataType::Date32, false),
        ]),
    );
    catalog.register(
        "items",
        table(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("qty", DataType::Int32, false),
            Field::new("price", DataType::Decimal128(15, 2), false),
        ]),
    );
    let statements = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
    bind(statements, &catalog)
}

fn plan_text(sql: &str) -> String {
    bind_sql(sql).unwrap().to_string()
}

fn not_supported_message(sql: &str) -> String {
    match bind_sql(sql) {
        Err(EngineError::NotSupported(msg)) => msg,
        other => panic!("expected NotSupported for {sql:?}, got {other:?}"),
    }
}

fn bind_error_message(sql: &str) -> String {
    match bind_sql(sql) {
        Err(EngineError::Bind(msg)) => msg,
        other => panic!("expected a Bind error for {sql:?}, got {other:?}"),
    }
}

fn filter_text(predicate: &str) -> String {
    let plan = plan_text(&format!("select name from people where {predicate}"));
    let filter = plan
        .lines()
        .nth(1)
        .unwrap_or_else(|| panic!("no Filter line in:\n{plan}"));
    filter
        .trim()
        .strip_prefix("Filter: ")
        .unwrap_or_else(|| panic!("second line is not a Filter:\n{plan}"))
        .to_string()
}

fn where_not_supported(predicate: &str) -> String {
    not_supported_message(&format!("select name from people where {predicate}"))
}

fn where_bind_error(predicate: &str) -> String {
    bind_error_message(&format!("select name from people where {predicate}"))
}

fn items_column(index: usize) -> Expr {
    let name = ["id", "qty", "price"][index];
    Expr::Column {
        index,
        name: name.into(),
    }
}

fn lit(value: ScalarValue) -> Expr {
    Expr::Literal(value)
}

fn cents(value: i128, precision: u8) -> ScalarValue {
    ScalarValue::Decimal128 {
        value,
        precision,
        scale: 2,
    }
}

fn cast(expr: Expr, to: DataType) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        to,
    }
}

fn binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn items_predicate(predicate: &str) -> Expr {
    let sql = format!("select id from items where {predicate}");
    match bind_sql(&sql).unwrap() {
        LogicalPlan::Project { input, .. } => match *input {
            LogicalPlan::Filter { predicate, .. } => predicate,
            other => panic!("expected a Filter under the Project, got:\n{other}"),
        },
        other => panic!("expected a Project, got:\n{other}"),
    }
}

fn items_filter_text(predicate: &str) -> String {
    items_predicate(predicate).to_string()
}

fn schema_of(sql: &str) -> Vec<(String, DataType, bool)> {
    bind_sql(sql)
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
        .collect()
}

fn field(name: &str, data_type: DataType, nullable: bool) -> (String, DataType, bool) {
    (name.to_string(), data_type, nullable)
}
