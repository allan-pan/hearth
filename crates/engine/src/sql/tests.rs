use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use storage::table::Snapshot;

use super::*;
use crate::catalog::MemoryCatalog;
use crate::expr::{BinaryOp, Expr, ScalarValue};

fn table(fields: Vec<Field>) -> Snapshot {
    Snapshot {
        version: 0,
        files: vec![],
        schema: Arc::new(Schema::new(fields)),
    }
}

/// Tables: `people(id, name)` and `items(id, qty Int32, price Decimal128(15, 2))`.
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

#[test]
fn binds_columns_in_select_order() {
    assert_eq!(
        plan_text("select name, id from people"),
        "Project: #1 name, #0 id\n  Scan: people\n"
    );
}

#[test]
fn wildcard_expands_to_every_column_in_schema_order() {
    assert_eq!(
        plan_text("select * from people"),
        "Project: #0 id, #1 name\n  Scan: people\n"
    );
}

#[test]
fn alias_names_the_output_column_and_is_case_folded() {
    assert_eq!(
        plan_text("select id as Key from people"),
        "Project: #0 id AS key\n  Scan: people\n"
    );
}

#[test]
fn unquoted_names_are_case_insensitive() {
    assert_eq!(
        plan_text("SELECT NAME FROM People"),
        "Project: #1 name\n  Scan: people\n"
    );
}

#[test]
fn quoted_names_keep_their_case() {
    assert_eq!(
        plan_text(r#"select "name" from "people""#),
        "Project: #1 name\n  Scan: people\n"
    );
    assert!(bind_error_message(r#"select id from "People""#).contains("People"));
    assert!(bind_error_message(r#"select "Name" from people"#).contains("Name"));
}

#[test]
fn unknown_names_are_bind_errors_that_name_them() {
    assert!(bind_error_message("select nme from people").contains("nme"));
    assert!(bind_error_message("select id from persons").contains("persons"));
}

#[test]
fn unsupported_clauses_are_rejected_by_name() {
    let cases = [
        ("select id from people having id = 1", "HAVING"),
        ("select distinct id from people", "DISTINCT"),
        ("select id from people group by id", "GROUP BY"),
        ("select id from people order by id", "ORDER BY"),
        ("select id from people limit 1", "LIMIT"),
        ("with p as (select id from people) select id from p", "WITH"),
        ("select id from people union select id from people", "UNION"),
    ];
    for (sql, clause) in cases {
        let msg = not_supported_message(sql);
        assert!(msg.contains(clause), "{sql:?} gave {msg:?}");
    }
}

#[test]
fn unsupported_from_items_are_rejected() {
    let cases = [
        ("select 1", "without FROM"),
        ("select id from people, people", "more than one table"),
        ("select id from people join people on true", "JOIN"),
        ("select id from people p", "alias"),
        ("select id from myschema.people", "qualified"),
        (
            "select id from (select id from people)",
            "other than a table",
        ),
    ];
    for (sql, expected) in cases {
        let msg = not_supported_message(sql);
        assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
    }
}

#[test]
fn unsupported_select_items_are_rejected() {
    let cases = [
        ("select id + 1 from people", "+"),
        ("select people.* from people", "table.*"),
        ("select * exclude (id) from people", "EXCLUDE"),
    ];
    for (sql, expected) in cases {
        let msg = not_supported_message(sql);
        assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
    }
}

#[test]
fn only_a_single_query_statement_is_accepted() {
    assert!(
        not_supported_message("select id from people; select id from people")
            .contains("2 statements")
    );
    assert!(not_supported_message("delete from people").contains("other than SELECT"));
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

#[test]
fn where_becomes_a_filter_between_scan_and_project() {
    assert_eq!(
        plan_text("select name from people where id = 1"),
        "Project: #1 name\n  Filter: #0 id = 1\n    Scan: people\n"
    );
}

#[test]
fn binds_every_comparison_operator() {
    let cases = [
        ("id = 1", "#0 id = 1"),
        ("id <> 1", "#0 id <> 1"),
        ("id != 1", "#0 id <> 1"),
        ("id < 1", "#0 id < 1"),
        ("id <= 1", "#0 id <= 1"),
        ("id > 1", "#0 id > 1"),
        ("id >= 1", "#0 id >= 1"),
        ("1 = id", "1 = #0 id"),
    ];
    for (sql, expected) in cases {
        assert_eq!(filter_text(sql), expected, "WHERE {sql}");
    }
}

#[test]
fn binds_string_and_boolean_literals() {
    assert_eq!(filter_text("name = 'bob'"), "#1 name = 'bob'");
    assert_eq!(filter_text("name = 'it''s'"), "#1 name = 'it''s'");
    assert_eq!(filter_text("id = 1 and true"), "#0 id = 1 AND true");
}

#[test]
fn and_or_keep_sql_precedence() {
    assert_eq!(
        filter_text("id = 1 or id = 2 and id = 3"),
        "#0 id = 1 OR #0 id = 2 AND #0 id = 3"
    );
    assert_eq!(
        filter_text("(id = 1 or id = 2) and id = 3"),
        "(#0 id = 1 OR #0 id = 2) AND #0 id = 3"
    );
}

#[test]
fn redundant_parentheses_disappear() {
    assert_eq!(filter_text("((id = 1))"), "#0 id = 1");
    assert_eq!(filter_text("(id) = (1)"), "#0 id = 1");
}

#[test]
fn binds_not() {
    assert_eq!(filter_text("not id = 1"), "NOT (#0 id = 1)");
    assert_eq!(filter_text("not (id = 1)"), "NOT (#0 id = 1)");
}

#[test]
fn binds_null_tests() {
    assert_eq!(filter_text("name is null"), "#1 name IS NULL");
    assert_eq!(filter_text("name is not null"), "#1 name IS NOT NULL");
}

#[test]
fn binds_in_lists() {
    assert_eq!(filter_text("id in (1, 2, 3)"), "#0 id IN (1, 2, 3)");
    assert_eq!(filter_text("id not in (1, 2)"), "#0 id NOT IN (1, 2)");
    assert_eq!(filter_text("name in ('a', 'b')"), "#1 name IN ('a', 'b')");
}

#[test]
fn between_becomes_two_comparisons() {
    assert_eq!(
        filter_text("id between 5 and 7"),
        "#0 id >= 5 AND #0 id <= 7"
    );
}

#[test]
fn not_between_becomes_an_or_storage_can_prune() {
    assert_eq!(
        filter_text("id not between 5 and 7"),
        "#0 id < 5 OR #0 id > 7"
    );
}

#[test]
fn between_inside_and_keeps_its_meaning() {
    assert_eq!(
        filter_text("id not between 5 and 7 and name is null"),
        "(#0 id < 5 OR #0 id > 7) AND #1 name IS NULL"
    );
}

#[test]
fn where_must_be_boolean() {
    where_bind_error("id");
    where_bind_error("name");
    where_bind_error("1");
}

#[test]
fn logical_operators_need_boolean_operands() {
    where_bind_error("id and id = 1");
    where_bind_error("id = 1 or name");
    where_bind_error("not id");
}

#[test]
fn comparing_incompatible_types_is_a_bind_error() {
    where_bind_error("name = 1");
    where_bind_error("id = 'a'");
    where_bind_error("id in (1, 'a')");
    where_bind_error("id between 1 and 'z'");
    where_bind_error("id = true");
}

#[test]
fn unsupported_literals_are_rejected() {
    where_not_supported("id = 1e3");
    where_not_supported("name = null");
    where_not_supported("id = 99999999999999999999");
}

#[test]
fn unsupported_predicates_are_rejected() {
    where_not_supported("name like 'a%'");
    where_not_supported("id in (select id from people)");
    where_not_supported("id + 1 = 2");
    where_not_supported("id is true");
}

#[test]
fn unknown_column_in_where_is_a_bind_error() {
    assert!(where_bind_error("nme = 'bob'").contains("nme"));
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

#[test]
fn integer_literal_takes_the_type_of_an_int32_column() {
    let qty = items_column(1);
    assert_eq!(
        items_predicate("qty = 1"),
        binary(qty.clone(), BinaryOp::Eq, lit(ScalarValue::Int32(1)))
    );
    assert_eq!(
        items_predicate("1 = qty"),
        binary(lit(ScalarValue::Int32(1)), BinaryOp::Eq, qty)
    );
}

#[test]
fn integer_literal_takes_the_type_of_a_decimal_column() {
    assert_eq!(
        items_predicate("price < 24"),
        binary(items_column(2), BinaryOp::Lt, lit(cents(2400, 15)))
    );
    assert_eq!(items_filter_text("price < 24"), "#2 price < 24.00");
}

#[test]
fn literal_too_large_for_the_column_widens_the_column_instead() {
    assert_eq!(
        items_predicate("qty = 5000000000"),
        binary(
            cast(items_column(1), DataType::Int64),
            BinaryOp::Eq,
            lit(ScalarValue::Int64(5_000_000_000)),
        )
    );
    let common = DataType::Decimal128(21, 2);
    assert_eq!(
        items_predicate("price = 99999999999999999"),
        binary(
            cast(items_column(2), common.clone()),
            BinaryOp::Eq,
            lit(cents(9_999_999_999_999_999_900, 21)),
        )
    );
}

#[test]
fn columns_of_different_types_are_cast_to_their_common_type() {
    let (id, qty, price) = (items_column(0), items_column(1), items_column(2));
    assert_eq!(
        items_predicate("qty = id"),
        binary(cast(qty.clone(), DataType::Int64), BinaryOp::Eq, id.clone())
    );
    assert_eq!(
        items_predicate("id = qty"),
        binary(id.clone(), BinaryOp::Eq, cast(qty.clone(), DataType::Int64))
    );
    assert_eq!(
        items_predicate("qty = price"),
        binary(
            cast(qty, DataType::Decimal128(15, 2)),
            BinaryOp::Eq,
            price.clone()
        )
    );
    let common = DataType::Decimal128(21, 2);
    assert_eq!(
        items_predicate("id = price"),
        binary(cast(id, common.clone()), BinaryOp::Eq, cast(price, common))
    );
}

#[test]
fn cast_shows_in_plan_text() {
    assert_eq!(
        items_filter_text("qty = id"),
        "CAST(#1 qty AS Int64) = #0 id"
    );
}

#[test]
fn in_list_literals_take_the_column_type() {
    assert_eq!(
        items_predicate("qty in (1, 2)"),
        Expr::InList {
            expr: Box::new(items_column(1)),
            list: vec![lit(ScalarValue::Int32(1)), lit(ScalarValue::Int32(2))],
            negated: false,
        }
    );
}

#[test]
fn in_list_with_a_literal_too_large_widens_the_column() {
    assert_eq!(
        items_predicate("qty in (1, 5000000000)"),
        Expr::InList {
            expr: Box::new(cast(items_column(1), DataType::Int64)),
            list: vec![
                lit(ScalarValue::Int64(1)),
                lit(ScalarValue::Int64(5_000_000_000)),
            ],
            negated: false,
        }
    );
}

#[test]
fn between_bounds_take_the_column_type() {
    assert_eq!(
        items_filter_text("price between 1 and 2"),
        "#2 price >= 1.00 AND #2 price <= 2.00"
    );
}

#[test]
fn incompatible_types_on_other_tables_are_bind_errors() {
    for predicate in ["qty = true", "price = 'a'", "qty in (1, 'a')"] {
        bind_error_message(&format!("select id from items where {predicate}"));
    }
}

#[test]
fn decimal_literals_take_the_column_scale() {
    assert_eq!(
        items_filter_text("price between 0.05 and 0.07"),
        "#2 price >= 0.05 AND #2 price <= 0.07"
    );
    assert_eq!(
        items_predicate("price = 1.5"),
        binary(items_column(2), BinaryOp::Eq, lit(cents(150, 15)))
    );
}

#[test]
fn decimal_literal_with_more_digits_than_the_column_is_not_rounded() {
    let common = DataType::Decimal128(16, 3);
    assert_eq!(
        items_predicate("price = 0.065"),
        binary(
            cast(items_column(2), common.clone()),
            BinaryOp::Eq,
            lit(ScalarValue::Decimal128 {
                value: 65,
                precision: 16,
                scale: 3,
            }),
        )
    );
}

#[test]
fn integer_column_compared_with_a_fraction_becomes_decimal() {
    assert_eq!(
        items_filter_text("qty < 2.5"),
        "CAST(#1 qty AS Decimal128(11, 1)) < 2.5"
    );
    assert_eq!(
        items_filter_text("id = 1.5"),
        "CAST(#0 id AS Decimal128(20, 1)) = 1.5"
    );
}

#[test]
fn negative_integer_literals() {
    assert_eq!(filter_text("id = -1"), "#0 id = -1");
    assert_eq!(filter_text("id in (-1, 2)"), "#0 id IN (-1, 2)");
    assert_eq!(
        items_predicate("qty > -1"),
        binary(items_column(1), BinaryOp::Gt, lit(ScalarValue::Int32(-1)))
    );
}

#[test]
fn negative_decimal_literals() {
    assert_eq!(
        items_predicate("price > -1.5"),
        binary(items_column(2), BinaryOp::Gt, lit(cents(-150, 15)))
    );
    assert_eq!(
        items_filter_text("price between -0.07 and -0.05"),
        "#2 price >= -0.07 AND #2 price <= -0.05"
    );
}

#[test]
fn minus_applied_twice_cancels_out() {
    assert_eq!(filter_text("id = -(-1)"), "#0 id = 1");
    assert_eq!(filter_text("id = - -1"), "#0 id = 1");
}

#[test]
fn smallest_and_largest_negative_i64_literals() {
    assert_eq!(
        filter_text("id = -9223372036854775807"),
        "#0 id = -9223372036854775807"
    );
    where_not_supported("id = -9223372036854775808");
}

#[test]
fn minus_on_a_column_is_not_supported_yet() {
    where_not_supported("-id = 1");
}

#[test]
fn minus_on_a_string_is_a_bind_error() {
    where_bind_error("name = -'a'");
}
