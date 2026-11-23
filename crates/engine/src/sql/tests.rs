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
        ("select id from people group by all", "GROUP BY ALL"),
        ("select id from people fetch first 1 rows only", "FETCH"),
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
        ("select id / 2 from people", "/"),
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
    where_not_supported("id / 2 = 1");
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

fn dec_lit(value: i128, precision: u8, scale: i8) -> Expr {
    lit(ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    })
}

/// The expression, output name and output type of `select <expr> from items`.
fn items_select(expr: &str) -> (Expr, String, DataType) {
    let plan = bind_sql(&format!("select {expr} from items")).unwrap();
    let data_type = plan.schema().field(0).data_type().clone();
    match plan {
        LogicalPlan::Project { mut exprs, .. } => {
            let (expr, name) = exprs.remove(0);
            (expr, name, data_type)
        }
        other => panic!("expected a Project, got:\n{other}"),
    }
}

#[test]
fn integer_arithmetic_keeps_the_column_type() {
    let (expr, _, data_type) = items_select("qty + 1");
    assert_eq!(
        expr,
        binary(items_column(1), BinaryOp::Add, lit(ScalarValue::Int32(1)))
    );
    assert_eq!(data_type, DataType::Int32);
    assert_eq!(items_select("id * 2").2, DataType::Int64);
}

#[test]
fn integer_columns_of_different_widths_are_widened() {
    let (expr, _, data_type) = items_select("qty + id");
    assert_eq!(
        expr,
        binary(
            cast(items_column(1), DataType::Int64),
            BinaryOp::Add,
            items_column(0)
        )
    );
    assert_eq!(data_type, DataType::Int64);
}

#[test]
fn integer_literal_next_to_a_decimal_becomes_the_narrowest_decimal() {
    let (expr, _, data_type) = items_select("1 - price");
    assert_eq!(
        expr,
        binary(dec_lit(1, 1, 0), BinaryOp::Subtract, items_column(2))
    );
    assert_eq!(data_type, DataType::Decimal128(16, 2));
    assert_eq!(
        items_select("price * 100").0,
        binary(items_column(2), BinaryOp::Multiply, dec_lit(100, 3, 0))
    );
    assert_eq!(
        items_select("price + -5").0,
        binary(items_column(2), BinaryOp::Add, dec_lit(-5, 1, 0))
    );
}

#[test]
fn integer_column_next_to_a_decimal_is_cast_to_a_decimal() {
    let (expr, _, data_type) = items_select("qty * price");
    assert_eq!(
        expr,
        binary(
            cast(items_column(1), DataType::Decimal128(10, 0)),
            BinaryOp::Multiply,
            items_column(2)
        )
    );
    assert_eq!(data_type, DataType::Decimal128(26, 2));
    let (expr, _, data_type) = items_select("id + price");
    assert_eq!(
        expr,
        binary(
            cast(items_column(0), DataType::Decimal128(19, 0)),
            BinaryOp::Add,
            items_column(2)
        )
    );
    assert_eq!(data_type, DataType::Decimal128(22, 2));
}

#[test]
fn decimals_of_different_scales_are_left_as_they_are() {
    let (expr, _, data_type) = items_select("price + 0.005");
    assert_eq!(
        expr,
        binary(items_column(2), BinaryOp::Add, dec_lit(5, 4, 3))
    );
    assert_eq!(data_type, DataType::Decimal128(17, 3));
}

#[test]
fn tpch_q1_and_q6_expression_types() {
    assert_eq!(
        items_select("price * (1 - price)").2,
        DataType::Decimal128(32, 4)
    );
    assert_eq!(
        items_select("price * (1 - price) * (1 + price)").2,
        DataType::Decimal128(38, 6)
    );
    assert_eq!(items_select("price * price").2, DataType::Decimal128(31, 4));
}

#[test]
fn arithmetic_result_is_coerced_in_comparisons() {
    assert_eq!(
        plan_text("select id from items where price * 2 > 10")
            .lines()
            .nth(1)
            .map(str::trim),
        Some("Filter: #2 price * 2 > 10.00")
    );
}

#[test]
fn arithmetic_on_non_numbers_is_a_bind_error() {
    bind_error_message("select name + 1 from people");
    bind_error_message("select id + 'a' from people");
    bind_error_message("select id * true from people");
    bind_error_message("select id + (id = 1) from people");
}

#[test]
fn division_is_not_supported_yet() {
    assert!(not_supported_message("select id / 2 from items").contains('/'));
}

#[test]
fn unaliased_expression_is_named_by_its_sql_text() {
    let (_, name, _) = items_select("price * 2");
    assert_eq!(name, "price * 2");
    assert_eq!(
        plan_text("select id = 1 from people"),
        "Project: #0 id = 1 AS id = 1\n  Scan: people\n"
    );
}

#[test]
fn alias_still_names_an_expression() {
    assert_eq!(items_select("price * 2 as twice").1, "twice");
}

fn shipdate() -> Expr {
    Expr::Column {
        index: 1,
        name: "shipdate".into(),
    }
}

fn date(days: i32) -> Expr {
    lit(ScalarValue::Date32(days))
}

fn orders_sql(predicate: &str) -> String {
    format!("select id from orders where {predicate}")
}

fn orders_predicate(predicate: &str) -> Expr {
    match bind_sql(&orders_sql(predicate)).unwrap() {
        LogicalPlan::Project { input, .. } => match *input {
            LogicalPlan::Filter { predicate, .. } => predicate,
            other => panic!("expected a Filter under the Project, got:\n{other}"),
        },
        other => panic!("expected a Project, got:\n{other}"),
    }
}

/// The date literal that `shipdate = <date_expr>` binds to.
fn folded(date_expr: &str) -> String {
    match orders_predicate(&format!("shipdate = {date_expr}")) {
        Expr::Binary { right, .. } => right.to_string(),
        other => panic!("expected a comparison, got {other}"),
    }
}

#[test]
fn date_literal_binds_to_days_since_the_epoch() {
    assert_eq!(
        orders_predicate("shipdate = date '1994-01-01'"),
        binary(shipdate(), BinaryOp::Eq, date(8766))
    );
    assert_eq!(folded("date '1969-12-31'"), "date '1969-12-31'");
    assert_eq!(
        orders_predicate("shipdate = date '1969-12-31'"),
        binary(shipdate(), BinaryOp::Eq, date(-1))
    );
}

#[test]
fn leap_days_are_valid_only_in_leap_years() {
    assert_eq!(
        orders_predicate("shipdate = date '1996-02-29'"),
        binary(shipdate(), BinaryOp::Eq, date(9555))
    );
    bind_error_message(&orders_sql("shipdate = date '1997-02-29'"));
}

#[test]
fn invalid_date_literals_are_bind_errors() {
    for text in ["1998-13-01", "1998-12-32", "tomorrow", ""] {
        bind_error_message(&orders_sql(&format!("shipdate = date '{text}'")));
    }
}

#[test]
fn other_typed_literals_are_not_supported_yet() {
    not_supported_message(&orders_sql("shipdate = timestamp '1994-01-01 00:00:00'"));
}

#[test]
fn string_literal_compared_with_a_date_becomes_a_date() {
    assert_eq!(
        orders_predicate("shipdate >= '1994-01-01'"),
        binary(shipdate(), BinaryOp::GtEq, date(8766))
    );
    bind_error_message(&orders_sql("shipdate >= 'soon'"));
}

#[test]
fn dates_do_not_compare_with_numbers() {
    bind_error_message(&orders_sql("shipdate = 1"));
}

#[test]
fn tpch_q1_date_filter_is_folded() {
    assert_eq!(
        orders_predicate("shipdate <= date '1998-12-01' - interval '90' day").to_string(),
        "#1 shipdate <= date '1998-09-02'"
    );
}

#[test]
fn tpch_q6_date_range_is_folded() {
    assert_eq!(
        orders_predicate(
            "shipdate >= date '1994-01-01' and shipdate < date '1994-01-01' + interval '1' year"
        )
        .to_string(),
        "#1 shipdate >= date '1994-01-01' AND #1 shipdate < date '1995-01-01'"
    );
}

#[test]
fn intervals_in_days_months_and_years() {
    assert_eq!(
        folded("date '1994-01-01' + interval '1' day"),
        "date '1994-01-02'"
    );
    assert_eq!(
        folded("date '1994-01-01' + interval '3' days"),
        "date '1994-01-04'"
    );
    assert_eq!(
        folded("date '1994-01-01' + interval '2' month"),
        "date '1994-03-01'"
    );
    assert_eq!(
        folded("date '1994-01-01' - interval '2' months"),
        "date '1993-11-01'"
    );
    assert_eq!(
        folded("date '1994-01-01' + interval '2' years"),
        "date '1996-01-01'"
    );
}

#[test]
fn adding_months_clamps_to_the_end_of_the_month() {
    assert_eq!(
        folded("date '1994-01-31' + interval '1' month"),
        "date '1994-02-28'"
    );
    assert_eq!(
        folded("date '1994-03-31' - interval '1' month"),
        "date '1994-02-28'"
    );
    assert_eq!(
        folded("date '1996-02-29' + interval '1' year"),
        "date '1997-02-28'"
    );
}

#[test]
fn interval_can_come_first_when_adding() {
    assert_eq!(
        folded("interval '1' day + date '1993-12-31'"),
        "date '1994-01-01'"
    );
}

#[test]
fn several_intervals_fold_in_order() {
    assert_eq!(
        folded("date '1994-01-01' + interval '1' year - interval '1' day"),
        "date '1994-12-31'"
    );
}

#[test]
fn subtracting_a_date_from_an_interval_is_a_bind_error() {
    bind_error_message(&orders_sql(
        "shipdate = interval '1' day - date '1994-01-01'",
    ));
}

#[test]
fn interval_on_a_column_is_not_supported_yet() {
    not_supported_message(&orders_sql(
        "shipdate + interval '1' day = date '1994-01-02'",
    ));
}

#[test]
fn unsupported_intervals_are_rejected() {
    for interval in [
        "interval '1' hour",
        "interval '1' week",
        "interval '1-2' year to month",
        "interval '1.5' day",
    ] {
        not_supported_message(&orders_sql(&format!(
            "shipdate = date '1994-01-01' + {interval}"
        )));
    }
    not_supported_message("select interval '1' day from orders");
}

#[test]
fn interval_past_the_last_representable_date_is_a_bind_error() {
    bind_error_message(&orders_sql(
        "shipdate = date '1994-01-01' + interval '1000000' year",
    ));
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

#[test]
fn group_by_puts_an_aggregate_between_the_filter_and_the_projection() {
    assert_eq!(
        plan_text(
            "select name, count(*), sum(id) + 1 as total from people where id > 0 group by name"
        ),
        "Project: #0 name, #1 COUNT(*) AS count(*), #2 SUM(#0 id) + 1 AS total\n  \
         Aggregate: group by [#1 name], aggregates [COUNT(*), SUM(#0 id)]\n    \
         Filter: #0 id > 0\n      \
         Scan: people\n"
    );
}

#[test]
fn aggregate_without_group_by_has_no_keys() {
    assert_eq!(
        plan_text("select sum(price * price) as revenue from items where price > 1"),
        "Project: #0 SUM(#2 price * #2 price) AS revenue\n  \
         Aggregate: group by [], aggregates [SUM(#2 price * #2 price)]\n    \
         Filter: #2 price > 1.00\n      \
         Scan: items\n"
    );
    assert_eq!(
        plan_text("select count(*) from people"),
        "Project: #0 COUNT(*) AS count(*)\n  \
         Aggregate: group by [], aggregates [COUNT(*)]\n    \
         Scan: people\n"
    );
}

#[test]
fn group_by_without_aggregates() {
    assert_eq!(
        plan_text("select name from people group by name"),
        "Project: #0 name\n  Aggregate: group by [#1 name], aggregates []\n    Scan: people\n"
    );
}

#[test]
fn aggregate_output_types_and_nullability() {
    assert_eq!(
        schema_of(
            "select qty, sum(price) as s, avg(price) as a, min(price) as lo, \
             count(*) as c, count(price) as n from items group by qty"
        ),
        [
            field("qty", DataType::Int32, false),
            field("s", DataType::Decimal128(38, 2), true),
            field("a", DataType::Float64, true),
            field("lo", DataType::Decimal128(15, 2), true),
            field("c", DataType::Int64, false),
            field("n", DataType::Int64, false),
        ]
    );
}

#[test]
fn tpch_q1_style_aggregates_bind() {
    assert_eq!(
        schema_of(
            "select qty, sum(price * (1 - price)) as disc, \
             sum(price * (1 - price) * (1 + price)) as charge \
             from items where id > 0 group by qty"
        ),
        [
            field("qty", DataType::Int32, false),
            field("disc", DataType::Decimal128(38, 4), true),
            field("charge", DataType::Decimal128(38, 6), true),
        ]
    );
}

#[test]
fn unaliased_aggregates_are_named_by_their_sql_text() {
    assert_eq!(
        schema_of("select count(*), sum(id), max(name) from people"),
        [
            field("count(*)", DataType::Int64, false),
            field("sum(id)", DataType::Int64, true),
            field("max(name)", DataType::Utf8View, true),
        ]
    );
}

#[test]
fn repeated_aggregate_is_computed_once() {
    assert_eq!(
        plan_text("select sum(id), sum(id) + 1 from people"),
        "Project: #0 SUM(#0 id) AS sum(id), #0 SUM(#0 id) + 1 AS sum(id) + 1\n  \
         Aggregate: group by [], aggregates [SUM(#0 id)]\n    \
         Scan: people\n"
    );
}

#[test]
fn group_keys_match_regardless_of_case() {
    assert_eq!(
        plan_text("select NAME, count(*) from people group by name"),
        "Project: #0 name, #1 COUNT(*) AS count(*)\n  \
         Aggregate: group by [#1 name], aggregates [COUNT(*)]\n    \
         Scan: people\n"
    );
}

#[test]
fn expression_keys_match_the_same_expression() {
    assert_eq!(
        schema_of("select id + 1, count(*) from people group by id + 1"),
        [
            field("id + 1", DataType::Int64, false),
            field("count(*)", DataType::Int64, false),
        ]
    );
    bind_error_message("select id + 2 from people group by id + 1");
}

#[test]
fn key_can_be_used_inside_an_expression() {
    assert_eq!(
        schema_of("select id * 2, count(*) from people group by id"),
        [
            field("id * 2", DataType::Int64, false),
            field("count(*)", DataType::Int64, false),
        ]
    );
}

#[test]
fn literals_are_allowed_next_to_aggregates() {
    assert_eq!(
        schema_of("select 1, count(*) from people"),
        [
            field("1", DataType::Int64, false),
            field("count(*)", DataType::Int64, false),
        ]
    );
}

#[test]
fn column_outside_group_by_and_aggregates_is_a_bind_error() {
    assert!(bind_error_message("select id, count(*) from people group by name").contains("id"));
    assert!(bind_error_message("select id + 1 from people group by name").contains("id"));
    assert!(bind_error_message("select id, count(*) from people").contains("id"));
}

#[test]
fn unknown_column_next_to_an_aggregate_is_reported_as_unknown() {
    assert!(bind_error_message("select nme, count(*) from people").contains("does not exist"));
    assert!(bind_error_message("select sum(nme) from people").contains("nme"));
    assert!(bind_error_message("select count(*) from people group by nme").contains("nme"));
}

#[test]
fn star_cannot_be_selected_with_group_by_or_aggregates() {
    bind_error_message("select * from people group by name");
    bind_error_message("select *, count(*) from people");
}

#[test]
fn aggregates_are_not_allowed_in_where_or_inside_aggregates() {
    bind_error_message("select name from people where count(*) > 1");
    bind_error_message("select sum(sum(id)) from people");
}

#[test]
fn sum_and_avg_need_numbers() {
    for sql in [
        "select sum(name) from people",
        "select avg(name) from people",
        "select sum(id = 1) from people",
    ] {
        bind_error_message(sql);
    }
}

#[test]
fn aggregates_take_exactly_one_argument() {
    for sql in [
        "select sum(*) from people",
        "select sum(id, id) from people",
        "select count() from people",
    ] {
        bind_error_message(sql);
    }
}

#[test]
fn unsupported_aggregate_forms_are_rejected() {
    let cases = [
        ("select count(distinct id) from people", "DISTINCT"),
        ("select sum(id) over () from people", "OVER"),
        ("select upper(name) from people", "upper"),
        ("select count(*) from people group by 1", "GROUP BY 1"),
    ];
    for (sql, expected) in cases {
        let msg = not_supported_message(sql);
        assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
    }
}

fn first_line(sql: &str) -> String {
    plan_text(sql)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

#[test]
fn order_by_puts_a_sort_above_the_projection() {
    assert_eq!(
        plan_text("select name, id from people order by id"),
        "Sort: #1 id ASC NULLS LAST\n  Project: #1 name, #0 id\n    Scan: people\n"
    );
}

#[test]
fn descending_sorts_put_nulls_first() {
    assert_eq!(
        first_line("select id from people order by id desc"),
        "Sort: #0 id DESC NULLS FIRST"
    );
    assert_eq!(
        first_line("select id from people order by id asc"),
        "Sort: #0 id ASC NULLS LAST"
    );
}

#[test]
fn several_sort_keys_keep_their_order() {
    assert_eq!(
        first_line("select id, name from people order by name desc, id"),
        "Sort: #1 name DESC NULLS FIRST, #0 id ASC NULLS LAST"
    );
}

#[test]
fn order_by_can_use_an_output_alias() {
    assert_eq!(
        first_line("select id as key from people order by key"),
        "Sort: #0 key ASC NULLS LAST"
    );
}

#[test]
fn order_by_can_use_an_output_position() {
    assert_eq!(
        first_line("select name, id from people order by 2, 1"),
        "Sort: #1 id ASC NULLS LAST, #0 name ASC NULLS LAST"
    );
}

#[test]
fn order_by_position_must_exist() {
    bind_error_message("select name, id from people order by 0");
    bind_error_message("select name, id from people order by 3");
}

#[test]
fn order_by_can_use_an_expression_of_the_outputs() {
    assert_eq!(
        first_line("select id from people order by id + 1"),
        "Sort: #0 id + 1 ASC NULLS LAST"
    );
}

#[test]
fn order_by_all_is_a_column_named_all_in_the_generic_dialect() {
    assert!(bind_error_message("select id from people order by all").contains("all"));
}

#[test]
fn order_by_a_column_that_is_not_selected_is_not_supported_yet() {
    not_supported_message("select name from people order by id");
    assert!(bind_error_message("select name from people order by nme").contains("nme"));
}

#[test]
fn order_by_after_group_by_sorts_the_grouped_output() {
    assert_eq!(
        plan_text("select qty, sum(price) as s from items group by qty order by s desc, qty"),
        "Sort: #1 s DESC NULLS FIRST, #0 qty ASC NULLS LAST\n  \
         Project: #0 qty, #1 SUM(#2 price) AS s\n    \
         Aggregate: group by [#1 qty], aggregates [SUM(#2 price)]\n      \
         Scan: items\n"
    );
}

#[test]
fn unsupported_order_by_forms_are_rejected() {
    let cases = [
        ("select id from people order by id nulls first", "NULLS"),
        ("select id from people order by id nulls last", "NULLS"),
    ];
    for (sql, expected) in cases {
        let msg = not_supported_message(sql);
        assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
    }
}

#[test]
fn limit_puts_a_limit_on_top() {
    assert_eq!(
        plan_text("select id from people limit 10"),
        "Limit: skip 0, fetch 10\n  Project: #0 id\n    Scan: people\n"
    );
    assert_eq!(
        first_line("select id from people limit 0"),
        "Limit: skip 0, fetch 0"
    );
}

#[test]
fn offset_skips_rows() {
    assert_eq!(
        first_line("select id from people limit 10 offset 5"),
        "Limit: skip 5, fetch 10"
    );
    assert_eq!(
        first_line("select id from people offset 5"),
        "Limit: skip 5, fetch all"
    );
}

#[test]
fn limit_goes_above_the_sort() {
    assert_eq!(
        plan_text("select id from people order by id desc limit 3"),
        "Limit: skip 0, fetch 3\n  \
         Sort: #0 id DESC NULLS FIRST\n    \
         Project: #0 id\n      \
         Scan: people\n"
    );
}

#[test]
fn limit_and_offset_must_be_non_negative_integers() {
    for clause in [
        "limit -1",
        "limit 'a'",
        "limit 1.5",
        "limit id",
        "offset -1",
    ] {
        bind_error_message(&format!("select id from people {clause}"));
    }
}

#[test]
fn mysql_limit_with_a_comma_is_not_supported() {
    not_supported_message("select id from people limit 1, 2");
}
