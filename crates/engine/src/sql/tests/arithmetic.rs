//! Arithmetic and its result types.
use super::*;

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
