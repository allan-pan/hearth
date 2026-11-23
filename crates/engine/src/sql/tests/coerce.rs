//! Coercing comparison operands to one type.
use super::*;

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
fn literal_probe_takes_the_type_of_its_list_or_bounds() {
    let qty = items_column(1);
    assert_eq!(
        items_predicate("1 in (qty)"),
        Expr::InList {
            expr: Box::new(lit(ScalarValue::Int32(1))),
            list: vec![qty.clone()],
            negated: false,
        }
    );
    assert_eq!(
        items_filter_text("5 between qty and qty"),
        "5 >= #1 qty AND 5 <= #1 qty"
    );
    assert_eq!(
        items_predicate("5 between qty and qty"),
        items_predicate("5 >= qty and 5 <= qty")
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
