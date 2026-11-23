//! WHERE predicates and the operators and literals they use.
use super::*;

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
