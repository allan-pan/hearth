//! ORDER BY, LIMIT and OFFSET.
use super::*;

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
