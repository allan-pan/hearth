//! GROUP BY and aggregate functions.
use super::*;

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
fn star_needs_every_column_in_group_by() {
    assert!(bind_error_message("select * from people group by name").contains("id"));
    assert!(bind_error_message("select *, count(*) from people").contains("id"));
    assert_eq!(
        plan_text("select * from people group by id, name"),
        "Project: #0 id, #1 name\n  Aggregate: group by [#0 id, #1 name], aggregates []\n    \
         Scan: people\n"
    );
}

#[test]
fn grouped_and_plain_select_items_are_named_alike() {
    assert_eq!(
        schema_of("select (id) from people group by id"),
        schema_of("select (id) from people")
    );
}

#[test]
fn group_key_named_like_an_aggregate_stays_a_separate_column() {
    let mut catalog = MemoryCatalog::new();
    catalog.register(
        "t",
        table(vec![Field::new("COUNT(*)", DataType::Utf8View, false)]),
    );
    let sql = r#"select "COUNT(*)", count(*) as n from t group by "COUNT(*)""#;
    let plan = bind(
        Parser::parse_sql(&GenericDialect {}, sql).unwrap(),
        &catalog,
    )
    .unwrap();
    assert_eq!(
        plan.to_string(),
        "Project: #0 COUNT(*), #1 COUNT(*) AS n\n  \
         Aggregate: group by [#0 COUNT(*)], aggregates [COUNT(*)]\n    \
         Scan: t\n"
    );
    assert_eq!(plan.schema().field(1).data_type(), &DataType::Int64);
}

#[test]
fn aggregates_are_not_allowed_outside_select_items() {
    let cases = [
        ("select name from people where count(*) > 1", "WHERE"),
        ("select sum(sum(id)) from people", "aggregate function"),
        ("select count(*) from people group by count(*)", "GROUP BY"),
        (
            "select count(*) as c from people order by count(*)",
            "ORDER BY",
        ),
    ];
    for (sql, clause) in cases {
        let msg = bind_error_message(sql);
        assert!(msg.contains(clause), "{sql:?} gave {msg:?}");
    }
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
