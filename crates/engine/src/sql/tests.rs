use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use storage::table::Snapshot;

use super::*;
use crate::catalog::MemoryCatalog;

/// Binds `sql` against a catalog holding `people(id Int64, name Utf8View)`.
fn bind_sql(sql: &str) -> EngineResult<LogicalPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8View, true),
    ]));
    let mut catalog = MemoryCatalog::new();
    catalog.register(
        "people",
        Snapshot {
            version: 0,
            files: vec![],
            schema,
        },
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

/// The plan text of `select name from people where <predicate>`, without the projection
/// and scan lines.
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
    // `id` is filtered on but not selected, so the filter must sit below the projection.
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
    // AND binds tighter than OR, so no parentheses are needed or added.
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
    // NOT binds more loosely than `=`, so both spellings are NOT applied to the comparison.
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
    // NOT BETWEEN's OR must stay grouped when it sits under an AND.
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
fn comparing_different_types_is_not_supported_yet() {
    where_not_supported("name = 1");
    where_not_supported("id = 'a'");
    where_not_supported("id in (1, 'a')");
    where_not_supported("id between 1 and 'z'");
}

#[test]
fn unsupported_literals_are_rejected() {
    where_not_supported("id = 1.5");
    where_not_supported("id = 1e3");
    where_not_supported("name = null");
    // Larger than i64::MAX.
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
