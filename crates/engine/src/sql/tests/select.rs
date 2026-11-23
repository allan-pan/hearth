//! SELECT lists, FROM and the statements and clauses accepted.
use super::*;

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
