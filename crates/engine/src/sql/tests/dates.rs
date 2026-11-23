//! Date literals and interval folding.
use super::*;

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
