use arrow::datatypes::Schema;
use chrono::{Days, Months, NaiveDate};
use sqlparser::ast::{self, DateTimeField};

use super::bind_expr::bind_expr;
use super::not_supported;
use crate::error::{EngineError, EngineResult};
use crate::expr::{Expr, ScalarValue, date_from_days, days_from_date};

enum Unit {
    Day,
    Month,
}

pub(super) fn fold_interval(
    date: ast::Expr,
    subtract: bool,
    interval: ast::Interval,
    input: &Schema,
) -> EngineResult<Expr> {
    let days = match bind_expr(date, input)? {
        Expr::Literal(ScalarValue::Date32(days)) => days,
        Expr::Literal(other) => {
            return Err(EngineError::Bind(format!(
                "cannot add an interval to {other}"
            )));
        }
        other => return Err(not_supported(&format!("intervals on {other}"))),
    };
    let (amount, unit) = interval_amount(interval)?;
    let amount = if subtract {
        amount.checked_neg()
    } else {
        Some(amount)
    };
    let shifted = amount
        .zip(date_from_days(days))
        .and_then(|(amount, date)| match unit {
            Unit::Day => shift_days(date, amount),
            Unit::Month => shift_months(date, amount),
        });
    let days = shifted
        .and_then(days_from_date)
        .ok_or_else(|| EngineError::Bind("date arithmetic is out of range".to_string()))?;
    Ok(Expr::Literal(ScalarValue::Date32(days)))
}

fn interval_amount(interval: ast::Interval) -> EngineResult<(i64, Unit)> {
    let unsupported = || not_supported(&format!("the interval {interval}"));
    let ast::Interval {
        value,
        leading_field,
        leading_precision,
        last_field,
        fractional_seconds_precision,
    } = &interval;
    if leading_precision.is_some() || last_field.is_some() || fractional_seconds_precision.is_some()
    {
        return Err(unsupported());
    }
    let amount = match value.as_ref() {
        ast::Expr::Value(ast::ValueWithSpan {
            value: ast::Value::SingleQuotedString(text) | ast::Value::Number(text, _),
            ..
        }) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
    .ok_or_else(unsupported)?;
    match leading_field {
        Some(DateTimeField::Day | DateTimeField::Days) => Ok((amount, Unit::Day)),
        Some(DateTimeField::Month | DateTimeField::Months) => Ok((amount, Unit::Month)),
        Some(DateTimeField::Year | DateTimeField::Years) => amount
            .checked_mul(12)
            .map(|months| (months, Unit::Month))
            .ok_or_else(|| EngineError::Bind("date arithmetic is out of range".to_string())),
        _ => Err(unsupported()),
    }
}

fn shift_days(date: NaiveDate, amount: i64) -> Option<NaiveDate> {
    let days = Days::new(amount.unsigned_abs());
    if amount >= 0 {
        date.checked_add_days(days)
    } else {
        date.checked_sub_days(days)
    }
}

fn shift_months(date: NaiveDate, amount: i64) -> Option<NaiveDate> {
    let months = Months::new(u32::try_from(amount.unsigned_abs()).ok()?);
    if amount >= 0 {
        date.checked_add_months(months)
    } else {
        date.checked_sub_months(months)
    }
}
