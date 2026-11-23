//! Constant values and exact conversions between their types.
use std::fmt;

use arrow::datatypes::{DataType, Decimal128Type, DecimalType};
use chrono::{Datelike, NaiveDate};

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "not every literal type is bound yet")
)]
pub enum ScalarValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    /// Days since 1970-01-01.
    Date32(i32),
    Utf8(String),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Decimal128 {
                precision, scale, ..
            } => DataType::Decimal128(*precision, *scale),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Utf8(_) => DataType::Utf8View,
        }
    }
    pub fn cast_to(&self, to: &DataType) -> Option<ScalarValue> {
        if self.data_type() == *to {
            return Some(self.clone());
        }
        match (self, to) {
            (ScalarValue::Int32(v), DataType::Int64) => Some(ScalarValue::Int64(i64::from(*v))),
            (ScalarValue::Int64(v), DataType::Int32) => {
                i32::try_from(*v).ok().map(ScalarValue::Int32)
            }
            (ScalarValue::Int32(v), DataType::Decimal128(precision, scale)) => {
                to_decimal(i128::from(*v), 0, *precision, *scale)
            }
            (ScalarValue::Int64(v), DataType::Decimal128(precision, scale)) => {
                to_decimal(i128::from(*v), 0, *precision, *scale)
            }
            (
                ScalarValue::Decimal128 {
                    value, scale: from, ..
                },
                DataType::Decimal128(precision, scale),
            ) => to_decimal(*value, *from, *precision, *scale),
            (ScalarValue::Utf8(text), DataType::Date32) => ScalarValue::parse_date(text),
            _ => None,
        }
    }
    pub fn parse_date(text: &str) -> Option<ScalarValue> {
        let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
        days_from_date(date).map(ScalarValue::Date32)
    }
}

const UNIX_EPOCH_CE_DAYS: i32 = 719_163;

pub fn date_from_days(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_num_days_from_ce_opt(days.checked_add(UNIX_EPOCH_CE_DAYS)?)
}

pub fn days_from_date(date: NaiveDate) -> Option<i32> {
    date.num_days_from_ce().checked_sub(UNIX_EPOCH_CE_DAYS)
}

fn to_decimal(value: i128, from: i8, precision: u8, scale: i8) -> Option<ScalarValue> {
    let diff = i32::from(scale) - i32::from(from);
    let factor = 10i128.checked_pow(diff.unsigned_abs())?;
    let value = if diff >= 0 {
        value.checked_mul(factor)?
    } else if value % factor == 0 {
        value / factor
    } else {
        return None;
    };
    let limit = 10u128.checked_pow(u32::from(precision))?;
    (value.unsigned_abs() < limit).then_some(ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    })
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Boolean(v) => write!(f, "{v}"),
            ScalarValue::Int32(v) => write!(f, "{v}"),
            ScalarValue::Int64(v) => write!(f, "{v}"),
            ScalarValue::Float64(v) => write!(f, "{v}"),
            ScalarValue::Decimal128 {
                value,
                precision,
                scale,
            } => write!(
                f,
                "{}",
                Decimal128Type::format_decimal(*value, *precision, *scale)
            ),
            ScalarValue::Date32(days) => {
                match arrow::temporal_conversions::date32_to_datetime(*days) {
                    Some(dt) => write!(f, "date '{}'", dt.format("%Y-%m-%d")),
                    None => write!(f, "date({days} days)"),
                }
            }
            ScalarValue::Utf8(v) => write!(f, "'{}'", v.replace('\'', "''")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(value: i128, scale: i8) -> String {
        ScalarValue::Decimal128 {
            value,
            precision: 15,
            scale,
        }
        .to_string()
    }

    #[test]
    fn decimal_display_places_the_point() {
        assert_eq!(decimal(1234, 2), "12.34");
        assert_eq!(decimal(100, 0), "100");
    }

    #[test]
    fn decimal_display_keeps_leading_zeros_and_sign() {
        assert_eq!(decimal(5, 2), "0.05");
        assert_eq!(decimal(-5, 2), "-0.05");
    }

    #[test]
    fn date_displays_as_sql_date_literal() {
        assert_eq!(ScalarValue::Date32(0).to_string(), "date '1970-01-01'");
        assert_eq!(ScalarValue::Date32(8766).to_string(), "date '1994-01-01'");
    }

    #[test]
    fn string_display_doubles_quotes() {
        assert_eq!(ScalarValue::Utf8("it's".into()).to_string(), "'it''s'");
    }

    #[test]
    fn literal_types_match_storage_column_types() {
        let cases = [
            (ScalarValue::Boolean(true), DataType::Boolean),
            (ScalarValue::Int32(1), DataType::Int32),
            (ScalarValue::Int64(1), DataType::Int64),
            (ScalarValue::Float64(1.5), DataType::Float64),
            (
                ScalarValue::Decimal128 {
                    value: 6,
                    precision: 15,
                    scale: 2,
                },
                DataType::Decimal128(15, 2),
            ),
            (ScalarValue::Date32(0), DataType::Date32),
            (ScalarValue::Utf8("AIR".into()), DataType::Utf8View),
        ];
        for (value, expected) in cases {
            assert_eq!(value.data_type(), expected, "type of {value}");
        }
    }

    fn dec(value: i128, precision: u8, scale: i8) -> ScalarValue {
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        }
    }

    #[test]
    fn cast_to_same_type_is_unchanged() {
        let values = [
            ScalarValue::Int64(7),
            ScalarValue::Utf8("a".into()),
            ScalarValue::Boolean(true),
            dec(5, 15, 2),
        ];
        for v in values {
            assert_eq!(v.cast_to(&v.data_type()), Some(v.clone()), "{v}");
        }
    }

    #[test]
    fn cast_widens_int32_to_int64() {
        assert_eq!(
            ScalarValue::Int32(i32::MIN).cast_to(&DataType::Int64),
            Some(ScalarValue::Int64(i32::MIN.into()))
        );
    }

    #[test]
    fn cast_narrows_int64_to_int32_only_when_it_fits() {
        let to = DataType::Int32;
        assert_eq!(
            ScalarValue::Int64(i32::MAX.into()).cast_to(&to),
            Some(ScalarValue::Int32(i32::MAX))
        );
        assert_eq!(
            ScalarValue::Int64(i32::MIN.into()).cast_to(&to),
            Some(ScalarValue::Int32(i32::MIN))
        );
        assert_eq!(
            ScalarValue::Int64(i64::from(i32::MAX) + 1).cast_to(&to),
            None
        );
        assert_eq!(
            ScalarValue::Int64(i64::from(i32::MIN) - 1).cast_to(&to),
            None
        );
    }

    #[test]
    fn cast_integer_to_decimal_scales_the_value() {
        let to = DataType::Decimal128(15, 2);
        assert_eq!(ScalarValue::Int64(24).cast_to(&to), Some(dec(2400, 15, 2)));
        assert_eq!(ScalarValue::Int32(-3).cast_to(&to), Some(dec(-300, 15, 2)));
        assert_eq!(ScalarValue::Int64(0).cast_to(&to), Some(dec(0, 15, 2)));
    }

    #[test]
    fn cast_integer_to_decimal_checks_precision_at_the_boundary() {
        let to = DataType::Decimal128(4, 2);
        assert_eq!(ScalarValue::Int64(99).cast_to(&to), Some(dec(9900, 4, 2)));
        assert_eq!(ScalarValue::Int64(-99).cast_to(&to), Some(dec(-9900, 4, 2)));
        assert_eq!(ScalarValue::Int64(100).cast_to(&to), None);
        assert_eq!(ScalarValue::Int64(-100).cast_to(&to), None);
    }

    #[test]
    fn cast_integer_to_decimal_does_not_overflow() {
        let to = DataType::Decimal128(38, 38);
        assert_eq!(ScalarValue::Int64(i64::MAX).cast_to(&to), None);
    }

    #[test]
    fn cast_decimal_to_decimal_rescales_exactly() {
        assert_eq!(
            dec(5, 2, 1).cast_to(&DataType::Decimal128(15, 2)),
            Some(dec(50, 15, 2))
        );
        assert_eq!(
            dec(60, 4, 3).cast_to(&DataType::Decimal128(15, 2)),
            Some(dec(6, 15, 2))
        );
        assert_eq!(
            dec(5, 3, 2).cast_to(&DataType::Decimal128(15, 2)),
            Some(dec(5, 15, 2))
        );
    }

    #[test]
    fn cast_decimal_to_decimal_refuses_to_lose_digits_or_overflow() {
        assert_eq!(dec(65, 4, 3).cast_to(&DataType::Decimal128(15, 2)), None);
        assert_eq!(dec(12345, 15, 2).cast_to(&DataType::Decimal128(4, 2)), None);
    }

    #[test]
    fn cast_refuses_unrelated_types() {
        let cases = [
            (ScalarValue::Utf8("1".into()), DataType::Int64),
            (ScalarValue::Int64(1), DataType::Utf8View),
            (ScalarValue::Boolean(true), DataType::Int64),
            (ScalarValue::Int64(1), DataType::Boolean),
            (ScalarValue::Int32(1), DataType::Date32),
            (ScalarValue::Float64(1.0), DataType::Int64),
            (dec(100, 15, 2), DataType::Int64),
        ];
        for (value, to) in cases {
            assert_eq!(value.cast_to(&to), None, "{value} to {to}");
        }
    }

    #[test]
    fn cast_parses_a_string_as_a_date() {
        assert_eq!(
            ScalarValue::Utf8("1994-01-01".into()).cast_to(&DataType::Date32),
            Some(ScalarValue::Date32(8766))
        );
        assert_eq!(
            ScalarValue::Utf8("1969-12-31".into()).cast_to(&DataType::Date32),
            Some(ScalarValue::Date32(-1))
        );
    }

    #[test]
    fn cast_refuses_a_string_that_is_not_a_date() {
        for text in ["1994-02-30", "1994-1-1x", "soon", ""] {
            assert_eq!(
                ScalarValue::Utf8(text.into()).cast_to(&DataType::Date32),
                None,
                "{text:?}"
            );
        }
        assert_eq!(ScalarValue::Date32(0).cast_to(&DataType::Utf8View), None);
    }
}
