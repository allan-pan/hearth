//! Bound, typed expressions: the per-row calculations inside plan nodes.
use std::fmt;

use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};

/// A calculation evaluated once per row of a plan node's input.
///
/// Column indexes refer to positions in the input's schema. The binder guarantees they are in
/// range, so the methods here index the schema directly.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "built by the binder in step 2.1b")
)]
pub enum Expr {
    /// The column at `index` in the input. `name` is only for display.
    Column { index: usize, name: String },
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the binder in step 2.1b")
)]
impl Expr {
    pub fn data_type(&self, input: &Schema) -> DataType {
        match self {
            Expr::Column { index, .. } => input.field(*index).data_type().clone(),
        }
    }

    pub fn nullable(&self, input: &Schema) -> bool {
        match self {
            Expr::Column { index, .. } => input.field(*index).is_nullable(),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { index, name } => write!(f, "#{index} {name}"),
        }
    }
}

/// A typed constant. The engine's one representation of literals, from binding to execution.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "becomes Expr::Literal in step 2.2")
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

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "used by the binder in step 2.2")
)]
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
            // Storage's string columns are Utf8View, so literals match them.
            ScalarValue::Utf8(_) => DataType::Utf8View,
        }
    }
}

/// Prints the value as SQL would spell it, so plan text reads like the query.
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
                    // Outside chrono's range. Display must not panic, so show the raw value.
                    None => write!(f, "date({days} days)"),
                }
            }
            ScalarValue::Utf8(v) => write!(f, "'{}'", v.replace('\'', "''")),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

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
            // Utf8View, not Utf8: storage's string columns are Utf8View.
            (ScalarValue::Utf8("AIR".into()), DataType::Utf8View),
        ];
        for (value, expected) in cases {
            assert_eq!(value.data_type(), expected, "type of {value}");
        }
    }

    #[test]
    fn column_takes_type_and_nullability_from_input() {
        let input = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]);
        let name = Expr::Column {
            index: 1,
            name: "name".into(),
        };
        assert_eq!(name.data_type(&input), DataType::Utf8View);
        assert!(name.nullable(&input));
        assert_eq!(name.to_string(), "#1 name");
    }
}
