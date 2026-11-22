//! Literal values and everything that depends on their type: coercion to a column's type,
//! ordering, min and max of an Arrow column, and conversion to a Vortex scalar.
//!
//! Supporting a new type means adding a [`Literal`] variant and one arm to each function here.

use std::cmp::Ordering;

use arrow::array::{Array, AsArray};
use arrow::compute::{max, max_string_view, min, min_string_view};
use arrow::datatypes::{DataType, Date32Type, Decimal128Type, Field, Int32Type, Int64Type};
use vortex::dtype::{DecimalDType, Nullability};
use vortex::extension::datetime::{Date, TimeUnit};
use vortex::scalar::{DecimalValue, Scalar};

use crate::error::{StorageError, StorageResult};

#[derive(Debug, Clone)]
pub enum Literal {
    Int64(i64),
    Int32(i32),
    Utf8(String),
    Date32(i32),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

impl From<i64> for Literal {
    fn from(v: i64) -> Self {
        Literal::Int64(v)
    }
}

impl From<&str> for Literal {
    fn from(s: &str) -> Self {
        Literal::Utf8(s.to_string())
    }
}

impl From<String> for Literal {
    fn from(s: String) -> Self {
        Literal::Utf8(s)
    }
}

impl Literal {
    /// Converts to the type of `field`, following the rules of the major engines: a wider
    /// integer narrows only if it fits, a string parses as an integer (surrounding whitespace
    /// allowed) or is an error, and a decimal rescales only if no digit is lost. Anything else
    /// is an error rather than a silent NULL.
    pub(crate) fn coerce_to(&self, field: &Field) -> StorageResult<Literal> {
        let column = field.name();
        let plan = |msg: String| StorageError::Plan(msg);
        Ok(match (self, field.data_type()) {
            (Literal::Int64(v), DataType::Int64) => Literal::Int64(*v),
            (Literal::Int64(v), DataType::Int32) => Literal::Int32(
                i32::try_from(*v)
                    .map_err(|_| plan(format!("value {v} does not fit in column {column}")))?,
            ),
            (Literal::Int32(v), DataType::Int32) => Literal::Int32(*v),
            (Literal::Utf8(s), DataType::Int64) => Literal::Int64(
                s.trim()
                    .parse()
                    .map_err(|_| plan(format!("'{s}' is not an integer for column {column}")))?,
            ),
            (Literal::Utf8(s), DataType::Int32) => Literal::Int32(
                s.trim()
                    .parse()
                    .map_err(|_| plan(format!("'{s}' is not an integer for column {column}")))?,
            ),
            (Literal::Utf8(s), DataType::Utf8View) => Literal::Utf8(s.clone()),
            (Literal::Date32(day), DataType::Date32) => Literal::Date32(*day),
            (
                Literal::Decimal128 { value, scale, .. },
                DataType::Decimal128(col_precision, col_scale),
            ) => {
                let rescaled = rescale(*value, *scale, *col_scale).ok_or_else(|| {
                    plan(format!(
                        "value {value} cannot be rescaled from scale {scale} to {col_scale} for column {column}"
                    ))
                })?;
                let fits = 10u128
                    .checked_pow(*col_precision as u32)
                    .is_some_and(|max| rescaled.unsigned_abs() < max);
                if !fits {
                    return Err(plan(format!(
                        "value {value} does not fit in column {column} with precision {col_precision} and scale {col_scale}"
                    )));
                }
                Literal::Decimal128 {
                    value: rescaled,
                    precision: *col_precision,
                    scale: *col_scale,
                }
            }
            _ => {
                return Err(plan(format!(
                    "column {column} cannot be compared with {self:?}"
                )));
            }
        })
    }

    /// Orders two literals of the same type. `None` for mixed types, and for decimals with
    /// different scales, whose raw values are not comparable.
    pub(crate) fn compare(&self, other: &Literal) -> Option<Ordering> {
        match (self, other) {
            (Literal::Int64(x), Literal::Int64(y)) => Some(x.cmp(y)),
            (Literal::Int32(x), Literal::Int32(y)) => Some(x.cmp(y)),
            (Literal::Utf8(x), Literal::Utf8(y)) => Some(x.cmp(y)),
            (Literal::Date32(x), Literal::Date32(y)) => Some(x.cmp(y)),
            (
                Literal::Decimal128 {
                    value: value_x,
                    scale: scale_x,
                    ..
                },
                Literal::Decimal128 {
                    value: value_y,
                    scale: scale_y,
                    ..
                },
            ) if scale_x == scale_y => Some(value_x.cmp(value_y)),
            _ => None,
        }
    }

    pub(crate) fn to_scalar(&self) -> Scalar {
        match self {
            Literal::Int64(v) => Scalar::from(*v),
            Literal::Int32(v) => Scalar::from(*v),
            Literal::Utf8(s) => Scalar::from(s.as_str()),
            Literal::Date32(days) => Scalar::extension::<Date>(TimeUnit::Days, Scalar::from(*days)),
            Literal::Decimal128 {
                value,
                precision,
                scale,
            } => Scalar::decimal(
                DecimalValue::I128(*value),
                DecimalDType::new(*precision, *scale),
                Nullability::NonNullable,
            ),
        }
    }
}

fn rescale(value: i128, from_scale: i8, to_scale: i8) -> Option<i128> {
    let scale_diff = i32::from(to_scale) - i32::from(from_scale);
    let factor = 10i128.checked_pow(scale_diff.unsigned_abs())?;
    if scale_diff >= 0 {
        value.checked_mul(factor)
    } else if value % factor == 0 {
        Some(value / factor)
    } else {
        None
    }
}

/// Min and max of the non-null values of `column`. `None` when every value is null or the
/// type has no [`Literal`] variant.
pub(crate) fn min_max(column: &dyn Array) -> Option<(Literal, Literal)> {
    fn both<T>(
        min: Option<T>,
        max: Option<T>,
        to_literal: impl Fn(T) -> Literal,
    ) -> Option<(Literal, Literal)> {
        Some((to_literal(min?), to_literal(max?)))
    }

    match column.data_type() {
        DataType::Int64 => {
            let values = column.as_primitive::<Int64Type>();
            both(min(values), max(values), Literal::Int64)
        }
        DataType::Int32 => {
            let values = column.as_primitive::<Int32Type>();
            both(min(values), max(values), Literal::Int32)
        }
        DataType::Date32 => {
            let values = column.as_primitive::<Date32Type>();
            both(min(values), max(values), Literal::Date32)
        }
        DataType::Utf8View => {
            let values = column.as_string_view();
            both(
                min_string_view(values),
                max_string_view(values),
                Literal::from,
            )
        }
        DataType::Decimal128(precision, scale) => {
            let values = column.as_primitive::<Decimal128Type>();
            both(min(values), max(values), |value| Literal::Decimal128 {
                value,
                precision: *precision,
                scale: *scale,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(data_type: DataType) -> Field {
        Field::new("x", data_type, false)
    }

    fn decimal(value: i128, precision: u8, scale: i8) -> Literal {
        Literal::Decimal128 {
            value,
            precision,
            scale,
        }
    }

    #[test]
    fn parses_string_for_int_column() {
        let coerced = Literal::from("5").coerce_to(&field(DataType::Int64));
        assert!(matches!(coerced, Ok(Literal::Int64(5))));
    }

    #[test]
    fn trims_whitespace_around_int_string() {
        let coerced = Literal::from(" 5 ").coerce_to(&field(DataType::Int64));
        assert!(matches!(coerced, Ok(Literal::Int64(5))));
    }

    #[test]
    fn rejects_unparseable_string() {
        assert!(
            Literal::from("two")
                .coerce_to(&field(DataType::Int64))
                .is_err()
        );
    }

    #[test]
    fn rejects_int_for_string_column() {
        assert!(
            Literal::Int64(1)
                .coerce_to(&field(DataType::Utf8View))
                .is_err()
        );
    }

    #[test]
    fn narrows_int64_to_int32_column() {
        let coerced = Literal::Int64(3).coerce_to(&field(DataType::Int32));
        assert!(matches!(coerced, Ok(Literal::Int32(3))));
    }

    #[test]
    fn rejects_int64_out_of_int32_range() {
        assert!(
            Literal::Int64(5_000_000_000)
                .coerce_to(&field(DataType::Int32))
                .is_err()
        );
    }

    #[test]
    fn keeps_date32() {
        let coerced = Literal::Date32(3).coerce_to(&field(DataType::Date32));
        assert!(matches!(coerced, Ok(Literal::Date32(3))));
    }

    #[test]
    fn decimal_keeps_same_scale() {
        let coerced = decimal(1234, 10, 2).coerce_to(&field(DataType::Decimal128(10, 2)));
        assert!(matches!(
            coerced,
            Ok(Literal::Decimal128 {
                value: 1234,
                precision: 10,
                scale: 2
            })
        ));
    }

    #[test]
    fn decimal_rescales_up() {
        let coerced = decimal(1234, 10, 2).coerce_to(&field(DataType::Decimal128(10, 4)));
        assert!(matches!(
            coerced,
            Ok(Literal::Decimal128 {
                value: 123_400,
                precision: 10,
                scale: 4
            })
        ));
    }

    #[test]
    fn decimal_rejects_rescale_that_loses_digits() {
        let coerced = decimal(1234, 10, 4).coerce_to(&field(DataType::Decimal128(10, 2)));
        assert!(coerced.is_err());
    }

    #[test]
    fn decimal_rejects_too_large() {
        let coerced =
            decimal(123_456_789_012, 10, 2).coerce_to(&field(DataType::Decimal128(10, 2)));
        assert!(coerced.is_err());
    }

    #[test]
    fn decimal_accepts_largest_value_for_precision() {
        let coerced = decimal(9999, 4, 2).coerce_to(&field(DataType::Decimal128(4, 2)));
        assert!(coerced.is_ok());
    }

    #[test]
    fn decimal_rejects_one_past_precision() {
        let coerced = decimal(10_000, 4, 2).coerce_to(&field(DataType::Decimal128(4, 2)));
        assert!(coerced.is_err());
    }

    #[test]
    fn compare_orders_dates() {
        let (a, b) = (Literal::Date32(1), Literal::Date32(2));
        assert_eq!(a.compare(&b), Some(Ordering::Less));
        assert_eq!(a.compare(&a), Some(Ordering::Equal));
        assert_eq!(b.compare(&a), Some(Ordering::Greater));
    }

    #[test]
    fn compare_orders_decimals_with_same_scale() {
        let (a, b) = (decimal(150, 15, 2), decimal(225, 15, 2));
        assert_eq!(a.compare(&b), Some(Ordering::Less));
        assert_eq!(a.compare(&a), Some(Ordering::Equal));
        assert_eq!(b.compare(&a), Some(Ordering::Greater));
    }

    #[test]
    fn compare_refuses_decimals_with_different_scales() {
        let (a, b) = (decimal(15, 15, 1), decimal(150, 15, 2));
        assert_eq!(a.compare(&b), None);
        assert_eq!(b.compare(&a), None);
    }

    #[test]
    fn compare_refuses_date_against_int32() {
        assert_eq!(Literal::Date32(1).compare(&Literal::Int32(1)), None);
    }

    #[test]
    fn min_max_of_all_null_column_is_none() {
        let column = arrow::array::Int64Array::from(vec![None, None]);
        assert!(min_max(&column).is_none());
    }

    #[test]
    fn min_max_of_unsupported_type_is_none() {
        let column = arrow::array::Float64Array::from(vec![1.0, 2.0]);
        assert!(min_max(&column).is_none());
    }
}
