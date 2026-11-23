//! Bound, typed expressions and constants.
use std::fmt;

use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};
use chrono::{Datelike, NaiveDate};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        index: usize,
        name: String,
    },
    Literal(ScalarValue),
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    Not(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        to: DataType,
    },
}

impl Expr {
    pub fn data_type(&self, input: &Schema) -> DataType {
        match self {
            Expr::Column { index, .. } => input.field(*index).data_type().clone(),
            Expr::Literal(v) => v.data_type(),
            Expr::Cast { to, .. } => to.clone(),
            Expr::Binary { left, op, right } => match op {
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or => DataType::Boolean,
                BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply => {
                    arithmetic_type(*op, &left.data_type(input), &right.data_type(input))
                }
            },
            Expr::Not(_) | Expr::IsNull { .. } | Expr::InList { .. } => DataType::Boolean,
        }
    }

    pub fn nullable(&self, input: &Schema) -> bool {
        match self {
            Expr::Column { index, .. } => input.field(*index).is_nullable(),
            Expr::Literal(_) => false,
            Expr::Binary { left, right, .. } => left.nullable(input) || right.nullable(input),
            Expr::Not(expr) => expr.nullable(input),
            Expr::IsNull { .. } => false,
            Expr::InList { expr, list, .. } => {
                expr.nullable(input) || list.iter().any(|e| e.nullable(input))
            }
            Expr::Cast { expr, .. } => expr.nullable(input),
        }
    }
}

/// The type Arrow's arithmetic kernels return for these operands.
fn arithmetic_type(op: BinaryOp, left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Int32, DataType::Int32) => DataType::Int32,
        (DataType::Int64, DataType::Int64) => DataType::Int64,
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            let (p1, s1, p2, s2) = (
                i16::from(*p1),
                i16::from(*s1),
                i16::from(*p2),
                i16::from(*s2),
            );
            let (precision, scale) = match op {
                BinaryOp::Multiply => (p1 + p2 + 1, s1 + s2),
                _ => {
                    let scale = s1.max(s2);
                    ((p1 - s1).max(p2 - s2) + scale + 1, scale)
                }
            };
            DataType::Decimal128(precision.min(38) as u8, scale as i8)
        }
        _ => unreachable!("the binder never builds {left} {op} {right}"),
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { index, name } => write!(f, "#{index} {name}"),

            Expr::Binary { left, op, right } => {
                fmt_operand(f, left, op.precedence(), false)?;
                write!(f, " {op} ")?;
                fmt_operand(f, right, op.precedence(), true)
            }
            Expr::Literal(v) => write!(f, "{v}"),
            Expr::Not(expr) => {
                write!(f, "NOT ")?;
                fmt_unary_input(f, expr)
            }
            Expr::IsNull { expr, negated } => {
                fmt_unary_input(f, expr)?;
                let not = if *negated { " NOT" } else { "" };
                write!(f, " IS{not} NULL")
            }
            Expr::Cast { expr, to } => write!(f, "CAST({expr} AS {to})"),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                fmt_unary_input(f, expr)?;
                let not = if *negated { " NOT" } else { "" };
                write!(f, "{not} IN (")?;
                for (i, item) in list.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{item}")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
}

impl BinaryOp {
    fn precedence(&self) -> u8 {
        match self {
            BinaryOp::Or => 1,
            BinaryOp::And => 2,
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => 3,
            BinaryOp::Add | BinaryOp::Subtract => 4,
            BinaryOp::Multiply => 5,
        }
    }
}

impl fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOp::Eq => write!(f, "="),
            BinaryOp::NotEq => write!(f, "<>"),
            BinaryOp::Lt => write!(f, "<"),
            BinaryOp::LtEq => write!(f, "<="),
            BinaryOp::Gt => write!(f, ">"),
            BinaryOp::GtEq => write!(f, ">="),
            BinaryOp::And => write!(f, "AND"),
            BinaryOp::Or => write!(f, "OR"),
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Subtract => write!(f, "-"),
            BinaryOp::Multiply => write!(f, "*"),
        }
    }
}

fn fmt_operand(
    f: &mut fmt::Formatter<'_>,
    child: &Expr,
    parent: u8,
    right_side: bool,
) -> fmt::Result {
    match child {
        Expr::Binary { op, .. }
            if op.precedence() < parent || (right_side && op.precedence() == parent) =>
        {
            write!(f, "({child})")
        }
        _ => write!(f, "{child}"),
    }
}

fn fmt_unary_input(f: &mut fmt::Formatter<'_>, input: &Expr) -> fmt::Result {
    match input {
        Expr::Binary { .. } => write!(f, "({input})"),
        _ => write!(f, "{input}"),
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggFunc::Count => write!(f, "COUNT"),
            AggFunc::Sum => write!(f, "SUM"),
            AggFunc::Min => write!(f, "MIN"),
            AggFunc::Max => write!(f, "MAX"),
            AggFunc::Avg => write!(f, "AVG"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    CountAll,
    Call { func: AggFunc, expr: Expr },
}

impl AggExpr {
    pub fn data_type(&self, input: &Schema) -> DataType {
        match self {
            AggExpr::CountAll => DataType::Int64,
            AggExpr::Call { func, expr } => match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum => match expr.data_type(input) {
                    DataType::Int32 | DataType::Int64 => DataType::Int64,
                    DataType::Decimal128(_, scale) => DataType::Decimal128(38, scale),
                    other => unreachable!("the binder never builds SUM over {other}"),
                },
                AggFunc::Avg => DataType::Float64,
                AggFunc::Max | AggFunc::Min => expr.data_type(input),
            },
        }
    }
    /// Counts are never NULL. The others are NULL over zero rows, whatever their input.
    pub fn nullable(&self) -> bool {
        !matches!(
            self,
            AggExpr::CountAll
                | AggExpr::Call {
                    func: AggFunc::Count,
                    ..
                }
        )
    }
}
impl fmt::Display for AggExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggExpr::CountAll => write!(f, "COUNT(*)"),
            AggExpr::Call { func, expr } => write!(f, "{func}({expr})"),
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

    fn people() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("active", DataType::Boolean, false),
        ])
    }

    fn id() -> Expr {
        Expr::Column {
            index: 0,
            name: "id".into(),
        }
    }

    fn name() -> Expr {
        Expr::Column {
            index: 1,
            name: "name".into(),
        }
    }

    fn active() -> Expr {
        Expr::Column {
            index: 2,
            name: "active".into(),
        }
    }

    fn int(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }

    fn text(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(s.into()))
    }

    fn bin(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    fn id_eq(v: i64) -> Expr {
        bin(id(), BinaryOp::Eq, int(v))
    }

    fn is_null(expr: Expr, negated: bool) -> Expr {
        Expr::IsNull {
            expr: Box::new(expr),
            negated,
        }
    }

    #[test]
    fn literal_has_its_value_type_and_is_never_null() {
        let one = int(1);
        assert_eq!(one.data_type(&people()), DataType::Int64);
        assert!(!one.nullable(&people()));
        assert_eq!(one.to_string(), "1");
    }

    #[test]
    fn predicates_are_boolean() {
        let schema = people();
        let predicates = [
            id_eq(1),
            bin(active(), BinaryOp::And, active()),
            Expr::Not(Box::new(active())),
            is_null(name(), false),
            Expr::InList {
                expr: Box::new(id()),
                list: vec![int(1), int(2)],
                negated: false,
            },
        ];
        for p in predicates {
            assert_eq!(p.data_type(&schema), DataType::Boolean, "type of {p}");
        }
    }

    #[test]
    fn comparison_is_nullable_when_either_side_is() {
        let schema = people();
        assert!(!id_eq(1).nullable(&schema));
        assert!(bin(name(), BinaryOp::Eq, text("bob")).nullable(&schema));
        assert!(bin(text("bob"), BinaryOp::Eq, name()).nullable(&schema));
    }

    #[test]
    fn not_is_nullable_when_its_input_is() {
        let schema = people();
        assert!(!Expr::Not(Box::new(active())).nullable(&schema));
        let name_is_bob = bin(name(), BinaryOp::Eq, text("bob"));
        assert!(Expr::Not(Box::new(name_is_bob)).nullable(&schema));
    }

    #[test]
    fn is_null_is_never_null_even_on_a_nullable_column() {
        assert!(!is_null(name(), false).nullable(&people()));
        assert!(!is_null(name(), true).nullable(&people()));
    }

    #[test]
    fn in_list_is_nullable_when_its_input_is() {
        let schema = people();
        let in_list = |expr| Expr::InList {
            expr: Box::new(expr),
            list: vec![text("a")],
            negated: false,
        };
        assert!(!in_list(text("b")).nullable(&schema));
        assert!(in_list(name()).nullable(&schema));
    }

    #[test]
    fn display_of_null_tests_and_in_lists() {
        assert_eq!(is_null(name(), false).to_string(), "#1 name IS NULL");
        assert_eq!(is_null(name(), true).to_string(), "#1 name IS NOT NULL");
        let in_list = |negated| Expr::InList {
            expr: Box::new(id()),
            list: vec![int(1), int(2), int(3)],
            negated,
        };
        assert_eq!(in_list(false).to_string(), "#0 id IN (1, 2, 3)");
        assert_eq!(in_list(true).to_string(), "#0 id NOT IN (1, 2, 3)");
    }

    #[test]
    fn display_of_comparison_operators() {
        let cases = [
            (BinaryOp::Eq, "="),
            (BinaryOp::NotEq, "<>"),
            (BinaryOp::Lt, "<"),
            (BinaryOp::LtEq, "<="),
            (BinaryOp::Gt, ">"),
            (BinaryOp::GtEq, ">="),
        ];
        for (op, sql) in cases {
            assert_eq!(bin(id(), op, int(1)).to_string(), format!("#0 id {sql} 1"));
        }
    }

    #[test]
    fn and_chain_needs_no_parentheses() {
        let chain = bin(
            bin(id_eq(1), BinaryOp::And, id_eq(2)),
            BinaryOp::And,
            id_eq(3),
        );
        assert_eq!(chain.to_string(), "#0 id = 1 AND #0 id = 2 AND #0 id = 3");
    }

    #[test]
    fn or_under_and_keeps_its_parentheses() {
        let expr = bin(
            bin(id_eq(1), BinaryOp::Or, id_eq(2)),
            BinaryOp::And,
            id_eq(3),
        );
        assert_eq!(expr.to_string(), "(#0 id = 1 OR #0 id = 2) AND #0 id = 3");
    }

    #[test]
    fn and_under_or_needs_no_parentheses() {
        let expr = bin(
            id_eq(1),
            BinaryOp::Or,
            bin(id_eq(2), BinaryOp::And, id_eq(3)),
        );
        assert_eq!(expr.to_string(), "#0 id = 1 OR #0 id = 2 AND #0 id = 3");
    }

    #[test]
    fn not_parenthesizes_a_binary_input_only() {
        assert_eq!(Expr::Not(Box::new(id_eq(1))).to_string(), "NOT (#0 id = 1)");
        assert_eq!(Expr::Not(Box::new(active())).to_string(), "NOT #2 active");
    }

    #[test]
    fn postfix_tests_parenthesize_a_binary_input() {
        assert_eq!(is_null(id_eq(1), false).to_string(), "(#0 id = 1) IS NULL");
        let in_list = Expr::InList {
            expr: Box::new(bin(active(), BinaryOp::And, active())),
            list: vec![Expr::Literal(ScalarValue::Boolean(true))],
            negated: false,
        };
        assert_eq!(in_list.to_string(), "(#2 active AND #2 active) IN (true)");
    }

    fn cast(expr: Expr, to: DataType) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            to,
        }
    }

    #[test]
    fn cast_has_its_target_type_and_its_input_nullability() {
        let schema = people();
        let id_as_decimal = cast(id(), DataType::Decimal128(19, 0));
        assert_eq!(
            id_as_decimal.data_type(&schema),
            DataType::Decimal128(19, 0)
        );
        assert!(!id_as_decimal.nullable(&schema));
        assert!(cast(name(), DataType::Utf8View).nullable(&schema));
    }

    #[test]
    fn cast_display_names_the_target_type() {
        assert_eq!(
            cast(id(), DataType::Decimal128(19, 0)).to_string(),
            "CAST(#0 id AS Decimal128(19, 0))"
        );
    }

    #[test]
    fn cast_is_a_single_operand_for_parentheses() {
        let expr = bin(cast(id(), DataType::Int64), BinaryOp::Eq, int(1));
        assert_eq!(expr.to_string(), "CAST(#0 id AS Int64) = 1");
        assert_eq!(
            cast(id_eq(1), DataType::Boolean).to_string(),
            "CAST(#0 id = 1 AS Boolean)"
        );
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

    /// `a Decimal128(15, 2)`, `b Decimal128(16, 2) NULL`, `c Decimal128(6, 4)`,
    /// `i Int32`, `j Int64`.
    fn numbers() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Decimal128(15, 2), false),
            Field::new("b", DataType::Decimal128(16, 2), true),
            Field::new("c", DataType::Decimal128(6, 4), false),
            Field::new("i", DataType::Int32, false),
            Field::new("j", DataType::Int64, false),
        ])
    }

    fn num(index: usize) -> Expr {
        Expr::Column {
            index,
            name: ["a", "b", "c", "i", "j"][index].into(),
        }
    }

    fn arithmetic_type(left: Expr, op: BinaryOp, right: Expr) -> DataType {
        bin(left, op, right).data_type(&numbers())
    }

    #[test]
    fn decimal_addition_keeps_the_larger_scale_and_adds_a_carry_digit() {
        for op in [BinaryOp::Add, BinaryOp::Subtract] {
            assert_eq!(
                arithmetic_type(num(0), op, num(1)),
                DataType::Decimal128(17, 2)
            );
            assert_eq!(
                arithmetic_type(num(0), op, num(2)),
                DataType::Decimal128(18, 4)
            );
        }
    }

    #[test]
    fn decimal_multiplication_adds_scales_and_precisions() {
        assert_eq!(
            arithmetic_type(num(0), BinaryOp::Multiply, num(1)),
            DataType::Decimal128(32, 4)
        );
    }

    #[test]
    fn decimal_precision_is_capped_at_38() {
        let product = bin(num(0), BinaryOp::Multiply, num(1));
        assert_eq!(
            arithmetic_type(product, BinaryOp::Multiply, num(1)),
            DataType::Decimal128(38, 6)
        );
    }

    #[test]
    fn integer_arithmetic_keeps_the_integer_type() {
        assert_eq!(
            arithmetic_type(num(3), BinaryOp::Add, num(3)),
            DataType::Int32
        );
        assert_eq!(
            arithmetic_type(num(4), BinaryOp::Multiply, num(4)),
            DataType::Int64
        );
    }

    #[test]
    fn arithmetic_is_nullable_when_either_side_is() {
        let schema = numbers();
        assert!(!bin(num(0), BinaryOp::Add, num(2)).nullable(&schema));
        assert!(bin(num(0), BinaryOp::Add, num(1)).nullable(&schema));
    }

    #[test]
    fn display_of_arithmetic_operators() {
        let cases = [
            (BinaryOp::Add, "+"),
            (BinaryOp::Subtract, "-"),
            (BinaryOp::Multiply, "*"),
        ];
        for (op, sql) in cases {
            assert_eq!(bin(id(), op, int(1)).to_string(), format!("#0 id {sql} 1"));
        }
    }

    #[test]
    fn multiplication_binds_tighter_than_addition() {
        let id_times_2 = bin(id(), BinaryOp::Multiply, int(2));
        assert_eq!(
            bin(id(), BinaryOp::Add, id_times_2).to_string(),
            "#0 id + #0 id * 2"
        );
        let id_plus_1 = bin(id(), BinaryOp::Add, int(1));
        assert_eq!(
            bin(id_plus_1, BinaryOp::Multiply, int(2)).to_string(),
            "(#0 id + 1) * 2"
        );
    }

    #[test]
    fn arithmetic_binds_tighter_than_comparison() {
        let id_plus_1 = bin(id(), BinaryOp::Add, int(1));
        assert_eq!(
            bin(id_plus_1, BinaryOp::Eq, int(2)).to_string(),
            "#0 id + 1 = 2"
        );
    }

    #[test]
    fn right_operand_of_equal_precedence_keeps_its_parentheses() {
        let id_minus_1 = || bin(id(), BinaryOp::Subtract, int(1));
        assert_eq!(
            bin(id(), BinaryOp::Subtract, id_minus_1()).to_string(),
            "#0 id - (#0 id - 1)"
        );
        assert_eq!(
            bin(id_minus_1(), BinaryOp::Subtract, int(2)).to_string(),
            "#0 id - 1 - 2"
        );
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

    fn agg(func: AggFunc, expr: Expr) -> AggExpr {
        AggExpr::Call { func, expr }
    }

    #[test]
    fn aggregate_display() {
        assert_eq!(AggExpr::CountAll.to_string(), "COUNT(*)");
        let cases = [
            (AggFunc::Count, "COUNT"),
            (AggFunc::Sum, "SUM"),
            (AggFunc::Min, "MIN"),
            (AggFunc::Max, "MAX"),
            (AggFunc::Avg, "AVG"),
        ];
        for (func, sql) in cases {
            assert_eq!(agg(func, id()).to_string(), format!("{sql}(#0 id)"));
        }
        let id_plus_1 = bin(id(), BinaryOp::Add, int(1));
        assert_eq!(agg(AggFunc::Sum, id_plus_1).to_string(), "SUM(#0 id + 1)");
    }

    #[test]
    fn count_is_int64() {
        let schema = numbers();
        assert_eq!(AggExpr::CountAll.data_type(&schema), DataType::Int64);
        assert_eq!(
            agg(AggFunc::Count, num(0)).data_type(&schema),
            DataType::Int64
        );
    }

    #[test]
    fn sum_of_integers_is_int64() {
        let schema = numbers();
        assert_eq!(
            agg(AggFunc::Sum, num(3)).data_type(&schema),
            DataType::Int64
        );
        assert_eq!(
            agg(AggFunc::Sum, num(4)).data_type(&schema),
            DataType::Int64
        );
    }

    #[test]
    fn sum_of_decimals_keeps_the_scale_at_full_precision() {
        let schema = numbers();
        assert_eq!(
            agg(AggFunc::Sum, num(0)).data_type(&schema),
            DataType::Decimal128(38, 2)
        );
        assert_eq!(
            agg(AggFunc::Sum, num(2)).data_type(&schema),
            DataType::Decimal128(38, 4)
        );
    }

    #[test]
    fn min_and_max_keep_the_input_type() {
        let schema = numbers();
        for func in [AggFunc::Min, AggFunc::Max] {
            assert_eq!(
                agg(func, num(0)).data_type(&schema),
                DataType::Decimal128(15, 2)
            );
            assert_eq!(agg(func, num(3)).data_type(&schema), DataType::Int32);
        }
        assert_eq!(
            agg(AggFunc::Max, name()).data_type(&people()),
            DataType::Utf8View
        );
    }

    #[test]
    fn avg_is_always_float64() {
        let schema = numbers();
        for column in [0, 2, 3, 4] {
            assert_eq!(
                agg(AggFunc::Avg, num(column)).data_type(&schema),
                DataType::Float64
            );
        }
    }

    #[test]
    fn counts_are_never_null_other_aggregates_always_can_be() {
        assert!(!AggExpr::CountAll.nullable());
        assert!(!agg(AggFunc::Count, num(1)).nullable());
        for func in [AggFunc::Sum, AggFunc::Min, AggFunc::Max, AggFunc::Avg] {
            assert!(agg(func, num(0)).nullable(), "{func}");
        }
    }
}
