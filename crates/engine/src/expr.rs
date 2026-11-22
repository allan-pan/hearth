//! Bound, typed expressions and constants.
use std::fmt;

use arrow::datatypes::{DataType, Decimal128Type, DecimalType, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `name` is only for display.
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
}

impl Expr {
    pub fn data_type(&self, input: &Schema) -> DataType {
        match self {
            Expr::Column { index, .. } => input.field(*index).data_type().clone(),
            Expr::Literal(v) => v.data_type(),
            Expr::Binary { .. } | Expr::Not(_) | Expr::IsNull { .. } | Expr::InList { .. } => {
                DataType::Boolean
            }
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
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column { index, name } => write!(f, "#{index} {name}"),

            Expr::Binary { left, op, right } => {
                fmt_operand(f, left, op.precedence())?;
                write!(f, " {op} ")?;
                fmt_operand(f, right, op.precedence())
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
        }
    }
}

/// Writes one side of a binary operator, in parentheses if it binds more loosely than the
/// operator.
fn fmt_operand(f: &mut fmt::Formatter<'_>, child: &Expr, parent: u8) -> fmt::Result {
    match child {
        Expr::Binary { op, .. } if op.precedence() < parent => write!(f, "({child})"),
        _ => write!(f, "{child}"),
    }
}

/// Writes the input of NOT, IS NULL or IN, in parentheses if it is a binary operator.
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

    /// `id Int64 NOT NULL, name Utf8View NULL, active Boolean NOT NULL`
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
        // ((id = 1 AND id = 2) AND id = 3), as the parser builds it.
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
        // Without the parentheses this would read back as `#0 id = (1 IS NULL)`.
        assert_eq!(is_null(id_eq(1), false).to_string(), "(#0 id = 1) IS NULL");
        let in_list = Expr::InList {
            expr: Box::new(bin(active(), BinaryOp::And, active())),
            list: vec![Expr::Literal(ScalarValue::Boolean(true))],
            negated: false,
        };
        assert_eq!(in_list.to_string(), "(#2 active AND #2 active) IN (true)");
    }
}
