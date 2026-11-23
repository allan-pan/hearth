//! Bound, typed expressions and constants.
use std::fmt;

use arrow::datatypes::{DataType, Schema};

use crate::types;

mod agg;
mod scalar;

pub use agg::{AggExpr, AggFunc};
pub use scalar::ScalarValue;

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
    Aggregate(Box<AggExpr>),
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
                    let (l, r) = (left.data_type(input), right.data_type(input));
                    types::arithmetic_type(*op, &l, &r).unwrap_or_else(|| {
                        unreachable!("the binder coerces {l} {op} {r} to operands it accepts")
                    })
                }
            },
            Expr::Not(_) | Expr::IsNull { .. } | Expr::InList { .. } => DataType::Boolean,
            Expr::Aggregate(agg) => agg.data_type(input),
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
            Expr::Aggregate(agg) => agg.nullable(),
        }
    }

    pub fn contains_aggregate(&self) -> bool {
        match self {
            Expr::Aggregate(_) => true,
            Expr::Column { .. } | Expr::Literal(_) => false,
            Expr::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
            Expr::Not(expr) | Expr::IsNull { expr, .. } | Expr::Cast { expr, .. } => {
                expr.contains_aggregate()
            }
            Expr::InList { expr, list, .. } => {
                expr.contains_aggregate() || list.iter().any(Expr::contains_aggregate)
            }
        }
    }

    pub fn try_map_children<E>(
        self,
        mut f: impl FnMut(Expr) -> Result<Expr, E>,
    ) -> Result<Expr, E> {
        let mut boxed = |expr: Box<Expr>| f(*expr).map(Box::new);
        Ok(match self {
            Expr::Column { .. } | Expr::Literal(_) | Expr::Aggregate(_) => self,
            Expr::Binary { left, op, right } => Expr::Binary {
                left: boxed(left)?,
                op,
                right: boxed(right)?,
            },
            Expr::Not(expr) => Expr::Not(boxed(expr)?),
            Expr::IsNull { expr, negated } => Expr::IsNull {
                expr: boxed(expr)?,
                negated,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: boxed(expr)?,
                list: list.into_iter().map(&mut f).collect::<Result<_, E>>()?,
                negated,
            },
            Expr::Cast { expr, to } => Expr::Cast {
                expr: boxed(expr)?,
                to,
            },
        })
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
            Expr::Aggregate(agg) => write!(f, "{agg}"),
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

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
}
