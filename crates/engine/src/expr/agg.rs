//! Aggregate function calls.
use std::fmt;

use arrow::datatypes::{DataType, Schema};

use super::Expr;
use crate::types;

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
            AggExpr::Call { func, expr } => {
                let input_type = expr.data_type(input);
                types::aggregate_type(*func, &input_type).unwrap_or_else(|| {
                    unreachable!("the binder checks that {func} accepts {input_type}")
                })
            }
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
    use crate::expr::{BinaryOp, ScalarValue};

    /// `a Decimal128(15, 2)`, `c Decimal128(6, 4)`, `i Int32`, `j Int64`, `s Utf8View NULL`.
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Decimal128(15, 2), false),
            Field::new("c", DataType::Decimal128(6, 4), false),
            Field::new("i", DataType::Int32, false),
            Field::new("j", DataType::Int64, false),
            Field::new("s", DataType::Utf8View, true),
        ])
    }

    fn col(index: usize) -> Expr {
        Expr::Column {
            index,
            name: ["a", "c", "i", "j", "s"][index].into(),
        }
    }

    fn agg(func: AggFunc, expr: Expr) -> AggExpr {
        AggExpr::Call { func, expr }
    }

    fn agg_type(func: AggFunc, index: usize) -> DataType {
        agg(func, col(index)).data_type(&schema())
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
            assert_eq!(agg(func, col(3)).to_string(), format!("{sql}(#3 j)"));
        }
        let j_plus_1 = Expr::Binary {
            left: Box::new(col(3)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        };
        assert_eq!(agg(AggFunc::Sum, j_plus_1).to_string(), "SUM(#3 j + 1)");
    }

    #[test]
    fn count_is_int64() {
        assert_eq!(AggExpr::CountAll.data_type(&schema()), DataType::Int64);
        assert_eq!(agg_type(AggFunc::Count, 0), DataType::Int64);
    }

    #[test]
    fn sum_of_integers_is_int64() {
        assert_eq!(agg_type(AggFunc::Sum, 2), DataType::Int64);
        assert_eq!(agg_type(AggFunc::Sum, 3), DataType::Int64);
    }

    #[test]
    fn sum_of_decimals_keeps_the_scale_at_full_precision() {
        assert_eq!(agg_type(AggFunc::Sum, 0), DataType::Decimal128(38, 2));
        assert_eq!(agg_type(AggFunc::Sum, 1), DataType::Decimal128(38, 4));
    }

    #[test]
    fn min_and_max_keep_the_input_type() {
        for func in [AggFunc::Min, AggFunc::Max] {
            assert_eq!(agg_type(func, 0), DataType::Decimal128(15, 2));
            assert_eq!(agg_type(func, 2), DataType::Int32);
            assert_eq!(agg_type(func, 4), DataType::Utf8View);
        }
    }

    #[test]
    fn avg_is_always_float64() {
        for column in [0, 1, 2, 3] {
            assert_eq!(agg_type(AggFunc::Avg, column), DataType::Float64);
        }
    }

    #[test]
    fn counts_are_never_null_other_aggregates_always_can_be() {
        assert!(!AggExpr::CountAll.nullable());
        assert!(!agg(AggFunc::Count, col(4)).nullable());
        for func in [AggFunc::Sum, AggFunc::Min, AggFunc::Max, AggFunc::Avg] {
            assert!(agg(func, col(0)).nullable(), "{func}");
        }
    }
}
