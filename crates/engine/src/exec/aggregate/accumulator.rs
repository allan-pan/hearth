use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Datum, Decimal128Array, Float64Array,
    Int32Array, Int64Array, StringViewArray,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DECIMAL128_MAX_PRECISION, DataType, Date32Type, Decimal128Type,
    Float64Type, Int32Type, Int64Type, Schema,
};

use crate::error::{EngineError, EngineResult};
use crate::exec::eval::ColumnarValue;
use crate::expr::{AggExpr, AggFunc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Min,
    Max,
}

impl Direction {
    fn is_better(self, ordering: Ordering) -> bool {
        match self {
            Self::Min => ordering.is_lt(),
            Self::Max => ordering.is_gt(),
        }
    }
}

enum ExtremumState {
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    Float64(Vec<Option<f64>>),
    Decimal128 {
        precision: u8,
        scale: i8,
        values: Vec<Option<i128>>,
    },
    Date32(Vec<Option<i32>>),
    Utf8View(Vec<Option<String>>),
    Boolean(Vec<Option<bool>>),
}

impl ExtremumState {
    fn new(func: AggFunc, input_type: DataType) -> EngineResult<Self> {
        match input_type {
            DataType::Int32 => Ok(Self::Int32(Vec::new())),
            DataType::Int64 => Ok(Self::Int64(Vec::new())),
            DataType::Float64 => Ok(Self::Float64(Vec::new())),
            DataType::Decimal128(precision, scale) => Ok(Self::Decimal128 {
                precision,
                scale,
                values: Vec::new(),
            }),
            DataType::Date32 => Ok(Self::Date32(Vec::new())),
            DataType::Utf8View => Ok(Self::Utf8View(Vec::new())),
            DataType::Boolean => Ok(Self::Boolean(Vec::new())),
            other => Err(EngineError::Execution(format!(
                "{func} does not support input type {other:?}"
            ))),
        }
    }

    fn resize(&mut self, num_groups: usize) {
        match self {
            Self::Int32(values) | Self::Date32(values) => values.resize(num_groups, None),
            Self::Int64(values) => values.resize(num_groups, None),
            Self::Float64(values) => values.resize(num_groups, None),
            Self::Decimal128 { values, .. } => values.resize(num_groups, None),
            Self::Utf8View(values) => values.resize(num_groups, None),
            Self::Boolean(values) => values.resize(num_groups, None),
        }
    }

    fn update(
        &mut self,
        dir: Direction,
        array: &dyn Array,
        is_scalar: bool,
        group_ids: Option<&[u32]>,
        num_rows: usize,
    ) -> EngineResult<()> {
        let scalar_rows = if is_scalar && group_ids.is_none() {
            1
        } else {
            num_rows
        };
        match self {
            Self::Int32(values) => update_prim_extremum::<Int32Type>(
                values, array, is_scalar, group_ids, scalar_rows, dir, Ord::cmp,
            ),
            Self::Int64(values) => update_prim_extremum::<Int64Type>(
                values, array, is_scalar, group_ids, scalar_rows, dir, Ord::cmp,
            ),
            Self::Float64(values) => update_prim_extremum::<Float64Type>(
                values,
                array,
                is_scalar,
                group_ids,
                scalar_rows,
                dir,
                f64::total_cmp,
            ),
            Self::Decimal128 { values, .. } => update_prim_extremum::<Decimal128Type>(
                values, array, is_scalar, group_ids, scalar_rows, dir, Ord::cmp,
            ),
            Self::Date32(values) => update_prim_extremum::<Date32Type>(
                values, array, is_scalar, group_ids, scalar_rows, dir, Ord::cmp,
            ),
            Self::Boolean(values) => {
                let typed = array.as_boolean();
                visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    scalar_rows,
                    |row_idx| typed.value(row_idx),
                    |group, val| {
                        update_opt(&mut values[group], val, |candidate, current| {
                            dir.is_better(candidate.cmp(&current))
                        });
                        Ok(())
                    },
                )
            }
            Self::Utf8View(values) => {
                let typed = array.as_string_view();
                visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    scalar_rows,
                    |row_idx| typed.value(row_idx),
                    |group, val| {
                        if values[group]
                            .as_deref()
                            .is_none_or(|current| dir.is_better(val.cmp(current)))
                        {
                            values[group] = Some(val.to_owned());
                        }
                        Ok(())
                    },
                )
            }
        }
    }

    fn merge_from(
        &mut self,
        dir: Direction,
        other: Self,
        group_map: &[u32],
    ) -> EngineResult<()> {
        match (self, other) {
            (Self::Int32(target), Self::Int32(source))
            | (Self::Date32(target), Self::Date32(source)) => {
                merge_opt(target, source, group_map, |candidate, current| {
                    dir.is_better(candidate.cmp(&current))
                });
            }
            (Self::Int64(target), Self::Int64(source)) => {
                merge_opt(target, source, group_map, |candidate, current| {
                    dir.is_better(candidate.cmp(&current))
                });
            }
            (Self::Float64(target), Self::Float64(source)) => {
                merge_opt(target, source, group_map, |candidate, current| {
                    dir.is_better(candidate.total_cmp(&current))
                });
            }
            (
                Self::Decimal128 {
                    precision: target_precision,
                    scale: target_scale,
                    values: target,
                },
                Self::Decimal128 {
                    precision: source_precision,
                    scale: source_scale,
                    values: source,
                },
            ) if *target_precision == source_precision && *target_scale == source_scale => {
                merge_opt(target, source, group_map, |candidate, current| {
                    dir.is_better(candidate.cmp(&current))
                });
            }
            (Self::Boolean(target), Self::Boolean(source)) => {
                merge_opt(target, source, group_map, |candidate, current| {
                    dir.is_better(candidate.cmp(&current))
                });
            }
            (Self::Utf8View(target), Self::Utf8View(source)) => {
                for (old_group, val) in source.into_iter().enumerate() {
                    if let Some(text) = val {
                        let slot = &mut target[group_map[old_group] as usize];
                        if slot
                            .as_deref()
                            .is_none_or(|current| dir.is_better(text.as_str().cmp(current)))
                        {
                            *slot = Some(text);
                        }
                    }
                }
            }
            _ => {
                return Err(EngineError::Execution(
                    "cannot merge accumulators of different types".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn finalize(self) -> EngineResult<ArrayRef> {
        let array: ArrayRef = match self {
            Self::Int32(values) => Arc::new(Int32Array::from(values)),
            Self::Int64(values) => Arc::new(Int64Array::from(values)),
            Self::Float64(values) => Arc::new(Float64Array::from(values)),
            Self::Decimal128 {
                precision,
                scale,
                values,
            } => Arc::new(
                Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
            ),
            Self::Date32(values) => Arc::new(Date32Array::from(values)),
            Self::Utf8View(values) => Arc::new(StringViewArray::from_iter(values)),
            Self::Boolean(values) => Arc::new(BooleanArray::from(values)),
        };
        Ok(array)
    }
}

pub struct Accumulator(State);

enum State {
    CountAll(Vec<i64>),
    Count(Vec<i64>),
    SumInt64(Vec<Option<i64>>),
    SumFloat64(Vec<Option<f64>>),
    SumDecimal128 {
        scale: i8,
        values: Vec<Option<i128>>,
    },
    /// Handles `AVG` over `Int32`/`Int64` (`scale = 0`) and `Decimal128(_, scale)`.
    AvgScaled {
        scale: i8,
        sums: Vec<i128>,
        counts: Vec<i64>,
    },
    AvgFloat64 {
        sums: Vec<f64>,
        counts: Vec<i64>,
    },
    Extremum {
        dir: Direction,
        state: ExtremumState,
    },
}

impl Accumulator {
    pub fn new(agg: &AggExpr, input_schema: &Schema) -> EngineResult<Self> {
        use State::*;
        let state = match agg {
            AggExpr::CountAll => CountAll(Vec::new()),
            AggExpr::Call { func, expr } => {
                let input_type = expr.data_type(input_schema);
                match func {
                    AggFunc::Count => Count(Vec::new()),
                    AggFunc::Sum => match input_type {
                        DataType::Int32 | DataType::Int64 => SumInt64(Vec::new()),
                        DataType::Float64 => SumFloat64(Vec::new()),
                        DataType::Decimal128(_, scale) => SumDecimal128 {
                            scale,
                            values: Vec::new(),
                        },
                        other => {
                            return Err(EngineError::Execution(format!(
                                "SUM does not support input type {other:?}"
                            )));
                        }
                    },
                    AggFunc::Avg => match input_type {
                        DataType::Int32 | DataType::Int64 => AvgScaled {
                            scale: 0,
                            sums: Vec::new(),
                            counts: Vec::new(),
                        },
                        DataType::Decimal128(_, scale) => AvgScaled {
                            scale,
                            sums: Vec::new(),
                            counts: Vec::new(),
                        },
                        DataType::Float64 => AvgFloat64 {
                            sums: Vec::new(),
                            counts: Vec::new(),
                        },
                        other => {
                            return Err(EngineError::Execution(format!(
                                "AVG does not support input type {other:?}"
                            )));
                        }
                    },
                    AggFunc::Min | AggFunc::Max => {
                        let dir = if *func == AggFunc::Min {
                            Direction::Min
                        } else {
                            Direction::Max
                        };
                        Extremum {
                            dir,
                            state: ExtremumState::new(*func, input_type)?,
                        }
                    }
                }
            }
        };
        Ok(Self(state))
    }

    pub fn resize(&mut self, num_groups: usize) {
        use State::*;
        match &mut self.0 {
            CountAll(counts) | Count(counts) => counts.resize(num_groups, 0),
            SumInt64(values) => values.resize(num_groups, None),
            SumFloat64(values) => values.resize(num_groups, None),
            SumDecimal128 { values, .. } => values.resize(num_groups, None),
            AvgScaled { sums, counts, .. } => {
                sums.resize(num_groups, 0);
                counts.resize(num_groups, 0);
            }
            AvgFloat64 { sums, counts } => {
                sums.resize(num_groups, 0.0);
                counts.resize(num_groups, 0);
            }
            Extremum { state, .. } => state.resize(num_groups),
        }
    }

    pub fn update_batch(
        &mut self,
        value: Option<&ColumnarValue>,
        group_ids: &[u32],
    ) -> EngineResult<()> {
        self.update(value, Some(group_ids), group_ids.len())
    }

    pub fn update_single(
        &mut self,
        value: Option<&ColumnarValue>,
        num_rows: usize,
    ) -> EngineResult<()> {
        self.update(value, None, num_rows)
    }

    fn update(
        &mut self,
        value: Option<&ColumnarValue>,
        group_ids: Option<&[u32]>,
        num_rows: usize,
    ) -> EngineResult<()> {
        if num_rows == 0 {
            return Ok(());
        }
        use State::*;
        if let CountAll(counts) = &mut self.0 {
            match group_ids {
                Some(gids) => {
                    for &group in gids {
                        counts[group as usize] += 1;
                    }
                }
                None => counts[0] += num_rows as i64,
            }
            return Ok(());
        }

        let (array, is_scalar) = value
            .ok_or_else(|| EngineError::Execution("aggregate requires an input value".to_string()))?
            .get();
        debug_assert!(is_scalar || array.len() == num_rows);

        match &mut self.0 {
            CountAll(_) => unreachable!(),
            Count(counts) => match (is_scalar, group_ids) {
                (false, None) => counts[0] += (array.len() - array.null_count()) as i64,
                (true, None) => {
                    if array.is_valid(0) {
                        counts[0] += num_rows as i64;
                    }
                }
                _ => visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    num_rows,
                    |_| (),
                    |group, ()| {
                        counts[group] += 1;
                        Ok(())
                    },
                )?,
            },
            SumInt64(values) => match array.data_type() {
                DataType::Int32 => {
                    let typed = array.as_primitive::<Int32Type>();
                    visit_rows(
                        array,
                        is_scalar,
                        group_ids,
                        num_rows,
                        |row_idx| i64::from(typed.value(row_idx)),
                        |group, val| add_opt_i64(&mut values[group], val),
                    )?;
                }
                DataType::Int64 => {
                    let typed = array.as_primitive::<Int64Type>();
                    visit_rows(
                        array,
                        is_scalar,
                        group_ids,
                        num_rows,
                        |row_idx| typed.value(row_idx),
                        |group, val| add_opt_i64(&mut values[group], val),
                    )?;
                }
                other => {
                    return Err(EngineError::Execution(format!(
                        "SUM(Int64) received unexpected type {other:?}"
                    )));
                }
            },
            SumFloat64(values) => {
                let typed = array.as_primitive::<Float64Type>();
                visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    num_rows,
                    |row_idx| typed.value(row_idx),
                    |group, val| {
                        values[group] = Some(values[group].map_or(val, |current| current + val));
                        Ok(())
                    },
                )?;
            }
            SumDecimal128 { values, .. } => {
                let typed = array.as_primitive::<Decimal128Type>();
                visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    num_rows,
                    |row_idx| typed.value(row_idx),
                    |group, val| add_opt_decimal(&mut values[group], val),
                )?;
            }
            AvgScaled { sums, counts, .. } => match array.data_type() {
                DataType::Int32 => {
                    let typed = array.as_primitive::<Int32Type>();
                    visit_rows(
                        array,
                        is_scalar,
                        group_ids,
                        num_rows,
                        |row_idx| i128::from(typed.value(row_idx)),
                        |group, val| add_avg_i128(&mut sums[group], &mut counts[group], val),
                    )?;
                }
                DataType::Int64 => {
                    let typed = array.as_primitive::<Int64Type>();
                    visit_rows(
                        array,
                        is_scalar,
                        group_ids,
                        num_rows,
                        |row_idx| i128::from(typed.value(row_idx)),
                        |group, val| add_avg_i128(&mut sums[group], &mut counts[group], val),
                    )?;
                }
                DataType::Decimal128(..) => {
                    let typed = array.as_primitive::<Decimal128Type>();
                    visit_rows(
                        array,
                        is_scalar,
                        group_ids,
                        num_rows,
                        |row_idx| typed.value(row_idx),
                        |group, val| add_avg_i128(&mut sums[group], &mut counts[group], val),
                    )?;
                }
                other => {
                    return Err(EngineError::Execution(format!(
                        "AVG received unexpected type {other:?}"
                    )));
                }
            },
            AvgFloat64 { sums, counts } => {
                let typed = array.as_primitive::<Float64Type>();
                visit_rows(
                    array,
                    is_scalar,
                    group_ids,
                    num_rows,
                    |row_idx| typed.value(row_idx),
                    |group, val| {
                        sums[group] += val;
                        counts[group] += 1;
                        Ok(())
                    },
                )?;
            }
            Extremum { dir, state } => state.update(*dir, array, is_scalar, group_ids, num_rows)?,
        }
        Ok(())
    }

    pub fn merge_from(&mut self, other: Accumulator, group_map: &[u32]) -> EngineResult<()> {
        use State::*;
        match (&mut self.0, other.0) {
            (CountAll(target), CountAll(source)) | (Count(target), Count(source)) => {
                for (old_group, count) in source.into_iter().enumerate() {
                    target[group_map[old_group] as usize] += count;
                }
            }
            (SumInt64(target), SumInt64(source)) => {
                for (old_group, val) in source.into_iter().enumerate() {
                    if let Some(val) = val {
                        add_opt_i64(&mut target[group_map[old_group] as usize], val)?;
                    }
                }
            }
            (SumFloat64(target), SumFloat64(source)) => {
                for (old_group, val) in source.into_iter().enumerate() {
                    if let Some(val) = val {
                        let slot = &mut target[group_map[old_group] as usize];
                        *slot = Some(slot.map_or(val, |current| current + val));
                    }
                }
            }
            (
                SumDecimal128 {
                    scale: target_scale,
                    values: target,
                },
                SumDecimal128 {
                    scale: source_scale,
                    values: source,
                },
            ) if *target_scale == source_scale => {
                for (old_group, val) in source.into_iter().enumerate() {
                    if let Some(val) = val {
                        add_opt_decimal(&mut target[group_map[old_group] as usize], val)?;
                    }
                }
            }
            (
                AvgScaled {
                    scale: target_scale,
                    sums: target_sums,
                    counts: target_counts,
                },
                AvgScaled {
                    scale: source_scale,
                    sums: source_sums,
                    counts: source_counts,
                },
            ) if *target_scale == source_scale => {
                for (old_group, (sum, count)) in source_sums.into_iter().zip(source_counts).enumerate()
                {
                    let target_group = group_map[old_group] as usize;
                    target_sums[target_group] = target_sums[target_group]
                        .checked_add(sum)
                        .ok_or_else(|| EngineError::Execution("AVG overflow".to_string()))?;
                    target_counts[target_group] += count;
                }
            }
            (
                AvgFloat64 {
                    sums: target_sums,
                    counts: target_counts,
                },
                AvgFloat64 {
                    sums: source_sums,
                    counts: source_counts,
                },
            ) => {
                for (old_group, (sum, count)) in source_sums.into_iter().zip(source_counts).enumerate()
                {
                    let target_group = group_map[old_group] as usize;
                    target_sums[target_group] += sum;
                    target_counts[target_group] += count;
                }
            }
            (
                Extremum {
                    dir: target_dir,
                    state: target_state,
                },
                Extremum {
                    dir: source_dir,
                    state: source_state,
                },
            ) if *target_dir == source_dir => {
                target_state.merge_from(*target_dir, source_state, group_map)?;
            }
            _ => {
                return Err(EngineError::Execution(
                    "cannot merge accumulators of different types".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn finalize(self) -> EngineResult<ArrayRef> {
        use State::*;
        let array: ArrayRef = match self.0 {
            CountAll(counts) | Count(counts) => Arc::new(Int64Array::from(counts)),
            SumInt64(values) => Arc::new(Int64Array::from(values)),
            SumFloat64(values) => Arc::new(Float64Array::from(values)),
            SumDecimal128 { scale, values } => Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale)?,
            ),
            AvgScaled {
                scale,
                sums,
                counts,
            } => {
                let factor = 10_f64.powi(i32::from(scale));
                Arc::new(Float64Array::from_iter(sums.into_iter().zip(counts).map(
                    |(sum, count)| (count > 0).then(|| sum as f64 / (count as f64 * factor)),
                )))
            }
            AvgFloat64 { sums, counts } => Arc::new(Float64Array::from_iter(
                sums.into_iter()
                    .zip(counts)
                    .map(|(sum, count)| (count > 0).then(|| sum / count as f64)),
            )),
            Extremum { state, .. } => return state.finalize(),
        };
        Ok(array)
    }
}

fn visit_rows<V>(
    array: &dyn Array,
    is_scalar: bool,
    group_ids: Option<&[u32]>,
    num_rows: usize,
    read: impl Fn(usize) -> V,
    mut apply: impl FnMut(usize, V) -> EngineResult<()>,
) -> EngineResult<()> {
    match (is_scalar, group_ids) {
        (false, Some(gids)) => {
            for (row_idx, &group) in gids.iter().enumerate() {
                if array.is_valid(row_idx) {
                    apply(group as usize, read(row_idx))?;
                }
            }
        }
        (false, None) => {
            for row_idx in 0..array.len() {
                if array.is_valid(row_idx) {
                    apply(0, read(row_idx))?;
                }
            }
        }
        (true, Some(gids)) => {
            if array.is_valid(0) {
                for &group in gids {
                    apply(group as usize, read(0))?;
                }
            }
        }
        (true, None) => {
            if array.is_valid(0) {
                for _ in 0..num_rows {
                    apply(0, read(0))?;
                }
            }
        }
    }
    Ok(())
}

fn add_opt_i64(slot: &mut Option<i64>, val: i64) -> EngineResult<()> {
    *slot = Some(match *slot {
        Some(current) => current
            .checked_add(val)
            .ok_or_else(|| EngineError::Execution("SUM(Int64) overflow".to_string()))?,
        None => val,
    });
    Ok(())
}

fn add_opt_decimal(slot: &mut Option<i128>, val: i128) -> EngineResult<()> {
    const LIMIT: u128 = 10_u128.pow(DECIMAL128_MAX_PRECISION as u32);
    let next = match *slot {
        Some(current) => current
            .checked_add(val)
            .ok_or_else(|| EngineError::Execution("SUM(Decimal128) overflow".to_string()))?,
        None => val,
    };
    if next.unsigned_abs() >= LIMIT {
        return Err(EngineError::Execution(
            "SUM(Decimal128) exceeds 38 digits of precision".to_string(),
        ));
    }
    *slot = Some(next);
    Ok(())
}

fn add_avg_i128(sum: &mut i128, count: &mut i64, val: i128) -> EngineResult<()> {
    *sum = sum
        .checked_add(val)
        .ok_or_else(|| EngineError::Execution("AVG overflow".to_string()))?;
    *count += 1;
    Ok(())
}

fn update_opt<T: Copy>(slot: &mut Option<T>, candidate: T, is_better: impl Fn(T, T) -> bool) {
    if slot.is_none_or(|current| is_better(candidate, current)) {
        *slot = Some(candidate);
    }
}

fn merge_opt<T: Copy>(
    target: &mut [Option<T>],
    source: Vec<Option<T>>,
    group_map: &[u32],
    is_better: impl Fn(T, T) -> bool,
) {
    for (old_group, val) in source.into_iter().enumerate() {
        if let Some(val) = val {
            update_opt(&mut target[group_map[old_group] as usize], val, &is_better);
        }
    }
}

fn update_prim_extremum<P: ArrowPrimitiveType>(
    values: &mut [Option<P::Native>],
    array: &dyn Array,
    is_scalar: bool,
    group_ids: Option<&[u32]>,
    num_rows: usize,
    dir: Direction,
    cmp: impl Fn(&P::Native, &P::Native) -> Ordering,
) -> EngineResult<()> {
    let typed = array.as_primitive::<P>();
    visit_rows(
        array,
        is_scalar,
        group_ids,
        num_rows,
        |row_idx| typed.value(row_idx),
        |group, val| {
            update_opt(&mut values[group], val, |candidate, current| {
                dir.is_better(cmp(&candidate, &current))
            });
            Ok(())
        },
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;
    use crate::expr::Expr;

    fn schema_of(data_type: DataType) -> Schema {
        Schema::new(vec![Field::new("x", data_type, true)])
    }

    fn call(func: AggFunc) -> AggExpr {
        AggExpr::Call {
            func,
            expr: Expr::Column {
                index: 0,
                name: "x".into(),
            },
        }
    }

    #[test]
    fn count_all_and_count_with_nulls_and_scalars() {
        let schema = schema_of(DataType::Int64);
        let mut count_all = Accumulator::new(&AggExpr::CountAll, &schema).unwrap();
        let mut count = Accumulator::new(&call(AggFunc::Count), &schema).unwrap();
        count_all.resize(3);
        count.resize(3);

        let arr = ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(10),
            None,
            None,
            Some(20),
        ])));
        let gids = [0, 0, 1, 2];
        count_all.update_batch(None, &gids).unwrap();
        count.update_batch(Some(&arr), &gids).unwrap();

        let non_null_scalar = ColumnarValue::Scalar(Arc::new(Int64Array::from(vec![Some(1)])));
        let null_scalar = ColumnarValue::Scalar(Arc::new(Int64Array::from(vec![None::<i64>])));
        count.update_batch(Some(&non_null_scalar), &[2]).unwrap();
        count.update_batch(Some(&null_scalar), &[1]).unwrap();

        assert_eq!(
            count_all
                .finalize()
                .unwrap()
                .as_primitive::<Int64Type>()
                .values(),
            &[2, 1, 1]
        );
        assert_eq!(
            count
                .finalize()
                .unwrap()
                .as_primitive::<Int64Type>()
                .values(),
            &[1, 0, 2]
        );
    }

    #[test]
    fn sum_int32_int64_float64_and_decimal128() {
        let mut sum_i32 =
            Accumulator::new(&call(AggFunc::Sum), &schema_of(DataType::Int32)).unwrap();
        sum_i32.resize(2);
        let arr = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(5), None, Some(7)])));
        sum_i32.update_batch(Some(&arr), &[0, 1, 0]).unwrap();
        let out = sum_i32.finalize().unwrap();
        let out = out.as_primitive::<Int64Type>();
        assert_eq!(out.value(0), 12);
        assert!(out.is_null(1));

        let mut sum_f64 =
            Accumulator::new(&call(AggFunc::Sum), &schema_of(DataType::Float64)).unwrap();
        sum_f64.resize(2);
        let arr = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(2.25),
            None,
        ])));
        sum_f64.update_batch(Some(&arr), &[0, 0, 1]).unwrap();
        let out = sum_f64.finalize().unwrap();
        let out = out.as_primitive::<Float64Type>();
        assert_eq!(out.value(0), 3.75);
        assert!(out.is_null(1));

        let mut sum_dec = Accumulator::new(
            &call(AggFunc::Sum),
            &schema_of(DataType::Decimal128(10, 2)),
        )
        .unwrap();
        sum_dec.resize(2);
        let arr = ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![Some(1050), Some(250), None])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ));
        sum_dec.update_batch(Some(&arr), &[0, 0, 1]).unwrap();
        let out = sum_dec.finalize().unwrap();
        assert_eq!(out.data_type(), &DataType::Decimal128(38, 2));
        let out = out.as_primitive::<Decimal128Type>();
        assert_eq!(out.value(0), 1300);
        assert!(out.is_null(1));
    }

    #[test]
    fn sum_and_avg_overflow_return_execution_error() {
        let mut sum_i64 =
            Accumulator::new(&call(AggFunc::Sum), &schema_of(DataType::Int64)).unwrap();
        sum_i64.resize(1);
        let arr = ColumnarValue::Array(Arc::new(Int64Array::from(vec![i64::MAX, 1])));
        assert!(matches!(
            sum_i64.update_single(Some(&arr), 2),
            Err(EngineError::Execution(_))
        ));

        let max_38: i128 = 10_i128.pow(38) - 1;
        let mut sum_dec = Accumulator::new(
            &call(AggFunc::Sum),
            &schema_of(DataType::Decimal128(38, 0)),
        )
        .unwrap();
        sum_dec.resize(1);
        let arr = ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![max_38, 1])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ));
        assert!(matches!(
            sum_dec.update_single(Some(&arr), 2),
            Err(EngineError::Execution(_))
        ));

        let mut avg_dec = Accumulator::new(
            &call(AggFunc::Avg),
            &schema_of(DataType::Decimal128(38, 0)),
        )
        .unwrap();
        avg_dec.resize(1);
        let arr = ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![max_38, max_38])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ));
        assert!(matches!(
            avg_dec.update_single(Some(&arr), 2),
            Err(EngineError::Execution(_))
        ));
    }

    #[test]
    fn avg_int_float_and_decimal_precision() {
        let mut avg_i64 =
            Accumulator::new(&call(AggFunc::Avg), &schema_of(DataType::Int64)).unwrap();
        avg_i64.resize(2);
        let arr = ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            Some(i64::MAX),
            None,
        ])));
        avg_i64.update_batch(Some(&arr), &[0, 0, 1]).unwrap();
        let out = avg_i64.finalize().unwrap();
        let out = out.as_primitive::<Float64Type>();
        assert_eq!(out.value(0), i64::MAX as f64);
        assert!(out.is_null(1));

        let mut avg_dec = Accumulator::new(
            &call(AggFunc::Avg),
            &schema_of(DataType::Decimal128(10, 2)),
        )
        .unwrap();
        avg_dec.resize(1);
        let arr = ColumnarValue::Array(Arc::new(
            Decimal128Array::from(vec![100, 200, 300])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ));
        avg_dec.update_single(Some(&arr), 3).unwrap();
        let out = avg_dec.finalize().unwrap();
        assert_eq!(out.as_primitive::<Float64Type>().value(0), 2.0);
    }

    #[test]
    fn min_and_max_across_all_types_including_float_nan() {
        let mut min_f =
            Accumulator::new(&call(AggFunc::Min), &schema_of(DataType::Float64)).unwrap();
        let mut max_f =
            Accumulator::new(&call(AggFunc::Max), &schema_of(DataType::Float64)).unwrap();
        min_f.resize(1);
        max_f.resize(1);
        let arr = ColumnarValue::Array(Arc::new(Float64Array::from(vec![1.0, f64::NAN, -2.5])));
        min_f.update_single(Some(&arr), 3).unwrap();
        max_f.update_single(Some(&arr), 3).unwrap();
        assert_eq!(
            min_f
                .finalize()
                .unwrap()
                .as_primitive::<Float64Type>()
                .value(0),
            -2.5
        );
        assert!(
            max_f
                .finalize()
                .unwrap()
                .as_primitive::<Float64Type>()
                .value(0)
                .is_nan()
        );

        let mut min_s =
            Accumulator::new(&call(AggFunc::Min), &schema_of(DataType::Utf8View)).unwrap();
        let mut max_s =
            Accumulator::new(&call(AggFunc::Max), &schema_of(DataType::Utf8View)).unwrap();
        min_s.resize(2);
        max_s.resize(2);
        let arr = ColumnarValue::Array(Arc::new(StringViewArray::from(vec![
            Some("banana"),
            Some("apple"),
            None,
        ])));
        min_s.update_batch(Some(&arr), &[0, 0, 1]).unwrap();
        max_s.update_batch(Some(&arr), &[0, 0, 1]).unwrap();
        let out_min = min_s.finalize().unwrap();
        let out_max = max_s.finalize().unwrap();
        assert_eq!(out_min.as_string_view().value(0), "apple");
        assert!(out_min.is_null(1));
        assert_eq!(out_max.as_string_view().value(0), "banana");
        assert!(out_max.is_null(1));

        let mut min_b =
            Accumulator::new(&call(AggFunc::Min), &schema_of(DataType::Boolean)).unwrap();
        let mut max_b =
            Accumulator::new(&call(AggFunc::Max), &schema_of(DataType::Boolean)).unwrap();
        min_b.resize(1);
        max_b.resize(1);
        let arr = ColumnarValue::Array(Arc::new(BooleanArray::from(vec![true, false, true])));
        min_b.update_single(Some(&arr), 3).unwrap();
        max_b.update_single(Some(&arr), 3).unwrap();
        assert!(!min_b.finalize().unwrap().as_boolean().value(0));
        assert!(max_b.finalize().unwrap().as_boolean().value(0));

        let mut min_d =
            Accumulator::new(&call(AggFunc::Min), &schema_of(DataType::Date32)).unwrap();
        min_d.resize(1);
        let arr = ColumnarValue::Array(Arc::new(Date32Array::from(vec![200, 100, 300])));
        min_d.update_single(Some(&arr), 3).unwrap();
        assert_eq!(
            min_d
                .finalize()
                .unwrap()
                .as_primitive::<Date32Type>()
                .value(0),
            100
        );
    }

    #[test]
    fn update_single_matches_update_batch_with_scalar_and_array() {
        let schema = schema_of(DataType::Int64);
        let scalar = ColumnarValue::Scalar(Arc::new(Int64Array::from(vec![3])));
        let mut single = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        let mut batch = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        single.resize(1);
        batch.resize(1);

        single.update_single(Some(&scalar), 4).unwrap();
        batch.update_batch(Some(&scalar), &[0, 0, 0, 0]).unwrap();

        assert_eq!(
            single
                .finalize()
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0),
            batch
                .finalize()
                .unwrap()
                .as_primitive::<Int64Type>()
                .value(0)
        );
    }

    #[test]
    fn merge_from_combines_groups_checks_overflow_and_rejects_mismatched_scale() {
        let schema = schema_of(DataType::Int64);
        let mut target = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        let mut source = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        target.resize(3);
        source.resize(3);

        let target_arr = ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(10), None])));
        target.update_batch(Some(&target_arr), &[0, 1]).unwrap();

        let source_arr = ColumnarValue::Array(Arc::new(Int64Array::from(vec![
            Some(5),
            Some(20),
            None,
        ])));
        source
            .update_batch(Some(&source_arr), &[0, 1, 2])
            .unwrap();

        target.merge_from(source, &[0, 1, 2]).unwrap();
        let out = target.finalize().unwrap();
        let out = out.as_primitive::<Int64Type>();
        assert_eq!(out.value(0), 15);
        assert_eq!(out.value(1), 20);
        assert!(out.is_null(2));

        let mut left = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        let mut right = Accumulator::new(&call(AggFunc::Sum), &schema).unwrap();
        left.resize(1);
        right.resize(1);
        left.update_single(
            Some(&ColumnarValue::Scalar(Arc::new(Int64Array::from(vec![
                i64::MAX,
            ])))),
            1,
        )
        .unwrap();
        right
            .update_single(
                Some(&ColumnarValue::Scalar(Arc::new(Int64Array::from(vec![1])))),
                1,
            )
            .unwrap();
        assert!(matches!(
            left.merge_from(right, &[0]),
            Err(EngineError::Execution(_))
        ));

        let mut scale2 = Accumulator::new(
            &call(AggFunc::Sum),
            &schema_of(DataType::Decimal128(10, 2)),
        )
        .unwrap();
        let scale4 = Accumulator::new(
            &call(AggFunc::Sum),
            &schema_of(DataType::Decimal128(10, 4)),
        )
        .unwrap();
        assert!(matches!(
            scale2.merge_from(scale4, &[]),
            Err(EngineError::Execution(_))
        ));
    }
}
