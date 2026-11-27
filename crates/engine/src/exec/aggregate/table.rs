use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::row::{Row, RowConverter, Rows, SortField};
use rapidhash::v3::rapidhash_v3;

use crate::error::EngineResult;

const EMPTY: u32 = u32::MAX;
const INITIAL_CAPACITY: usize = 16;

#[derive(Clone, Copy)]
struct Entry {
    hash: u64,
    group_id: u32,
}

pub struct GroupTable {
    converter: Arc<RowConverter>,
    keys: Rows,
    slots: Vec<Entry>,
}

impl GroupTable {
    pub fn new(key_types: &[DataType]) -> EngineResult<Self> {
        let converter = Arc::new(RowConverter::new(
            key_types.iter().cloned().map(SortField::new).collect(),
        )?);
        Ok(Self::with_converter(converter))
    }

    pub fn with_converter(converter: Arc<RowConverter>) -> Self {
        let keys = converter.empty_rows(0, 0);
        Self {
            converter,
            keys,
            slots: vec![
                Entry {
                    hash: 0,
                    group_id: EMPTY,
                };
                INITIAL_CAPACITY
            ],
        }
    }

    pub fn converter(&self) -> &Arc<RowConverter> {
        &self.converter
    }

    pub fn len(&self) -> usize {
        self.keys.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn group_ids(&mut self, key_arrays: &[ArrayRef], out: &mut Vec<u32>) -> EngineResult<()> {
        let rows = self.converter.convert_columns(key_arrays)?;
        out.clear();
        out.reserve(rows.num_rows());
        for row in rows.iter() {
            out.push(self.insert_or_get(row));
        }
        Ok(())
    }

    pub fn merge_from(&mut self, other: GroupTable) -> Vec<u32> {
        assert!(
            Arc::ptr_eq(&self.converter, &other.converter),
            "cannot merge GroupTables with different RowConverters"
        );
        if self.is_empty() {
            let n = other.len() as u32;
            *self = other;
            return (0..n).collect();
        }
        let mut map = Vec::with_capacity(other.len());
        for row in other.keys.iter() {
            map.push(self.insert_or_get(row));
        }
        map
    }

    pub fn into_arrays(self) -> EngineResult<Vec<ArrayRef>> {
        Ok(self.converter.convert_rows(self.keys.iter())?)
    }

    fn insert_or_get(&mut self, row: Row<'_>) -> u32 {
        let bytes = row.as_ref();
        let hash = rapidhash_v3(bytes);
        let mask = self.slots.len() - 1;
        let mut index = (hash as usize) & mask;
        let mut psl = 0;

        loop {
            let entry = self.slots[index];
            if entry.group_id == EMPTY {
                break;
            }
            if entry.hash == hash && self.keys.row(entry.group_id as usize).as_ref() == bytes {
                return entry.group_id;
            }
            if psl > probe_distance(index, entry.hash, mask) {
                break;
            }
            index = (index + 1) & mask;
            psl += 1;
        }

        if (self.len() + 1) * 4 > self.slots.len() * 3 {
            self.grow();
            let mask = self.slots.len() - 1;
            index = (hash as usize) & mask;
            psl = 0;
        }

        let group_id = u32::try_from(self.len())
            .ok()
            .filter(|&id| id < EMPTY)
            .expect("group table capacity exceeded");
        self.keys.push(row);
        place_entry(&mut self.slots, Entry { hash, group_id }, index, psl);
        group_id
    }

    fn grow(&mut self) {
        let new_capacity = self.slots.len() * 2;
        let mask = new_capacity - 1;
        let mut new_slots = vec![
            Entry {
                hash: 0,
                group_id: EMPTY,
            };
            new_capacity
        ];
        for &entry in &self.slots {
            if entry.group_id != EMPTY {
                place_entry(&mut new_slots, entry, (entry.hash as usize) & mask, 0);
            }
        }
        self.slots = new_slots;
    }
}

fn probe_distance(index: usize, hash: u64, mask: usize) -> usize {
    index.wrapping_sub(hash as usize) & mask
}

fn place_entry(slots: &mut [Entry], mut current: Entry, mut index: usize, mut psl: usize) {
    let mask = slots.len() - 1;
    loop {
        let slot = &mut slots[index];
        if slot.group_id == EMPTY {
            *slot = current;
            return;
        }
        let resident_psl = probe_distance(index, slot.hash, mask);
        if psl > resident_psl {
            std::mem::swap(slot, &mut current);
            psl = resident_psl;
        }
        index = (index + 1) & mask;
        psl += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, AsArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        Int64Array, StringViewArray,
    };
    use arrow::datatypes::{Date32Type, Decimal128Type, Float64Type, Int32Type, Int64Type};

    use super::*;

    #[test]
    fn single_column_keys_assign_dense_ids_and_reuse_existing() {
        let mut table = GroupTable::new(&[DataType::Int64]).unwrap();
        assert!(table.is_empty());

        let batch1: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 10, 30, 20]));
        let mut ids = Vec::new();
        table.group_ids(&[batch1], &mut ids).unwrap();
        assert_eq!(ids, vec![0, 1, 0, 2, 1]);
        assert_eq!(table.len(), 3);

        let batch2: ArrayRef = Arc::new(Int64Array::from(vec![30, 40, 10]));
        table.group_ids(&[batch2], &mut ids).unwrap();
        assert_eq!(ids, vec![2, 3, 0]);
        assert_eq!(table.len(), 4);

        let arrays = table.into_arrays().unwrap();
        assert_eq!(arrays.len(), 1);
        assert_eq!(
            arrays[0].as_primitive::<Int64Type>().values(),
            &[10, 20, 30, 40]
        );
    }

    #[test]
    fn multi_column_keys_group_by_tuple() {
        let mut table = GroupTable::new(&[DataType::Utf8View, DataType::Int32]).unwrap();
        let col_a: ArrayRef = Arc::new(StringViewArray::from(vec!["eng", "eng", "sales", "eng"]));
        let col_b: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 1, 1]));

        let mut ids = Vec::new();
        table.group_ids(&[col_a, col_b], &mut ids).unwrap();
        assert_eq!(ids, vec![0, 1, 2, 0]);
        assert_eq!(table.len(), 3);

        let arrays = table.into_arrays().unwrap();
        let names: Vec<&str> = arrays[0]
            .as_string_view()
            .iter()
            .map(Option::unwrap)
            .collect();
        let nums = arrays[1].as_primitive::<Int32Type>().values();
        assert_eq!(names, vec!["eng", "eng", "sales"]);
        assert_eq!(nums, &[1, 2, 1]);
    }

    #[test]
    fn null_keys_group_together() {
        let mut table = GroupTable::new(&[DataType::Int64, DataType::Utf8View]).unwrap();
        let col_a: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1), None, Some(1), None]));
        let col_b: ArrayRef = Arc::new(StringViewArray::from(vec![
            None,
            None,
            None,
            None,
            Some("x"),
        ]));

        let mut ids = Vec::new();
        table.group_ids(&[col_a, col_b], &mut ids).unwrap();
        assert_eq!(ids, vec![0, 1, 0, 1, 2]);
        assert_eq!(table.len(), 3);

        let arrays = table.into_arrays().unwrap();
        assert!(arrays[0].is_null(0) && arrays[1].is_null(0));
        assert_eq!(arrays[0].as_primitive::<Int64Type>().value(1), 1);
        assert!(arrays[1].is_null(1));
        assert!(arrays[0].is_null(2));
        assert_eq!(arrays[1].as_string_view().value(2), "x");
    }

    #[test]
    fn supports_all_key_types_including_float_nan_and_signed_zero() {
        let types = vec![
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Decimal128(10, 2),
            DataType::Date32,
            DataType::Utf8View,
            DataType::Boolean,
        ];
        let mut table = GroupTable::new(&types).unwrap();

        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![10, 10, 10, 20])),
            Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN, -0.0, 0.0])),
            Arc::new(
                Decimal128Array::from(vec![1234, 1234, 1234, 5678])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![100, 100, 100, 200])),
            Arc::new(StringViewArray::from(vec!["a", "a", "a", "b"])),
            Arc::new(BooleanArray::from(vec![true, true, true, false])),
        ];

        let mut ids = Vec::new();
        table.group_ids(&cols, &mut ids).unwrap();
        // Two NaN rows must collapse into the same group.
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        assert_ne!(ids[2], ids[3]);
        assert_eq!(table.len(), 3);

        let arrays = table.into_arrays().unwrap();
        assert_eq!(arrays[0].as_primitive::<Int32Type>().values(), &[1, 1, 2]);
        assert_eq!(
            arrays[1].as_primitive::<Int64Type>().values(),
            &[10, 10, 20]
        );
        let floats = arrays[2].as_primitive::<Float64Type>();
        assert!(floats.value(0).is_nan());
        assert_eq!(floats.value(1).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(floats.value(2).to_bits(), (0.0_f64).to_bits());
        assert_eq!(
            arrays[3].as_primitive::<Decimal128Type>().values(),
            &[1234, 1234, 5678]
        );
        assert_eq!(
            arrays[4].as_primitive::<Date32Type>().values(),
            &[100, 100, 200]
        );
        assert_eq!(arrays[5].as_string_view().value(2), "b");
        assert!(!arrays[6].as_boolean().value(2));
    }

    #[test]
    fn grows_across_multiple_thresholds_and_preserves_ids() {
        let mut table = GroupTable::new(&[DataType::Int64]).unwrap();
        let values: Vec<i64> = (0..500).collect();
        let batch: ArrayRef = Arc::new(Int64Array::from(values.clone()));

        let mut first_ids = Vec::new();
        table
            .group_ids(std::slice::from_ref(&batch), &mut first_ids)
            .unwrap();
        assert_eq!(table.len(), 500);
        let expected: Vec<u32> = (0..500).collect();
        assert_eq!(first_ids, expected);

        // Re-probing all 500 keys in reverse order must return the exact same IDs.
        let reversed: Vec<i64> = (0..500).rev().collect();
        let rev_batch: ArrayRef = Arc::new(Int64Array::from(reversed));
        let mut rev_ids = Vec::new();
        table.group_ids(&[rev_batch], &mut rev_ids).unwrap();
        assert_eq!(table.len(), 500);
        let expected_rev: Vec<u32> = (0..500).rev().collect();
        assert_eq!(rev_ids, expected_rev);

        let arrays = table.into_arrays().unwrap();
        assert_eq!(
            arrays[0].as_primitive::<Int64Type>().values(),
            values.as_slice()
        );
    }

    #[test]
    fn merge_from_handles_overlapping_and_disjoint_keys() {
        let mut left = GroupTable::new(&[DataType::Utf8View]).unwrap();
        let mut right = GroupTable::with_converter(Arc::clone(left.converter()));
        let mut ids = Vec::new();

        let left_keys: ArrayRef = Arc::new(StringViewArray::from(vec!["a", "b", "c"]));
        left.group_ids(&[left_keys], &mut ids).unwrap();

        let right_keys: ArrayRef = Arc::new(StringViewArray::from(vec!["b", "d", "a", "e"]));
        right.group_ids(&[right_keys], &mut ids).unwrap();

        let map = left.merge_from(right);
        // right had: 0->"b", 1->"d", 2->"a", 3->"e"
        // left had:  0->"a", 1->"b", 2->"c"; after merge: 3->"d", 4->"e"
        assert_eq!(map, vec![1, 3, 0, 4]);
        assert_eq!(left.len(), 5);

        let arrays = left.into_arrays().unwrap();
        let keys: Vec<&str> = arrays[0]
            .as_string_view()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(keys, vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn merge_from_into_empty_table() {
        let mut dst = GroupTable::new(&[DataType::Int32]).unwrap();
        let mut src = GroupTable::with_converter(Arc::clone(dst.converter()));
        let mut ids = Vec::new();

        let src_keys: ArrayRef = Arc::new(Int32Array::from(vec![7, 8, 7, 9]));
        src.group_ids(&[src_keys], &mut ids).unwrap();

        let map = dst.merge_from(src);
        assert_eq!(map, vec![0, 1, 2]);
        assert_eq!(dst.len(), 3);

        let arrays = dst.into_arrays().unwrap();
        assert_eq!(arrays[0].as_primitive::<Int32Type>().values(), &[7, 8, 9]);
    }

    #[test]
    fn merge_from_empty_source_is_noop() {
        let mut dst = GroupTable::new(&[DataType::Int32]).unwrap();
        let src = GroupTable::with_converter(Arc::clone(dst.converter()));
        let mut ids = Vec::new();

        let dst_keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        dst.group_ids(&[dst_keys], &mut ids).unwrap();

        let map = dst.merge_from(src);
        assert!(map.is_empty());
        assert_eq!(dst.len(), 2);

        let arrays = dst.into_arrays().unwrap();
        assert_eq!(arrays[0].as_primitive::<Int32Type>().values(), &[1, 2]);
    }

    #[test]
    fn empty_batch_and_empty_table_into_arrays() {
        let mut table = GroupTable::new(&[DataType::Int64, DataType::Utf8View]).unwrap();
        let mut ids = vec![99];

        let empty_a: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
        let empty_b: ArrayRef = Arc::new(StringViewArray::from(Vec::<&str>::new()));
        table.group_ids(&[empty_a, empty_b], &mut ids).unwrap();
        assert!(ids.is_empty());
        assert!(table.is_empty());

        let arrays = table.into_arrays().unwrap();
        assert_eq!(arrays.len(), 2);
        assert_eq!(arrays[0].len(), 0);
        assert_eq!(arrays[1].len(), 0);
    }

    #[test]
    fn probing_existing_keys_at_load_threshold_does_not_grow() {
        let mut table = GroupTable::new(&[DataType::Int64]).unwrap();
        // 12 keys in 16 slots = exact 0.75 load factor.
        let batch: ArrayRef = Arc::new(Int64Array::from((0..12_i64).collect::<Vec<_>>()));
        let mut ids = Vec::new();
        table
            .group_ids(std::slice::from_ref(&batch), &mut ids)
            .unwrap();
        assert_eq!(table.len(), 12);
        assert_eq!(table.slots.len(), INITIAL_CAPACITY);

        // Re-probing existing keys must not trigger a resize.
        table.group_ids(&[batch], &mut ids).unwrap();
        assert_eq!(table.len(), 12);
        assert_eq!(table.slots.len(), INITIAL_CAPACITY);
    }

    #[test]
    fn robin_hood_invariant_holds_under_high_collision() {
        let mut table = GroupTable::new(&[DataType::Int64]).unwrap();
        let n = 2_000_i64;
        let values: Vec<i64> = (0..n).collect();
        let batch: ArrayRef = Arc::new(Int64Array::from(values));

        let mut ids = Vec::new();
        table
            .group_ids(std::slice::from_ref(&batch), &mut ids)
            .unwrap();
        assert_eq!(table.len(), n as usize);

        // In a Robin Hood table with no deletions, every occupied slot with probe distance
        // `psl > 0` must follow an occupied slot whose own probe distance is at least `psl - 1`.
        let mask = table.slots.len() - 1;
        for (idx, entry) in table.slots.iter().enumerate() {
            if entry.group_id == EMPTY {
                continue;
            }
            let psl = probe_distance(idx, entry.hash, mask);
            if psl > 0 {
                let prev_idx = idx.wrapping_sub(1) & mask;
                let prev = table.slots[prev_idx];
                assert_ne!(prev.group_id, EMPTY, "gap before displaced slot {idx}");
                let prev_psl = probe_distance(prev_idx, prev.hash, mask);
                assert!(
                    prev_psl >= psl - 1,
                    "Robin Hood invariant violated at slot {idx}: prev_psl={prev_psl}, psl={psl}"
                );
            }
        }

        // Re-probing all keys must return the exact same group IDs.
        let mut recheck = Vec::new();
        table.group_ids(&[batch], &mut recheck).unwrap();
        assert_eq!(recheck, ids);
    }
}
