//! End-to-end scan tests. Each test writes real Vortex files and reads them back through
//! `stream::scan`, so they cover predicates, NULL handling, pruning, and empty data together.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::error::StorageError;
use crate::runtime::StorageRuntime;
use crate::scan::{CmpOp, Predicate, Projection};
use crate::stream;
use crate::table::Snapshot;
use crate::test_util::{TestTable, int64_values, write_table};
use crate::value::Literal;

/// The `name` value for ids 0..10. `None` marks a NULL.
const NAMES: [Option<&str>; 10] = [
    Some("apple"),
    None,
    Some("banana"),
    Some("cherry"),
    None,
    Some("apple"),
    Some("date"),
    None,
    Some("fig"),
    Some("grape"),
];

fn people_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8View, true),
    ]))
}

fn people() -> TestTable {
    let ids = Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef;
    let names = Arc::new(StringViewArray::from(NAMES.to_vec())) as ArrayRef;
    write_table(people_schema(), vec![vec![ids, names]])
}

fn scan_batches(snap: &Snapshot, column: &str, predicate: &Predicate) -> stream::BatchStream {
    let rt = Arc::new(StorageRuntime::new(1, 2).unwrap());
    stream::scan(&rt, snap, &Projection::new([column]), predicate, None).unwrap()
}

/// Scans the Int64 `column` and returns the matching values in ascending order.
fn run(snap: &Snapshot, column: &str, predicate: Predicate) -> Vec<i64> {
    let batches: Vec<RecordBatch> = scan_batches(snap, column, &predicate)
        .map(|b| b.unwrap())
        .collect();
    let mut values = int64_values(&batches);
    values.sort();
    values
}

/// Scans the `id` column and returns how many files planning skipped, then the matching ids in
/// ascending order.
fn skipped_and_ids(snap: &Snapshot, predicate: Predicate) -> (u64, Vec<i64>) {
    let stream = scan_batches(snap, "id", &predicate);
    let skipped = stream.pruning_report().files_skipped;
    let batches: Vec<RecordBatch> = stream.map(|b| b.unwrap()).collect();
    let mut ids = int64_values(&batches);
    ids.sort();
    (skipped, ids)
}

fn id(op: CmpOp, value: i64) -> Predicate {
    Predicate::compare("id", op, value)
}

fn name(op: CmpOp, value: &str) -> Predicate {
    Predicate::compare("name", op, value)
}

#[test]
fn eq_matches_one_value() {
    let t = people();
    assert_eq!(run(&t.snapshot, "id", id(CmpOp::Eq, 3)), vec![3]);
}

#[test]
fn gt_lt_ge_le_use_the_right_bounds() {
    let t = people();
    assert_eq!(run(&t.snapshot, "id", id(CmpOp::Gt, 7)), vec![8, 9]);
    assert_eq!(run(&t.snapshot, "id", id(CmpOp::Lt, 2)), vec![0, 1]);
    assert_eq!(run(&t.snapshot, "id", id(CmpOp::Ge, 8)), vec![8, 9]);
    assert_eq!(run(&t.snapshot, "id", id(CmpOp::Le, 1)), vec![0, 1]);
}

#[test]
fn in_list_matches_any_listed_value() {
    let t = people();
    let p = Predicate::In {
        column: "id".into(),
        values: vec![2i64.into(), 5i64.into(), 9i64.into()],
    };
    assert_eq!(run(&t.snapshot, "id", p), vec![2, 5, 9]);
}

#[test]
fn string_equality_matches_only_equal_rows() {
    let t = people();
    assert_eq!(run(&t.snapshot, "id", name(CmpOp::Eq, "apple")), vec![0, 5]);
}

#[test]
fn string_range_drops_null_rows() {
    // Every non-NULL name is greater than "", so only the NULL rows (1, 4, 7) are dropped.
    let t = people();
    assert_eq!(
        run(&t.snapshot, "id", name(CmpOp::Gt, "")),
        vec![0, 2, 3, 5, 6, 8, 9]
    );
}

#[test]
fn is_null_matches_only_null_rows() {
    let t = people();
    let p = Predicate::IsNull {
        column: "name".into(),
    };
    assert_eq!(run(&t.snapshot, "id", p), vec![1, 4, 7]);
}

#[test]
fn and_requires_both_parts() {
    let t = people();
    let p = Predicate::And(vec![id(CmpOp::Gt, 2), name(CmpOp::Eq, "apple")]);
    assert_eq!(run(&t.snapshot, "id", p), vec![5]);
}

#[test]
fn or_accepts_either_part() {
    let t = people();
    let p = Predicate::Or(vec![id(CmpOp::Eq, 0), id(CmpOp::Eq, 9)]);
    assert_eq!(run(&t.snapshot, "id", p), vec![0, 9]);
}

#[test]
fn not_inverts_a_comparison() {
    let t = people();
    let p = Predicate::Not(Box::new(id(CmpOp::Gt, 7)));
    assert_eq!(run(&t.snapshot, "id", p), vec![0, 1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn not_drops_null_rows_too() {
    // NOT(name = "apple") is NULL for the NULL rows, so they are dropped like any NULL test.
    let t = people();
    let p = Predicate::Not(Box::new(name(CmpOp::Eq, "apple")));
    assert_eq!(run(&t.snapshot, "id", p), vec![2, 3, 6, 8, 9]);
}

#[test]
fn true_returns_every_row() {
    let t = people();
    assert_eq!(
        run(&t.snapshot, "id", Predicate::True),
        (0..10).collect::<Vec<i64>>()
    );
}

#[test]
fn all_null_column_matches_is_null_and_nothing_else() {
    let ids = Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef;
    let names = Arc::new(StringViewArray::from(vec![None::<&str>; 10])) as ArrayRef;
    let t = write_table(people_schema(), vec![vec![ids, names]]);

    let is_null = Predicate::IsNull {
        column: "name".into(),
    };
    assert_eq!(
        run(&t.snapshot, "id", is_null),
        (0..10).collect::<Vec<i64>>()
    );
    assert!(run(&t.snapshot, "id", name(CmpOp::Eq, "apple")).is_empty());
}

#[test]
fn empty_table_returns_no_rows() {
    let ids = Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef;
    let names = Arc::new(StringViewArray::from(Vec::<Option<&str>>::new())) as ArrayRef;
    let t = write_table(people_schema(), vec![vec![ids, names]]);
    assert!(run(&t.snapshot, "id", Predicate::True).is_empty());
}

#[test]
fn int32_column_accepts_int64_literal() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let x = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef;
    let t = write_table(schema, vec![vec![x]]);
    let p = Predicate::compare("x", CmpOp::Gt, 3i64);

    let mut values: Vec<i32> = scan_batches(&t.snapshot, "x", &p)
        .map(|b| b.unwrap())
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    values.sort();
    assert_eq!(values, vec![4, 5]);
}

#[test]
fn numeric_string_is_parsed_for_int_column() {
    let t = people();
    let p = Predicate::compare("id", CmpOp::Eq, " 5 ");
    assert_eq!(run(&t.snapshot, "id", p), vec![5]);
}

#[test]
fn out_of_range_literal_is_plan_error() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let x = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
    let t = write_table(schema, vec![vec![x]]);

    let rt = Arc::new(StorageRuntime::new(1, 1).unwrap());
    let p = Predicate::compare("x", CmpOp::Gt, 5_000_000_000i64);
    match stream::scan(&rt, &t.snapshot, &Projection::new(["x"]), &p, None) {
        Err(StorageError::Plan(_)) => {}
        Err(other) => panic!("expected a plan error, got {other}"),
        Ok(_) => panic!("expected a plan error, scan started"),
    }
}

#[test]
fn pruning_skips_files_outside_the_predicate() {
    // File 1 holds ids 0..10 and file 2 holds 100..110. `id > 50` can only match file 2.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids = |range: std::ops::Range<i64>| {
        vec![Arc::new(Int64Array::from_iter_values(range)) as ArrayRef]
    };
    let t = write_table(schema, vec![ids(0..10), ids(100..110)]);

    let stream = scan_batches(&t.snapshot, "id", &id(CmpOp::Gt, 50));
    assert_eq!(stream.pruning_report().files_skipped, 1);
    assert_eq!(stream.pruning_report().files_total, 2);

    let batches: Vec<RecordBatch> = stream.map(|b| b.unwrap()).collect();
    let mut values = int64_values(&batches);
    values.sort();
    assert_eq!(values, (100..110).collect::<Vec<i64>>());
}

/// Two files of `(id, day)`: ids 0..4 with days 100..103, and ids 10..14 with days 200..203.
/// Day 201 (id 11) is NULL.
fn dated() -> TestTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Date32, true),
    ]));
    let file = |ids: Vec<i64>, days: Vec<Option<i32>>| {
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(Date32Array::from(days)) as ArrayRef,
        ]
    };
    write_table(
        schema,
        vec![
            file(
                vec![0, 1, 2, 3],
                vec![Some(100), Some(101), Some(102), Some(103)],
            ),
            file(
                vec![10, 11, 12, 13],
                vec![Some(200), None, Some(202), Some(203)],
            ),
        ],
    )
}

/// Two files of `(id, price)` with `price: Decimal128(15, 2)`: ids 0..4 priced 0.01..0.04,
/// and ids 10..14 priced 0.05, NULL, 0.07, 0.50.
fn priced() -> TestTable {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("price", DataType::Decimal128(15, 2), true),
    ]));
    let file = |ids: Vec<i64>, cents: Vec<Option<i128>>| {
        let prices = Decimal128Array::from(cents)
            .with_precision_and_scale(15, 2)
            .unwrap();
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(prices) as ArrayRef,
        ]
    };
    write_table(
        schema,
        vec![
            file(vec![0, 1, 2, 3], vec![Some(1), Some(2), Some(3), Some(4)]),
            file(vec![10, 11, 12, 13], vec![Some(5), None, Some(7), Some(50)]),
        ],
    )
}

fn cents(value: i128) -> Literal {
    Literal::Decimal128 {
        value,
        precision: 15,
        scale: 2,
    }
}

#[test]
fn date_range_filters_rows_and_skips_file() {
    // `day >= 202` drops the NULL day, and no day in file 1 can match.
    let t = dated();
    let p = Predicate::compare("day", CmpOp::Ge, Literal::Date32(202));
    assert_eq!(skipped_and_ids(&t.snapshot, p), (1, vec![12, 13]));
}

#[test]
fn date_equality_matches_one_row() {
    let t = dated();
    let p = Predicate::compare("day", CmpOp::Eq, Literal::Date32(102));
    assert_eq!(skipped_and_ids(&t.snapshot, p), (1, vec![2]));
}

#[test]
fn decimal_range_filters_rows_and_skips_file() {
    // Q6's discount filter: `price BETWEEN 0.05 AND 0.07`. The NULL price is dropped.
    let t = priced();
    let p = Predicate::And(vec![
        Predicate::compare("price", CmpOp::Ge, cents(5)),
        Predicate::compare("price", CmpOp::Le, cents(7)),
    ]);
    assert_eq!(skipped_and_ids(&t.snapshot, p), (1, vec![10, 12]));
}

#[test]
fn decimal_literal_with_smaller_scale_is_rescaled() {
    // 0.5 at scale 1 must match 0.50 at the column's scale 2. Without rescaling, the raw
    // value 5 would match 0.05 (id 10) instead.
    let t = priced();
    let half = Literal::Decimal128 {
        value: 5,
        precision: 2,
        scale: 1,
    };
    let p = Predicate::compare("price", CmpOp::Eq, half);
    assert_eq!(skipped_and_ids(&t.snapshot, p), (1, vec![13]));
}
