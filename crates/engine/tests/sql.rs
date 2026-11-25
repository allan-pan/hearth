use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Int32Array, Int64Array, StringViewArray,
};
use arrow::datatypes::{DataType, Date32Type, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use chrono::NaiveDate;
use engine::catalog::MemoryCatalog;
use engine::error::EngineError;
use engine::{Engine, QueryResult};
use storage::runtime::StorageRuntime;
use storage::table::Snapshot;
use storage::write::write_tablefile;

struct TestDb {
    engine: Engine,
    _dir: tempfile::TempDir,
}

fn empty_engine() -> Engine {
    let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
    Engine::new(runtime, Arc::new(MemoryCatalog::new()))
}

fn date(y: i32, m: u32, d: u32) -> i32 {
    Date32Type::from_naive_date(NaiveDate::from_ymd_opt(y, m, d).unwrap())
}

/// Builds a 2-file `orders` table so batches arrive from multiple splits.
fn orders_db() -> TestDb {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(StorageRuntime::new(1, 2).unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("qty", DataType::Int32, false),
        Field::new("price", DataType::Decimal128(10, 2), false),
        Field::new("discount", DataType::Decimal128(4, 2), true),
        Field::new("shipdate", DataType::Date32, true),
        Field::new("status", DataType::Utf8View, true),
    ]));

    let file1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![10, 20, i32::MAX])),
            Arc::new(
                Decimal128Array::from(vec![1000, 2500, 500])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(5), None, Some(10)])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![
                Some(date(1994, 3, 15)),
                Some(date(1995, 6, 1)),
                None,
            ])),
            Arc::new(StringViewArray::from(vec![
                Some("open"),
                None,
                Some("hold"),
            ])),
        ],
    )
    .unwrap();

    let file2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])) as ArrayRef,
            Arc::new(Int32Array::from(vec![30, 40])),
            Arc::new(
                Decimal128Array::from(vec![4000, 1500])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(vec![Some(20), Some(0)])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![
                Some(date(1994, 12, 31)),
                Some(date(1993, 12, 31)),
            ])),
            Arc::new(StringViewArray::from(vec![Some("closed"), Some("open")])),
        ],
    )
    .unwrap();

    let files = vec![
        write_tablefile(&runtime, dir.path(), 1, file1).unwrap(),
        write_tablefile(&runtime, dir.path(), 2, file2).unwrap(),
    ];
    let mut catalog = MemoryCatalog::new();
    catalog.register(
        "orders",
        Snapshot {
            version: 0,
            schema,
            files,
        },
    );
    TestDb {
        engine: Engine::new(runtime, Arc::new(catalog)),
        _dir: dir,
    }
}

fn sorted_rows(result: &QueryResult) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        assert_eq!(batch.schema(), result.schema);
        for r in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|col| {
                    if col.is_null(r) {
                        "NULL".to_string()
                    } else {
                        array_value_to_string(col, r).unwrap()
                    }
                })
                .collect();
            rows.push(cells.join(", "));
        }
    }
    rows.sort();
    rows
}

fn row_count(result: &QueryResult) -> usize {
    result.batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn select_without_from_is_not_supported() {
    let engine = empty_engine();
    assert!(matches!(
        engine.sql("SELECT 1"),
        Err(EngineError::NotSupported(_))
    ));
}

#[test]
fn invalid_sql_is_a_parse_error() {
    let engine = empty_engine();
    assert!(matches!(engine.sql("selec 1"), Err(EngineError::Parse(_))));
}

#[test]
fn aggregate_and_sort_are_not_supported_yet() {
    let db = orders_db();
    assert!(matches!(
        db.engine.sql("SELECT COUNT(*) FROM orders"),
        Err(EngineError::NotSupported(_))
    ));
    assert!(matches!(
        db.engine.sql("SELECT id FROM orders ORDER BY id"),
        Err(EngineError::NotSupported(_))
    ));
}

#[test]
fn select_star_subsets_and_reordered_columns() {
    let db = orders_db();

    let all = db.engine.sql("SELECT * FROM orders").unwrap();
    assert_eq!(all.schema.fields().len(), 6);
    assert_eq!(row_count(&all), 5);

    let reordered = db
        .engine
        .sql("SELECT status AS st, id FROM orders WHERE id <= 2")
        .unwrap();
    assert_eq!(reordered.schema.field(0).name(), "st");
    assert_eq!(reordered.schema.field(1).name(), "id");
    assert_eq!(sorted_rows(&reordered), vec!["NULL, 2", "open, 1"]);
}

#[test]
fn decimal_arithmetic_and_int32_cast() {
    let db = orders_db();
    let res = db
        .engine
        .sql("SELECT id, price * (1 - discount) AS net, qty + id AS total FROM orders WHERE id IN (1, 4)")
        .unwrap();
    assert_eq!(sorted_rows(&res), vec!["1, 9.5000, 11", "4, 32.0000, 34"]);
}

#[test]
fn date_between_in_is_null_and_or_with_nulls() {
    let db = orders_db();

    let dates = db
        .engine
        .sql("SELECT id FROM orders WHERE shipdate >= date '1994-01-01' AND shipdate < date '1994-01-01' + interval '1' year")
        .unwrap();
    assert_eq!(sorted_rows(&dates), vec!["1", "4"]);

    let between = db
        .engine
        .sql("SELECT id FROM orders WHERE qty BETWEEN 10 AND 30")
        .unwrap();
    assert_eq!(sorted_rows(&between), vec!["1", "2", "4"]);

    let in_list = db
        .engine
        .sql("SELECT id FROM orders WHERE status IN ('open', 'closed')")
        .unwrap();
    assert_eq!(sorted_rows(&in_list), vec!["1", "4", "5"]);

    let is_null = db
        .engine
        .sql("SELECT id FROM orders WHERE status IS NULL")
        .unwrap();
    assert_eq!(sorted_rows(&is_null), vec!["2"]);

    // Row 2 has status = NULL, so `status = 'open' OR id = 2` is `NULL OR true` = true.
    let or_null = db
        .engine
        .sql("SELECT id FROM orders WHERE status = 'open' OR id = 2")
        .unwrap();
    assert_eq!(sorted_rows(&or_null), vec!["1", "2", "5"]);
}

#[test]
fn limit_and_offset_across_multiple_files() {
    let db = orders_db();

    let lim = db.engine.sql("SELECT id FROM orders LIMIT 3").unwrap();
    assert_eq!(row_count(&lim), 3);

    let off_lim = db
        .engine
        .sql("SELECT id FROM orders LIMIT 2 OFFSET 2")
        .unwrap();
    assert_eq!(row_count(&off_lim), 2);

    let off_only = db.engine.sql("SELECT id FROM orders OFFSET 3").unwrap();
    assert_eq!(row_count(&off_only), 2);
}

#[test]
fn limit_zero_and_empty_filter_keep_schema() {
    let db = orders_db();

    let lim0 = db
        .engine
        .sql("SELECT id, status AS st FROM orders LIMIT 0")
        .unwrap();
    assert_eq!(row_count(&lim0), 0);
    assert_eq!(lim0.schema.field(0).name(), "id");
    assert_eq!(lim0.schema.field(1).name(), "st");

    let none = db
        .engine
        .sql("SELECT id, status FROM orders WHERE id < 0")
        .unwrap();
    assert_eq!(row_count(&none), 0);
    assert_eq!(none.schema.fields().len(), 2);
}

#[test]
fn arithmetic_overflow_returns_execution_error() {
    let db = orders_db();
    // Row 3 has qty = i32::MAX; adding 1 overflows Int32.
    let res = db.engine.sql("SELECT qty + 1 FROM orders WHERE id = 3");
    assert!(matches!(res, Err(EngineError::Execution(_))));
}
