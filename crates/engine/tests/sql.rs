use std::collections::HashSet;
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

impl TestDb {
    fn from_batches(table: &str, batches: Vec<RecordBatch>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let runtime = Arc::new(StorageRuntime::new(1, batches.len().max(1)).unwrap());
        let schema = batches[0].schema();
        let files = batches
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                write_tablefile(&runtime, dir.path(), index as u64 + 1, batch).unwrap()
            })
            .collect();
        let mut catalog = MemoryCatalog::new();
        catalog.register(
            table,
            Snapshot {
                version: 0,
                schema,
                files,
            },
        );
        Self {
            engine: Engine::new(runtime, Arc::new(catalog)),
            _dir: dir,
        }
    }
}

fn empty_engine() -> Engine {
    let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
    Engine::new(runtime, Arc::new(MemoryCatalog::new()))
}

fn date(y: i32, m: u32, d: u32) -> i32 {
    Date32Type::from_naive_date(NaiveDate::from_ymd_opt(y, m, d).unwrap())
}

fn dec128(values: Vec<i128>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn dec128_opt(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// Builds a 2-file `orders` table so batches arrive from multiple splits.
fn orders_db() -> TestDb {
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
            dec128(vec![1000, 2500, 500], 10, 2),
            dec128_opt(vec![Some(5), None, Some(10)], 4, 2),
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
        schema,
        vec![
            Arc::new(Int64Array::from(vec![4, 5])) as ArrayRef,
            Arc::new(Int32Array::from(vec![30, 40])),
            dec128(vec![4000, 1500], 10, 2),
            dec128_opt(vec![Some(20), Some(0)], 4, 2),
            Arc::new(Date32Array::from(vec![
                Some(date(1994, 12, 31)),
                Some(date(1993, 12, 31)),
            ])),
            Arc::new(StringViewArray::from(vec![Some("closed"), Some("open")])),
        ],
    )
    .unwrap();

    TestDb::from_batches("orders", vec![file1, file2])
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
fn sort_is_not_supported_yet() {
    let db = orders_db();
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

#[test]
fn ungrouped_aggregates_on_populated_empty_and_limit_zero() {
    let db = orders_db();

    // Rows 1..4: discount has 3 non-nulls; prices = 10 + 25 + 5 + 40 = 80.00 (avg 20); avg(id) = 2.5.
    let populated = db
        .engine
        .sql(
            "SELECT COUNT(*), COUNT(discount), SUM(price), MIN(price), MAX(price), \
             AVG(price), AVG(id) FROM orders WHERE id <= 4",
        )
        .unwrap();
    assert_eq!(
        sorted_rows(&populated),
        vec!["4, 3, 80.00, 5.00, 40.00, 20, 2.5"]
    );

    let empty = db
        .engine
        .sql(
            "SELECT COUNT(*), COUNT(discount), SUM(price), MIN(price), MAX(price), AVG(price) \
             FROM orders WHERE id < 0",
        )
        .unwrap();
    assert_eq!(sorted_rows(&empty), vec!["0, 0, NULL, NULL, NULL, NULL"]);

    let lim0 = db
        .engine
        .sql("SELECT COUNT(*) AS cnt FROM orders LIMIT 0")
        .unwrap();
    assert_eq!(row_count(&lim0), 0);
    assert_eq!(lim0.schema.field(0).name(), "cnt");
}

#[test]
fn grouped_aggregates_composite_keys_expressions_and_limit() {
    let db = orders_db();

    let grouped = db
        .engine
        .sql("SELECT status, COUNT(*), SUM(id) FROM orders GROUP BY status")
        .unwrap();
    assert_eq!(
        sorted_rows(&grouped),
        vec!["NULL, 1, 2", "closed, 1, 4", "hold, 1, 3", "open, 2, 6"]
    );

    let zero_aggs = db
        .engine
        .sql("SELECT status FROM orders GROUP BY status")
        .unwrap();
    assert_eq!(
        sorted_rows(&zero_aggs),
        vec!["NULL", "closed", "hold", "open"]
    );

    // Composite key merges ("open", true) across file 1 (row 1) and file 2 (row 5).
    let composite = db
        .engine
        .sql("SELECT status, price >= 10, COUNT(*) FROM orders GROUP BY status, price >= 10")
        .unwrap();
    assert_eq!(
        sorted_rows(&composite),
        vec![
            "NULL, true, 1",
            "closed, true, 1",
            "hold, false, 1",
            "open, true, 2",
        ]
    );

    let expr_key = db
        .engine
        .sql("SELECT id + 1 AS next_id, COUNT(*) FROM orders WHERE id <= 3 GROUP BY id + 1")
        .unwrap();
    assert_eq!(sorted_rows(&expr_key), vec!["2, 1", "3, 1", "4, 1"]);

    let limited = db
        .engine
        .sql("SELECT status, COUNT(*) FROM orders GROUP BY status LIMIT 2 OFFSET 1")
        .unwrap();
    let rows = sorted_rows(&limited);
    let valid: HashSet<&str> = ["NULL, 1", "closed, 1", "hold, 1", "open, 2"]
        .into_iter()
        .collect();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0], rows[1]);
    assert!(rows.iter().all(|row| valid.contains(row.as_str())));
}

#[test]
fn scalar_aggregate_args_and_post_aggregate_projections() {
    let db = orders_db();
    let res = db
        .engine
        .sql(
            "SELECT COUNT(1) AS c1, SUM(1) AS s1, SUM(id) + 1 AS plus_one, COUNT(*) * 2 AS doubled \
             FROM orders",
        )
        .unwrap();
    assert_eq!(sorted_rows(&res), vec!["5, 5, 16, 10"]);
}

#[test]
fn sum_overflow_on_int64_and_decimal128_returns_execution_error() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("big_int", DataType::Int64, false),
        Field::new("big_dec", DataType::Decimal128(38, 2), false),
    ]));
    let max_38: i128 = 10_i128.pow(38) - 1;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![i64::MAX, 1])) as ArrayRef,
            dec128(vec![max_38, 1], 38, 2),
        ],
    )
    .unwrap();
    let db = TestDb::from_batches("big", vec![batch]);

    assert!(matches!(
        db.engine.sql("SELECT SUM(big_int) FROM big"),
        Err(EngineError::Execution(_))
    ));
    assert!(matches!(
        db.engine.sql("SELECT SUM(big_dec) FROM big"),
        Err(EngineError::Execution(_))
    ));
}

#[test]
fn tpch_q6_returns_exact_decimal_revenue() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_discount", DataType::Decimal128(15, 2), false),
        Field::new("l_quantity", DataType::Decimal128(15, 2), false),
        Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
    ]));
    let batch_from_rows = |rows: &[(i32, i128, i128, i128)]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                dec128(rows.iter().map(|r| r.1).collect(), 15, 2),
                dec128(rows.iter().map(|r| r.2).collect(), 15, 2),
                dec128(rows.iter().map(|r| r.3).collect(), 15, 2),
            ],
        )
        .unwrap()
    };

    let file1 = batch_from_rows(&[
        (date(1994, 1, 1), 5, 2300, 10000), // matches: 100.00 * 0.05 = 5.0000
        (date(1993, 12, 31), 6, 1000, 50000), // excluded: before 1994
        (date(1994, 6, 15), 8, 1000, 50000), // excluded: discount > 0.07
    ]);
    let file2 = batch_from_rows(&[
        (date(1994, 12, 31), 7, 1250, 20050), // matches: 200.50 * 0.07 = 14.0350
        (date(1995, 1, 1), 6, 500, 50000),    // excluded: >= 1995-01-01
        (date(1994, 7, 4), 6, 2400, 50000),   // excluded: quantity == 24
    ]);
    let db = TestDb::from_batches("lineitem", vec![file1, file2]);

    let res = db
        .engine
        .sql(
            "SELECT SUM(l_extendedprice * l_discount) AS revenue \
             FROM lineitem \
             WHERE l_shipdate >= DATE '1994-01-01' \
               AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR \
               AND l_discount BETWEEN 0.05 AND 0.07 \
               AND l_quantity < 24",
        )
        .unwrap();
    assert_eq!(res.schema.field(0).name(), "revenue");
    assert_eq!(
        res.schema.field(0).data_type(),
        &DataType::Decimal128(38, 4)
    );
    assert_eq!(sorted_rows(&res), vec!["19.0350"]);
}
