//! TPC-H checks against DuckDB at scale factor 0.1. Expected values come from `expected.json`,
//! written by `gen_tpch.py`.
//!
//! These tests are ignored by default because they need the generated CSV files.
//! Run them with: `cargo test -p storage tpch -- --ignored`
//! The data directory defaults to `/tmp/hearth-tpch/data` and can be set with `HEARTH_TPCH_DIR`.
//! The timing test also reads `HEARTH_CPU` for the number of CPU threads.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, ArrayRef, Decimal128Array, StringViewArray};
use arrow::compute::{cast, concat_batches};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::runtime::StorageRuntime;
use crate::scan::{CmpOp, Predicate, Projection};
use crate::stream;
use crate::table::Snapshot;
use crate::test_util::{TestTable, write_table};

// Row counts from expected.json.
const ROW_COUNTS: [(&str, usize); 8] = [
    ("region", 5),
    ("nation", 25),
    ("supplier", 1_000),
    ("customer", 15_000),
    ("part", 20_000),
    ("partsupp", 80_000),
    ("orders", 150_000),
    ("lineitem", 600_572),
];

// Lineitem values from expected.json, computed by DuckDB at SF 0.1.
const LINEITEM_ROWS: usize = 600_572;
const SUM_QUANTITY_CENTS: i128 = 1_533_480_200;
const SUM_EXTENDEDPRICE_CENTS: i128 = 2_161_592_928_024;
const SUM_COMMENT_BYTES: i64 = 15_922_811;
const ORDERKEY_1000_TO_50000: usize = 49_188;
const PARTKEY_1000_TO_5000: usize = 119_939; // Not in expected.json; computed with DuckDB.
const SHIPMODE_AIR: usize = 85_689;

const DECIMAL_15_2: DataType = DataType::Decimal128(15, 2);

/// The schema of one TPC-H table, using the DuckDB types from the TPC-H spec. Strings are
/// `Utf8View`, the type the engine works with.
fn table_schema(name: &str) -> SchemaRef {
    use DataType::{Date32, Int32, Int64, Utf8View};
    let fields: Vec<Field> = match name {
        "region" => vec![
            Field::new("r_regionkey", Int32, false),
            Field::new("r_name", Utf8View, false),
            Field::new("r_comment", Utf8View, true),
        ],
        "nation" => vec![
            Field::new("n_nationkey", Int32, false),
            Field::new("n_name", Utf8View, false),
            Field::new("n_regionkey", Int32, false),
            Field::new("n_comment", Utf8View, true),
        ],
        "supplier" => vec![
            Field::new("s_suppkey", Int64, false),
            Field::new("s_name", Utf8View, false),
            Field::new("s_address", Utf8View, false),
            Field::new("s_nationkey", Int32, false),
            Field::new("s_phone", Utf8View, false),
            Field::new("s_acctbal", DECIMAL_15_2, false),
            Field::new("s_comment", Utf8View, false),
        ],
        "customer" => vec![
            Field::new("c_custkey", Int64, false),
            Field::new("c_name", Utf8View, false),
            Field::new("c_address", Utf8View, false),
            Field::new("c_nationkey", Int32, false),
            Field::new("c_phone", Utf8View, false),
            Field::new("c_acctbal", DECIMAL_15_2, false),
            Field::new("c_mktsegment", Utf8View, false),
            Field::new("c_comment", Utf8View, false),
        ],
        "part" => vec![
            Field::new("p_partkey", Int64, false),
            Field::new("p_name", Utf8View, false),
            Field::new("p_mfgr", Utf8View, false),
            Field::new("p_brand", Utf8View, false),
            Field::new("p_type", Utf8View, false),
            Field::new("p_size", Int32, false),
            Field::new("p_container", Utf8View, false),
            Field::new("p_retailprice", DECIMAL_15_2, false),
            Field::new("p_comment", Utf8View, false),
        ],
        "partsupp" => vec![
            Field::new("ps_partkey", Int64, false),
            Field::new("ps_suppkey", Int64, false),
            Field::new("ps_availqty", Int32, false),
            Field::new("ps_supplycost", DECIMAL_15_2, false),
            Field::new("ps_comment", Utf8View, false),
        ],
        "orders" => vec![
            Field::new("o_orderkey", Int64, false),
            Field::new("o_custkey", Int64, false),
            Field::new("o_orderstatus", Utf8View, false),
            Field::new("o_totalprice", DECIMAL_15_2, false),
            Field::new("o_orderdate", Date32, false),
            Field::new("o_orderpriority", Utf8View, false),
            Field::new("o_clerk", Utf8View, false),
            Field::new("o_shippriority", Int32, false),
            Field::new("o_comment", Utf8View, false),
        ],
        "lineitem" => vec![
            Field::new("l_orderkey", Int64, false),
            Field::new("l_partkey", Int64, false),
            Field::new("l_suppkey", Int64, false),
            Field::new("l_linenumber", Int32, false),
            Field::new("l_quantity", DECIMAL_15_2, false),
            Field::new("l_extendedprice", DECIMAL_15_2, false),
            Field::new("l_discount", DECIMAL_15_2, false),
            Field::new("l_tax", DECIMAL_15_2, false),
            Field::new("l_returnflag", Utf8View, false),
            Field::new("l_linestatus", Utf8View, false),
            Field::new("l_shipdate", Date32, false),
            Field::new("l_commitdate", Date32, false),
            Field::new("l_receiptdate", Date32, false),
            Field::new("l_shipinstruct", Utf8View, false),
            Field::new("l_shipmode", Utf8View, false),
            Field::new("l_comment", Utf8View, false),
        ],
        other => panic!("no TPC-H schema for table {other}"),
    };
    Arc::new(Schema::new(fields))
}

fn data_dir() -> PathBuf {
    std::env::var("HEARTH_TPCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/hearth-tpch/data"))
}

/// Reads `{name}.csv` and writes it as one Vortex file in a temporary directory. The CSV reader
/// gives `Utf8`, so string columns are cast to `Utf8View` before writing.
fn load_table(name: &str) -> TestTable {
    let target = table_schema(name);
    let read_schema: SchemaRef = Arc::new(Schema::new(
        target
            .fields()
            .iter()
            .map(|f| {
                let dt = if f.data_type() == &DataType::Utf8View {
                    DataType::Utf8
                } else {
                    f.data_type().clone()
                };
                Field::new(f.name(), dt, f.is_nullable())
            })
            .collect::<Vec<_>>(),
    ));

    let file = std::fs::File::open(data_dir().join(format!("{name}.csv")))
        .expect("CSV missing; run gen_tpch.py first");
    let reader = ReaderBuilder::new(read_schema.clone())
        .with_header(true)
        .build(file)
        .unwrap();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    let combined = concat_batches(&read_schema, &batches).unwrap();

    let columns: Vec<ArrayRef> = combined
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(column, field)| {
            if field.data_type() == &DataType::Utf8View {
                cast(column, &DataType::Utf8View).unwrap()
            } else {
                column.clone()
            }
        })
        .collect();
    write_table(target, vec![columns])
}

/// Runs a scan with the given projection and returns every batch.
fn scan_all(snap: &Snapshot, columns: &[&str], predicate: Predicate) -> Vec<RecordBatch> {
    let rt = Arc::new(StorageRuntime::with_defaults().unwrap());
    stream::scan(
        &rt,
        snap,
        &Projection::new(columns.iter().copied()),
        &predicate,
        None,
    )
    .unwrap()
    .map(|b| b.unwrap())
    .collect()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Sums a decimal column as its unscaled integer. At scale 2 that is the total in cents.
fn decimal_sum_cents(batches: &[RecordBatch], column: &str) -> i128 {
    batches
        .iter()
        .map(|b| {
            let array = b
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal column");
            assert_eq!(array.null_count(), 0);
            array.values().iter().sum::<i128>()
        })
        .sum()
}

fn comment_bytes(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .map(|b| {
            let array = b
                .column_by_name("l_comment")
                .unwrap()
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("string view column");
            array.iter().map(|s| s.unwrap().len() as i64).sum::<i64>()
        })
        .sum()
}

#[test]
#[ignore = "needs TPC-H CSVs from gen_tpch.py; run with --ignored"]
fn tpch_row_counts_match_duckdb() {
    for (name, expected) in ROW_COUNTS {
        let table = load_table(name);
        let snap = &table.snapshot;
        let first_column = snap.schema.field(0).name().clone();
        let batches = scan_all(snap, &[first_column.as_str()], Predicate::True);
        assert_eq!(row_count(&batches), expected, "row count of {name}");
    }
}

#[test]
#[ignore = "needs TPC-H CSVs from gen_tpch.py; run with --ignored"]
fn tpch_lineitem_matches_duckdb() {
    let table = load_table("lineitem");
    let snap = &table.snapshot;

    let all = scan_all(snap, &["l_orderkey"], Predicate::True);
    assert_eq!(row_count(&all), LINEITEM_ROWS, "row count");

    let quantity = scan_all(snap, &["l_quantity"], Predicate::True);
    assert_eq!(
        decimal_sum_cents(&quantity, "l_quantity"),
        SUM_QUANTITY_CENTS,
        "sum(l_quantity)"
    );

    let price = scan_all(snap, &["l_extendedprice"], Predicate::True);
    assert_eq!(
        decimal_sum_cents(&price, "l_extendedprice"),
        SUM_EXTENDEDPRICE_CENTS,
        "sum(l_extendedprice)"
    );

    let comments = scan_all(snap, &["l_comment"], Predicate::True);
    assert_eq!(
        comment_bytes(&comments),
        SUM_COMMENT_BYTES,
        "sum(octet_length(l_comment))"
    );

    let range = Predicate::And(vec![
        Predicate::compare("l_orderkey", CmpOp::Ge, 1000i64),
        Predicate::compare("l_orderkey", CmpOp::Le, 50_000i64),
    ]);
    assert_eq!(
        row_count(&scan_all(snap, &["l_orderkey"], range)),
        ORDERKEY_1000_TO_50000
    );

    let partkey = Predicate::And(vec![
        Predicate::compare("l_partkey", CmpOp::Ge, 1000i64),
        Predicate::compare("l_partkey", CmpOp::Le, 5000i64),
    ]);
    assert_eq!(
        row_count(&scan_all(snap, &["l_partkey"], partkey)),
        PARTKEY_1000_TO_5000
    );

    let air = Predicate::compare("l_shipmode", CmpOp::Eq, "AIR");
    assert_eq!(
        row_count(&scan_all(snap, &["l_shipmode"], air)),
        SHIPMODE_AIR
    );
}

/// Times lineitem scans with one column or all columns, with and without a filter. Each case
/// runs three times and prints the median. Nothing is asserted; read the output.
#[test]
#[ignore = "timing run; needs TPC-H CSVs. Run with --ignored --nocapture"]
fn tpch_lineitem_timing() {
    let table = load_table("lineitem");
    let snap = &table.snapshot;
    let cpu = std::env::var("HEARTH_CPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let rt = Arc::new(StorageRuntime::new(2, cpu).unwrap());

    let all_columns: Vec<String> = snap
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let one_column = vec!["l_quantity".to_string()];
    let selective = Predicate::And(vec![
        Predicate::compare("l_orderkey", CmpOp::Ge, 1000i64),
        Predicate::compare("l_orderkey", CmpOp::Le, 50_000i64),
    ]);
    let cases: [(&str, &Vec<String>, Predicate); 4] = [
        ("all columns, no filter", &all_columns, Predicate::True),
        ("one column, no filter", &one_column, Predicate::True),
        (
            "one column, selective filter",
            &one_column,
            selective.clone(),
        ),
        ("all columns, selective filter", &all_columns, selective),
    ];

    eprintln!("cpu threads: {cpu}");
    for (label, columns, predicate) in cases {
        let projection = Projection::new(columns.iter().cloned());
        let mut times: Vec<Duration> = (0..3)
            .map(|_| {
                let start = Instant::now();
                let rows: usize = stream::scan(&rt, snap, &projection, &predicate, None)
                    .unwrap()
                    .map(|b| b.unwrap().num_rows())
                    .sum();
                assert!(rows > 0);
                start.elapsed()
            })
            .collect();
        times.sort();
        eprintln!(
            "{label:<32} median {:>8.1} ms",
            times[1].as_secs_f64() * 1000.0
        );
    }
}
