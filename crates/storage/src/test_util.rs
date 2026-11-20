//! Helpers shared by the storage tests.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use tempfile::TempDir;

use crate::runtime::StorageRuntime;
use crate::table::Snapshot;
use crate::write::write_tablefile;

pub(crate) fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8View, true),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
    let names = Arc::new(StringViewArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, names]).unwrap()
}

/// A snapshot whose files live in a temporary directory, removed when this is dropped.
pub(crate) struct TestTable {
    pub snapshot: Snapshot,
    _dir: TempDir,
}

/// Writes one file per entry of `files`, each holding the given columns. File ids start at 1.
pub(crate) fn write_table(schema: SchemaRef, files: Vec<Vec<ArrayRef>>) -> TestTable {
    let dir = tempfile::tempdir().unwrap();
    let rt = StorageRuntime::new(1, 1).unwrap();
    let files = files
        .into_iter()
        .enumerate()
        .map(|(i, columns)| {
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            write_tablefile(&rt, dir.path(), i as u64 + 1, batch).unwrap()
        })
        .collect();
    TestTable {
        snapshot: Snapshot {
            version: 0,
            schema,
            files,
        },
        _dir: dir,
    }
}

/// The values of the first column, which must be `Int64`, across all batches.
pub(crate) fn int64_values(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("first column is Int64")
                .values()
                .to_vec()
        })
        .collect()
}
