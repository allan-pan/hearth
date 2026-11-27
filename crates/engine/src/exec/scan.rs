use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use storage::runtime::StorageRuntime;
use storage::scan::{Predicate, Projection};
use storage::stream::{self, BatchStream};
use storage::table::Snapshot;

use crate::error::EngineResult;
use crate::exec::{SinkGlobal, Source};

pub struct ScanSource {
    pub runtime: Arc<StorageRuntime>,
    pub snapshot: Snapshot,
    pub projection: Projection,
    pub predicate: Predicate,
    pub limit: Option<usize>,
    pub table: String,
}

impl ScanSource {
    pub fn all_columns(
        table: impl Into<String>,
        runtime: Arc<StorageRuntime>,
        snapshot: Snapshot,
    ) -> Self {
        let projection =
            Projection::new(snapshot.schema.fields().iter().map(|f| f.name().as_str()));
        Self {
            table: table.into(),
            runtime,
            snapshot,
            projection,
            predicate: Predicate::True,
            limit: None,
        }
    }
}

impl Source for ScanSource {
    type Global = ();
    type Local = BatchStream;

    fn init_global(&self, _slots: &[SinkGlobal]) -> Self::Global {}

    fn init_local(&self, _global: &Self::Global) -> EngineResult<Self::Local> {
        Ok(stream::scan(
            &self.runtime,
            &self.snapshot,
            &self.projection,
            &self.predicate,
            self.limit,
        )?)
    }

    fn next(
        &self,
        _global: &Self::Global,
        local: &mut Self::Local,
    ) -> EngineResult<Option<RecordBatch>> {
        Ok(local.next().transpose()?)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use storage::write::write_tablefile;

    use super::*;
    use crate::error::EngineError;

    fn write_test_snapshot(
        runtime: &StorageRuntime,
        schema: SchemaRef,
        files: Vec<Vec<ArrayRef>>,
    ) -> (Snapshot, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let table_files = files
            .into_iter()
            .enumerate()
            .map(|(i, cols)| {
                let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
                write_tablefile(runtime, dir.path(), i as u64 + 1, batch).unwrap()
            })
            .collect();
        (
            Snapshot {
                version: 0,
                schema,
                files: table_files,
            },
            dir,
        )
    }

    #[test]
    fn scans_all_files_and_matches_snapshot_schema() {
        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let (snapshot, _dir) = write_test_snapshot(
            &runtime,
            schema.clone(),
            vec![
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringViewArray::from(vec![Some("a"), None])),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![3])),
                    Arc::new(StringViewArray::from(vec![Some("c")])),
                ],
            ],
        );

        let source = ScanSource::all_columns("people", runtime, snapshot);
        let mut local = source.init_local(&()).unwrap();

        let mut ids = Vec::new();
        while let Some(batch) = source.next(&(), &mut local).unwrap() {
            assert_eq!(batch.schema(), schema);
            ids.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn scans_subset_and_reordered_projection() {
        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let (snapshot, _dir) = write_test_snapshot(
            &runtime,
            schema,
            vec![vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringViewArray::from(vec![Some("x"), None])),
            ]],
        );

        let source = ScanSource {
            table: "people".into(),
            runtime,
            snapshot,
            projection: Projection::new(["name"]),
            predicate: Predicate::True,
            limit: None,
        };
        let mut local = source.init_local(&()).unwrap();
        let batch = source.next(&(), &mut local).unwrap().unwrap();
        assert_eq!(
            batch.schema(),
            Arc::new(Schema::new(vec![Field::new(
                "name",
                DataType::Utf8View,
                true
            )]))
        );
        let names: Vec<Option<&str>> = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(names, vec![Some("x"), None]);
    }

    #[test]
    fn storage_read_error_maps_to_engine_storage_error() {
        let runtime = Arc::new(StorageRuntime::new(1, 1).unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let (mut snapshot, _dir) = write_test_snapshot(
            &runtime,
            schema,
            vec![vec![Arc::new(Int64Array::from(vec![1]))]],
        );
        snapshot.files[0].path = snapshot.files[0].path.with_file_name("missing.vortex");

        let source = ScanSource::all_columns("people", runtime, snapshot);
        let mut local = source.init_local(&()).unwrap();
        assert!(matches!(
            source.next(&(), &mut local),
            Err(EngineError::Storage(_))
        ));
    }
}
