use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;

use crate::error::StorageResult;
use crate::runtime::StorageRuntime;
use crate::table::{ColumnStats, TableFile};
use crate::value::min_max;

/// Makes temporary file names unique among writers in this process.
static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

/// Writes `batch` to `dir/{id}.vortex` and returns its [`TableFile`] with stats.
///
/// The file appears at its final path only once it is complete and synced, so a crash never
/// leaves a partial table file behind. An existing file is never replaced: if `dir/{id}.vortex`
/// already exists, this returns an [`Io`](crate::error::StorageError::Io) error of kind
/// `AlreadyExists`.
///
/// Blocks until the file is written. Like [`BatchStream::next`](crate::stream::BatchStream),
/// it must not be called from a thread that runs a Tokio runtime.
///
/// The file is not part of any snapshot until a commit adds it.
pub fn write_tablefile(
    runtime: &StorageRuntime,
    dir: &Path,
    id: u64,
    batch: RecordBatch,
) -> StorageResult<TableFile> {
    let path = dir.join(format!("{id}.vortex"));
    let stats = compute_stats(&batch);
    let row_count = batch.num_rows() as u64;

    // Write under a unique hidden name, then hard-link it into place. Unlike a rename, the
    // link fails instead of replacing a file that is already there.
    let temp = dir.join(format!(
        ".{id}.vortex.{}-{}.tmp",
        std::process::id(),
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let written = write_vortex(runtime, &temp, batch)
        .and_then(|()| std::fs::hard_link(&temp, &path).map_err(Into::into));
    // Best effort: a leftover temporary file is harmless, and the write's own error matters more.
    let _ = std::fs::remove_file(&temp);
    written?;

    let size_bytes = std::fs::metadata(&path)?.len();
    Ok(TableFile {
        id,
        path,
        row_count,
        size_bytes,
        stats,
    })
}

/// Writes `batch` as a new Vortex file at `path` and syncs it to disk.
fn write_vortex(runtime: &StorageRuntime, path: &Path, batch: RecordBatch) -> StorageResult<()> {
    let session = runtime.session();
    runtime.io().block_on(async {
        let schema = batch.schema();
        let array = session.arrow().from_arrow_record_batch(batch, &schema)?;
        let mut file = tokio::fs::File::create_new(path).await?;
        session
            .write_options()
            .write(&mut file, array.to_array_stream())
            .await?;
        file.sync_all().await?;
        Ok(())
    })
}

/// Min, max and null count for each column. Types without stats support get `None` for min
/// and max, which means "unknown" and never lets a scan skip the file.
fn compute_stats(batch: &RecordBatch) -> Vec<ColumnStats> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| {
            let (min, max) = min_max(column).unzip();
            ColumnStats {
                name: field.name().clone(),
                min,
                max,
                null_count: column.null_count() as u64,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StorageError;
    use crate::test_util::sample_batch;
    use crate::value::Literal;
    use arrow::array::{ArrayRef, Date32Array, Decimal128Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn date_and_decimal_stats_skip_nulls_and_keep_column_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("day", DataType::Date32, true),
            Field::new("price", DataType::Decimal128(15, 2), true),
        ]));
        let days = Arc::new(Date32Array::from(vec![Some(3), None, Some(1)])) as ArrayRef;
        let prices = Arc::new(
            Decimal128Array::from(vec![Some(250), None, Some(-125)])
                .with_precision_and_scale(15, 2)
                .unwrap(),
        ) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![days, prices]).unwrap();

        let stats = compute_stats(&batch);

        assert!(matches!(stats[0].min, Some(Literal::Date32(1))));
        assert!(matches!(stats[0].max, Some(Literal::Date32(3))));
        assert_eq!(stats[0].null_count, 1);

        assert!(matches!(
            stats[1].min,
            Some(Literal::Decimal128 {
                value: -125,
                precision: 15,
                scale: 2
            })
        ));
        assert!(matches!(
            stats[1].max,
            Some(Literal::Decimal128 {
                value: 250,
                precision: 15,
                scale: 2
            })
        ));
        assert_eq!(stats[1].null_count, 1);
    }

    #[test]
    fn writes_file_and_reports_size_and_rows() {
        let rt = StorageRuntime::new(1, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();

        let file = write_tablefile(&rt, dir.path(), 2, sample_batch()).unwrap();

        assert_eq!(file.row_count, 3);
        assert_eq!(file.path, dir.path().join("2.vortex"));
        assert_eq!(
            file.size_bytes,
            std::fs::metadata(&file.path).unwrap().len()
        );
        assert!(file.size_bytes > 0);
    }

    #[test]
    fn refuses_to_replace_an_existing_file_and_cleans_up() {
        let rt = StorageRuntime::new(1, 1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let first = write_tablefile(&rt, dir.path(), 1, sample_batch()).unwrap();

        let second = write_tablefile(&rt, dir.path(), 1, sample_batch());

        assert!(matches!(
            second,
            Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(
            std::fs::metadata(&first.path).unwrap().len(),
            first.size_bytes
        );
        let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1, "temporary file left behind");
    }

    #[test]
    fn stats_skip_nulls_and_count_them() {
        let stats = compute_stats(&sample_batch());

        assert!(matches!(stats[0].min, Some(Literal::Int64(1))));
        assert!(matches!(stats[0].max, Some(Literal::Int64(3))));
        assert_eq!(stats[0].null_count, 0);

        assert!(matches!(&stats[1].min, Some(Literal::Utf8(s)) if s == "a"));
        assert!(matches!(&stats[1].max, Some(Literal::Utf8(s)) if s == "c"));
        assert_eq!(stats[1].null_count, 1);
    }
}
