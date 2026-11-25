//! Bridge from the async Vortex scan to the synchronous engine.
//!
//! [`scan`] plans a scan, then a driver task on the storage I/O pool reads the splits and pushes
//! Arrow batches into a bounded channel. The engine pulls them with [`BatchStream`], a plain
//! [`Iterator`].
//!
//! Each split is read chunk by chunk as Vortex produces it. The filter and the projection are
//! passed to the Vortex scan (see `pushdown`), so Vortex drops non-matching rows and reads only
//! the needed columns. Each file is opened once, by the first of its splits to run, and its
//! other splits reuse it. Converting each chunk to Arrow runs on the storage CPU pool. At most
//! `cpu_threads` splits are read at once, and the channel holds at most `2 * cpu_threads`
//! batches, so a slow consumer stops the readers instead of buffering the whole table.

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{Array, StructArray};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio::sync::{OnceCell, mpsc};
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::expr::{BoundExpression, Expression};
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::session::VortexSession;

use crate::error::{StorageError, StorageResult};
use crate::plan::{PruningReport, plan};
use crate::pushdown::{predicate_expr, projection_expr};
use crate::runtime::StorageRuntime;
use crate::scan::{Predicate, Projection};
use crate::table::Snapshot;

/// Arrow batches from one scan, pulled by a synchronous caller.
///
/// Order: batches from one split arrive in row order, but splits are read concurrently, so
/// batches from different splits (and files) interleave in no fixed order.
///
/// Batches are never empty. With a limit, the total number of rows yielded is exactly
/// `min(limit, matching rows)`.
///
/// After an `Err` item the stream is finished: the scan is stopped and `next` returns `None`.
///
/// Dropping the stream stops the background readers. They notice at their next chunk or send.
pub struct BatchStream {
    rx: mpsc::Receiver<StorageResult<RecordBatch>>,
    pruning_report: PruningReport,
    /// Set on drop, on error and when the limit is reached. Readers check it before each chunk.
    cancelled: Arc<AtomicBool>,
    /// Number of Vortex chunks read so far, so tests can check that a dropped stream stops.
    #[cfg(test)]
    chunks_read: Arc<AtomicUsize>,
    finished: bool,
    /// Keeps the I/O and CPU pools alive while the scan runs.
    _runtime: Arc<StorageRuntime>,
}

impl BatchStream {
    pub fn pruning_report(&self) -> &PruningReport {
        &self.pruning_report
    }

    #[cfg(test)]
    fn chunks_read(&self) -> usize {
        self.chunks_read.load(Ordering::SeqCst)
    }
}

impl Iterator for BatchStream {
    type Item = StorageResult<RecordBatch>;

    /// Blocks until the next batch is ready, or returns `None` when the scan is done.
    ///
    /// # Panics
    ///
    /// Must not be called from a thread that runs a Tokio runtime (including the storage pools,
    /// or inside `block_on`): it uses `blocking_recv`, and Tokio panics when a thread inside a
    /// runtime blocks. Call it from a plain thread such as an engine worker.
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let item = self.rx.blocking_recv()?;
        if item.is_err() {
            self.finished = true;
            self.cancelled.store(true, Ordering::SeqCst);
        }
        Some(item)
    }
}

impl Drop for BatchStream {
    fn drop(&mut self) {
        // The receiver is dropped right after this, which also fails any pending send.
        self.cancelled.store(true, Ordering::SeqCst);
    }
}

/// State shared by every split of one scan.
struct Shared {
    session: VortexSession,
    cpu: tokio::runtime::Handle,
    schema: SchemaRef,
    projection: Expression,
    /// `None` when the scan keeps every row.
    filter: Option<Expression>,
    tx: mpsc::Sender<StorageResult<RecordBatch>>,
    cancelled: Arc<AtomicBool>,
    #[cfg(test)]
    chunks_read: Arc<AtomicUsize>,
    /// Rows still allowed by the limit. `usize::MAX` when there is no limit.
    remaining: AtomicUsize,
}

impl Shared {
    fn stopped(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Takes up to `want` rows from the limit and returns how many were granted. Stops the scan
    /// once the limit is used up.
    fn reserve(&self, want: usize) -> usize {
        let before = self
            .remaining
            .update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left - want.min(left)
            });
        let granted = want.min(before);
        if granted == before {
            self.cancelled.store(true, Ordering::SeqCst);
        }
        granted
    }

    async fn fail(&self, e: StorageError) {
        self.cancelled.store(true, Ordering::SeqCst);
        let _ = self.tx.send(Err(e)).await;
    }
}

/// One planned file. The first split to read it opens it, and the others reuse it.
struct FileSource {
    path: PathBuf,
    opened: OnceCell<OpenedFile>,
}

/// An open file with the scan's expressions bound to its schema.
struct OpenedFile {
    file: VortexFile,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
}

impl FileSource {
    async fn open(&self, shared: &Shared) -> StorageResult<&OpenedFile> {
        self.opened
            .get_or_try_init(|| async {
                let file = shared.session.open_options().open_path(&self.path).await?;
                let dtype = file.dtype();
                let projection = shared.projection.bind(dtype)?;
                let filter = shared.filter.as_ref().map(|f| f.bind(dtype)).transpose()?;
                Ok::<_, StorageError>(OpenedFile {
                    file,
                    projection,
                    filter,
                })
            })
            .await
    }
}

/// Plans the scan, then reads its splits in the background.
///
/// Planning errors (unknown column, bad predicate) are returned here as
/// [`StorageError::Plan`] before any task starts. Errors while reading arrive as an `Err` item in
/// the stream.
///
/// Batches from different splits arrive in no fixed order; see [`BatchStream`].
pub fn scan(
    runtime: &Arc<StorageRuntime>,
    snapshot: &Snapshot,
    projection: &Projection,
    predicate: &Predicate,
    limit: Option<usize>,
) -> StorageResult<BatchStream> {
    let scan_plan = plan(snapshot, projection, predicate, limit)?;
    let projected_schema = Arc::new(Schema::new(
        projection
            .columns
            .iter()
            .map(|name| {
                snapshot
                    .schema
                    .field_with_name(name)
                    .expect("validated by plan")
                    .clone()
            })
            .collect::<Vec<_>>(),
    ));

    let splits: Vec<(Arc<FileSource>, Range<u64>)> = scan_plan
        .files
        .into_iter()
        .flat_map(|file| {
            let source = Arc::new(FileSource {
                path: file.path,
                opened: OnceCell::new(),
            });
            file.splits
                .into_iter()
                .map(move |rows| (source.clone(), rows))
        })
        .collect();

    let threads = runtime.cpu_threads();
    let (tx, rx) = mpsc::channel(2 * threads);
    let cancelled = Arc::new(AtomicBool::new(false));
    #[cfg(test)]
    let chunks_read = Arc::new(AtomicUsize::new(0));

    let shared = Arc::new(Shared {
        session: runtime.session(),
        cpu: runtime.cpu().clone(),
        schema: projected_schema,
        projection: projection_expr(&projection.columns),
        filter: scan_plan.filter.as_ref().map(predicate_expr),
        tx,
        cancelled: cancelled.clone(),
        #[cfg(test)]
        chunks_read: chunks_read.clone(),
        remaining: AtomicUsize::new(limit.unwrap_or(usize::MAX)),
    });

    // The driver holds no `Arc<StorageRuntime>`: if the stream (and with it the last runtime
    // reference) is dropped, the pools shut down and the driver is dropped with them.
    runtime.io().spawn(async move {
        futures::stream::iter(splits)
            .for_each_concurrent(threads, |(source, rows)| {
                let shared = shared.clone();
                async move {
                    if let Err(e) = read_split(&shared, &source, rows).await {
                        shared.fail(e).await;
                    }
                }
            })
            .await;
        // `shared.tx` drops here, closing the channel once every split is done.
    });

    Ok(BatchStream {
        rx,
        pruning_report: scan_plan.pruning_report,
        cancelled,
        #[cfg(test)]
        chunks_read,
        finished: false,
        _runtime: runtime.clone(),
    })
}

/// Reads one split chunk by chunk and sends its non-empty batches. Returns early, without
/// error, when the scan is cancelled or the consumer is gone.
async fn read_split(
    shared: &Arc<Shared>,
    source: &FileSource,
    rows: Range<u64>,
) -> StorageResult<()> {
    if shared.stopped() {
        return Ok(());
    }
    let opened = source.open(shared).await?;
    let mut builder = opened
        .file
        .scan()?
        .with_row_range(rows)
        .with_projection(opened.projection.clone());
    if let Some(filter) = &opened.filter {
        builder = builder.with_filter(filter.clone());
    }
    let mut chunks = Box::pin(builder.into_array_stream()?);

    loop {
        if shared.stopped() {
            return Ok(());
        }
        let Some(chunk) = chunks.next().await else {
            return Ok(());
        };
        let chunk = chunk?;
        #[cfg(test)]
        shared.chunks_read.fetch_add(1, Ordering::SeqCst);

        let work = shared.clone();
        let batch = shared
            .cpu
            .spawn(async move { to_record_batch(&work.session, chunk, &work.schema) })
            .await
            .map_err(|e| StorageError::Internal(format!("cpu task failed: {e}")))??;

        if batch.num_rows() == 0 {
            continue;
        }
        let granted = shared.reserve(batch.num_rows());
        if granted == 0 {
            return Ok(());
        }
        let batch = if granted < batch.num_rows() {
            batch.slice(0, granted)
        } else {
            batch
        };
        if shared.tx.send(Ok(batch)).await.is_err() {
            return Ok(());
        }
    }
}

fn to_record_batch(
    session: &VortexSession,
    array: vortex::array::ArrayRef,
    schema: &SchemaRef,
) -> StorageResult<RecordBatch> {
    let mut ctx = session.create_execution_ctx();
    let arrow_array = session.arrow().execute_arrow(array, None, &mut ctx)?;
    let structs = arrow_array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            StorageError::Internal(format!(
                "expected a struct array, got {}",
                arrow_array.data_type()
            ))
        })?;
    let batch = RecordBatch::from(structs);
    if batch.schema() != *schema {
        Ok(RecordBatch::try_new(
            schema.clone(),
            batch.columns().to_vec(),
        )?)
    } else {
        Ok(batch)
    }
}

#[cfg(test)]
// `&[0..n]` is a list of one file holding the range `0..n`, not a mistyped range.
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::*;
    use crate::scan::CmpOp;
    use crate::test_util::{TestTable, int64_values, write_table};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::time::Duration;

    fn id_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    /// Writes one file per range, with an Int64 `id` column holding the range. File ids start at 1.
    fn id_table(ranges: &[Range<i64>]) -> TestTable {
        let files = ranges
            .iter()
            .map(|range| vec![Arc::new(Int64Array::from_iter_values(range.clone())) as ArrayRef])
            .collect();
        write_table(id_schema(), files)
    }

    fn runtime(cpu: usize) -> Arc<StorageRuntime> {
        Arc::new(StorageRuntime::new(1, cpu).unwrap())
    }

    fn ids() -> Projection {
        Projection::new(["id"])
    }

    fn collect(stream: BatchStream) -> Vec<RecordBatch> {
        stream.map(|b| b.unwrap()).collect()
    }

    #[test]
    fn full_scan_returns_every_row() {
        let table = id_table(&[0..300_000]);
        let rt = runtime(2);
        let batches = collect(scan(&rt, &table.snapshot, &ids(), &Predicate::True, None).unwrap());

        assert!(
            batches
                .iter()
                .all(|b| b.num_rows() > 0 && b.num_rows() <= 131_072)
        );
        let values = int64_values(&batches);
        assert_eq!(values.len(), 300_000);
        assert_eq!(values.iter().sum::<i64>(), (0..300_000i64).sum::<i64>());
    }

    #[test]
    fn predicate_and_projection_apply() {
        let table = id_table(&[0..300_000]);
        let rt = runtime(2);
        let predicate = Predicate::compare("id", CmpOp::Ge, 250_000i64);
        let batches = collect(scan(&rt, &table.snapshot, &ids(), &predicate, None).unwrap());

        assert!(batches.iter().all(|b| b.num_columns() == 1));
        let values = int64_values(&batches);
        assert_eq!(values.len(), 50_000);
        assert!(values.iter().all(|&v| v >= 250_000));
    }

    #[test]
    fn limit_is_exact() {
        let table = id_table(&[0..300_000]);
        let rt = runtime(2);

        let batches =
            collect(scan(&rt, &table.snapshot, &ids(), &Predicate::True, Some(10)).unwrap());
        assert_eq!(int64_values(&batches).len(), 10);

        let predicate = Predicate::compare("id", CmpOp::Gt, 100i64);
        let batches = collect(scan(&rt, &table.snapshot, &ids(), &predicate, Some(5)).unwrap());
        let values = int64_values(&batches);
        assert_eq!(values.len(), 5);
        assert!(values.iter().all(|&v| v > 100));
    }

    #[test]
    fn limit_is_exact_across_concurrent_splits() {
        // Many splits read at once; the shared counter must still cap the total exactly.
        let table = id_table(&[0..300_000, 300_000..600_000]);
        let rt = runtime(4);
        let predicate = Predicate::compare("id", CmpOp::Ge, 0i64);
        let batches =
            collect(scan(&rt, &table.snapshot, &ids(), &predicate, Some(150_000)).unwrap());
        assert_eq!(int64_values(&batches).len(), 150_000);
    }

    #[test]
    fn limit_zero_reads_nothing() {
        let table = id_table(&[0..1_000]);
        let rt = runtime(1);
        let predicate = Predicate::compare("id", CmpOp::Ge, 0i64);
        let mut stream = scan(&rt, &table.snapshot, &ids(), &predicate, Some(0)).unwrap();
        assert!(stream.next().is_none());
        assert_eq!(stream.chunks_read(), 0);
    }

    #[test]
    fn empty_snapshot_yields_nothing() {
        let snap = Snapshot {
            version: 0,
            schema: id_schema(),
            files: vec![],
        };
        let rt = runtime(1);
        let mut stream = scan(&rt, &snap, &ids(), &Predicate::True, None).unwrap();
        assert!(stream.next().is_none());
        assert_eq!(stream.pruning_report().files_total, 0);
    }

    #[test]
    fn unknown_projection_column_is_plan_error() {
        let snap = Snapshot {
            version: 0,
            schema: id_schema(),
            files: vec![],
        };
        let rt = runtime(1);
        let result = scan(
            &rt,
            &snap,
            &Projection::new(["missing"]),
            &Predicate::True,
            None,
        );
        assert!(matches!(result, Err(StorageError::Plan(_))));
    }

    #[test]
    fn read_error_is_reported_then_stream_ends() {
        let mut table = id_table(&[0..10]);
        let file = &mut table.snapshot.files[0];
        file.path = file.path.with_file_name("does-not-exist.vortex");
        let rt = runtime(1);
        let mut stream = scan(&rt, &table.snapshot, &ids(), &Predicate::True, None).unwrap();
        assert!(matches!(stream.next(), Some(Err(StorageError::Vortex(_)))));
        assert!(stream.next().is_none());
    }

    #[test]
    fn dropping_the_stream_stops_reading() {
        // 6 files x 140,000 rows = 12 splits.
        let ranges: Vec<_> = (0..6).map(|i| i * 140_000..(i + 1) * 140_000).collect();
        let table = id_table(&ranges);
        let rt = runtime(1);

        let full = scan(&rt, &table.snapshot, &ids(), &Predicate::True, None).unwrap();
        let full_chunks = full.chunks_read.clone();
        assert_eq!(int64_values(&collect(full)).len(), 840_000);
        let total = full_chunks.load(Ordering::SeqCst);

        let mut stream = scan(&rt, &table.snapshot, &ids(), &Predicate::True, None).unwrap();
        let chunks = stream.chunks_read.clone();
        stream.next().unwrap().unwrap();
        let at_first = stream.chunks_read();
        drop(stream);
        std::thread::sleep(Duration::from_millis(300));
        let after = chunks.load(Ordering::SeqCst);
        eprintln!("chunks: full scan {total}, at first batch {at_first}, after drop {after}");

        // With 1 CPU thread one split is read at a time and the channel holds 2 batches. Every
        // chunk here gives a non-empty batch, so the reader can be at most 1 (taken) + 2
        // (buffered) + 1 (blocked in send) = 4 chunks ahead before it must wait for the
        // consumer. Once the stream is dropped, that send fails and the cancelled flag stops it.
        assert!(after <= 4, "read {after} chunks after drop");
        assert!(total > 4, "full scan read only {total} chunks");
    }

    #[test]
    fn dropping_stream_mid_scan_returns_quickly_and_cleanly() {
        let table = id_table(&[0..300_000, 300_000..600_000]);
        let rt = runtime(2);
        let mut stream = scan(&rt, &table.snapshot, &ids(), &Predicate::True, None).unwrap();
        assert!(stream.next().unwrap().is_ok());

        let start = std::time::Instant::now();
        drop(stream);
        // Shutting the pools down runs in the background, so the drop should not wait for reads.
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "drop took {:?}",
            start.elapsed()
        );

        // The caller's own handle keeps working after the scan is gone.
        let again = collect(scan(&rt, &table.snapshot, &ids(), &Predicate::True, Some(5)).unwrap());
        assert_eq!(again.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
    }

    #[test]
    fn pruning_report_passes_through() {
        let table = id_table(&[0..1_000, 1_000..2_000]);
        let rt = runtime(1);
        let predicate = Predicate::compare("id", CmpOp::Ge, 1_500i64);
        let stream = scan(&rt, &table.snapshot, &ids(), &predicate, None).unwrap();
        assert_eq!(stream.pruning_report().files_total, 2);
        assert_eq!(stream.pruning_report().files_skipped, 1);
        let values = int64_values(&collect(stream));
        assert_eq!(values.len(), 500);
        assert!(values.iter().all(|&v| v >= 1_500));
    }

    #[test]
    fn projected_batches_match_snapshot_schema() {
        use arrow::array::StringViewArray;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let table = write_table(
            schema,
            vec![vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringViewArray::from(vec![Some("a"), None])) as ArrayRef,
            ]],
        );
        let rt = runtime(1);
        let proj = Projection::new(["name", "id"]);
        let batches = collect(scan(&rt, &table.snapshot, &proj, &Predicate::True, None).unwrap());
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].schema(),
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8View, true),
                Field::new("id", DataType::Int64, false),
            ]))
        );
    }
}
