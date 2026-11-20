//! Thread pools that run Vortex work for the storage layer.
//!
//! Storage keeps I/O threads separate from processing threads.
//! Later the engine can supply its own CPU pool in place of the one created here.

use std::sync::Arc;

use futures::future::BoxFuture;
use tokio::runtime::{Builder, Runtime};
use vortex::VortexSessionDefault;
use vortex::io::runtime::{AbortHandleRef, Executor, Handle};
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

struct RoutingExecutor {
    io: tokio::runtime::Handle,
    cpu: tokio::runtime::Handle,
}

impl Executor for RoutingExecutor {
    /// Vortex `spawn` futures are light and only schedule other work, so they stay on I/O.
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
        Box::new(self.io.spawn(fut).abort_handle())
    }

    fn spawn_io(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
        Box::new(self.io.spawn(fut).abort_handle())
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        Box::new(self.cpu.spawn(async move { task() }).abort_handle())
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        Box::new(self.io.spawn_blocking(task).abort_handle())
    }
}

/// Dropping a `StorageRuntime` is safe anywhere, including inside an async context: the pools
/// are shut down in the background instead of being joined.
pub struct StorageRuntime {
    /// `None` only while dropping.
    io: Option<Runtime>,
    /// `None` only while dropping.
    cpu: Option<Runtime>,
    /// Vortex handles hold a weak reference to this, so it must live as long as the runtimes.
    executor: Arc<dyn Executor>,
    cpu_threads: usize,
}

impl StorageRuntime {
    /// Builds both pools. `io_threads` and `cpu_threads` must be >= 1.
    pub fn new(io_threads: usize, cpu_threads: usize) -> std::io::Result<Self> {
        if io_threads == 0 || cpu_threads == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "io_threads and cpu_threads must be >= 1",
            ));
        }
        let io = Builder::new_multi_thread()
            .worker_threads(io_threads)
            .thread_name("hearth-io")
            .enable_all()
            .build()?;
        let cpu = Builder::new_multi_thread()
            .worker_threads(cpu_threads)
            .thread_name("hearth-cpu")
            .enable_all()
            .build()?;
        let executor: Arc<dyn Executor> = Arc::new(RoutingExecutor {
            io: io.handle().clone(),
            cpu: cpu.handle().clone(),
        });
        Ok(Self {
            io: Some(io),
            cpu: Some(cpu),
            executor,
            cpu_threads,
        })
    }

    /// 2 I/O threads and one CPU thread per core (1 if the core count is unknown).
    pub fn with_defaults() -> std::io::Result<Self> {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self::new(2, cores)
    }

    /// A Vortex session whose tasks run on these pools.
    ///
    /// The session holds a weak handle: it stops working once this `StorageRuntime` is dropped.
    pub fn session(&self) -> VortexSession {
        VortexSession::default().with_handle(Handle::new(Arc::downgrade(&self.executor)))
    }

    pub(crate) fn io(&self) -> &tokio::runtime::Handle {
        self.io
            .as_ref()
            .expect("io runtime present until drop")
            .handle()
    }

    pub(crate) fn cpu(&self) -> &tokio::runtime::Handle {
        self.cpu
            .as_ref()
            .expect("cpu runtime present until drop")
            .handle()
    }

    pub fn cpu_threads(&self) -> usize {
        self.cpu_threads
    }
}

impl Drop for StorageRuntime {
    fn drop(&mut self) {
        // A tokio Runtime panics if dropped inside an async context; shutdown_background does not.
        // The executor field is dropped after this, so it outlives both runtimes.
        if let Some(io) = self.io.take() {
            io.shutdown_background();
        }
        if let Some(cpu) = self.cpu.take() {
            cpu.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::array::stream::ArrayStreamExt;
    use vortex::arrow::ArrowSessionExt;
    use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};

    #[test]
    fn storage_runtime_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StorageRuntime>();
    }

    #[test]
    fn rejects_zero_threads() {
        assert!(StorageRuntime::new(0, 1).is_err());
        assert!(StorageRuntime::new(1, 0).is_err());
    }

    #[test]
    fn spawn_cpu_runs_on_cpu_pool() {
        let rt = StorageRuntime::new(1, 2).unwrap();
        let session = rt.session();
        let task = RuntimeSessionExt::handle(&session)
            .spawn_cpu(|| std::thread::current().name().map(String::from));
        let name = rt.io().block_on(task);
        assert_eq!(name.as_deref(), Some("hearth-cpu"));
    }

    #[test]
    fn spawn_io_runs_on_io_pool() {
        let rt = StorageRuntime::new(1, 2).unwrap();
        let session = rt.session();
        let task = RuntimeSessionExt::handle(&session)
            .spawn_io(async { std::thread::current().name().map(String::from) });
        let name = rt.io().block_on(task);
        assert_eq!(name.as_deref(), Some("hearth-io"));
    }

    #[test]
    fn vortex_round_trip_on_session() {
        let rt = StorageRuntime::new(2, 2).unwrap();
        let session = rt.session();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roundtrip.vortex");

        let rows = rt
            .io()
            .block_on(async {
                let batch = crate::test_util::sample_batch();
                let schema = batch.schema();
                let array = session.arrow().from_arrow_record_batch(batch, &schema)?;
                session
                    .write_options()
                    .write(
                        &mut tokio::fs::File::create(&path).await?,
                        array.to_array_stream(),
                    )
                    .await?;
                let read = session
                    .open_options()
                    .open_path(path.clone())
                    .await?
                    .scan()?
                    .into_array_stream()?
                    .read_all()
                    .await?;
                Ok::<_, Box<dyn std::error::Error>>(read.len())
            })
            .unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn drop_after_work_does_not_panic() {
        let rt = StorageRuntime::new(1, 1).unwrap();
        let session = rt.session();
        let handle = RuntimeSessionExt::handle(&session);
        let v = rt.io().block_on(handle.spawn_cpu(|| 41 + 1));
        assert_eq!(v, 42);
        drop(rt);
    }

    #[test]
    fn drop_inside_async_context_does_not_panic() {
        let rt = StorageRuntime::new(1, 1).unwrap();
        let outer = tokio::runtime::Runtime::new().unwrap();
        outer.block_on(async move { drop(rt) });
    }
}
