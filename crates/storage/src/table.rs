//! What a table is made of: immutable files, and snapshots that list them.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::value::Literal;

/// Summary of one column inside one file. `None` for min or max means "unknown",
/// for example an all-null column. Unknown stats never let a scan skip the file.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub name: String,
    pub min: Option<Literal>,
    pub max: Option<Literal>,
    pub null_count: u64,
}

/// One immutable Vortex file of a table, with the metadata planning needs without opening it.
#[derive(Debug, Clone)]
pub struct TableFile {
    pub id: u64,
    pub path: PathBuf,
    pub row_count: u64,
    pub size_bytes: u64,
    pub stats: Vec<ColumnStats>,
}

/// A table at one version: its schema and the files that make it up. Scans read a snapshot, so
/// files added or removed later don't change what a running scan sees.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub version: u64,
    pub files: Vec<TableFile>,
    /// Every file in the snapshot has this schema. A snapshot with no files is valid.
    pub schema: Arc<Schema>,
}
