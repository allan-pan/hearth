//! Writes Arrow batches as Vortex table files and scans them back as Arrow batches.
//!
//! - [`write::write_tablefile`] writes file & computes stats
//! - [`stream::scan`] plans a scan over a [`table::Snapshot`] & streams the matching rows
//! - [`scan`] the request types: [`scan::Projection`] and [`scan::Predicate`]
//! - [`value::Literal`] literal values and their type rules
//! - [`runtime::StorageRuntime`] io & cpu thread pools

pub mod error;
pub mod plan;
pub mod runtime;
pub mod scan;
pub mod stream;
pub mod table;
pub mod value;
pub mod write;

mod pushdown;

#[cfg(test)]
mod scan_tests;
#[cfg(test)]
mod test_util;
#[cfg(test)]
mod tpch_tests;
