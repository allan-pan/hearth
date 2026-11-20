//! Scan planning: validates a request against a snapshot, prunes files by their stats, and cuts
//! the remaining files into row ranges (splits) that the scan reads concurrently.

use std::cmp::Ordering;
use std::ops::Range;
use std::path::PathBuf;

use crate::error::StorageResult;
use crate::scan::{CmpOp, Predicate, Projection};
use crate::table::{ColumnStats, Snapshot};
use crate::value::Literal;

#[derive(Debug, Clone)]
pub struct PruningReport {
    pub files_total: u64,
    pub files_skipped: u64,
}

const BLOCK_ROWS: u64 = 8_192;

const SPLIT_ROWS: u64 = 16 * BLOCK_ROWS;

/// One file to read and the row ranges to read from it.
#[derive(Debug, Clone)]
pub(crate) struct FileScan {
    pub path: PathBuf,
    /// Each range is one split, handed to one worker. Starts fall on multiples of 8,192 rows,
    /// and every split is 131,072 rows (16 blocks) except the last, which may be shorter.
    pub splits: Vec<Range<u64>>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScanPlan {
    /// Files that survived pruning, in snapshot order. Each has at least one split.
    pub files: Vec<FileScan>,
    pub pruning_report: PruningReport,
    /// The request's predicate coerced to the snapshot's schema, or `None` when it keeps every
    /// row.
    pub filter: Option<Predicate>,
}

/// Validates the request against the snapshot's schema, prunes files by their stats, and cuts
/// the remaining files into splits.
///
/// The limit drops splits that can't be needed. Without a filter every planned row reaches the
/// output, so planning stops once the limit is covered. With a filter no row is certain to
/// match, so every unpruned split is kept unless the limit is 0.
pub(crate) fn plan(
    snapshot: &Snapshot,
    projection: &Projection,
    predicate: &Predicate,
    limit: Option<usize>,
) -> StorageResult<ScanPlan> {
    projection.validate(&snapshot.schema)?;
    let filter = match predicate.coerce(&snapshot.schema)? {
        Predicate::True => None,
        p => Some(p),
    };
    let limit_covered = |certain_rows: u64| limit.is_some_and(|n| certain_rows >= n as u64);

    let mut files = Vec::new();
    let mut files_skipped = 0;
    // Planned rows that are certain to reach the output.
    let mut certain_rows: u64 = 0;

    'files: for file in &snapshot.files {
        if filter.as_ref().is_some_and(|f| !can_match(f, &file.stats)) {
            files_skipped += 1;
            continue;
        }
        let mut splits = Vec::new();
        for start in (0..file.row_count).step_by(SPLIT_ROWS as usize) {
            if limit_covered(certain_rows) {
                break;
            }
            let end = (start + SPLIT_ROWS).min(file.row_count);
            if filter.is_none() {
                certain_rows += end - start;
            }
            splits.push(start..end);
        }
        if !splits.is_empty() {
            files.push(FileScan {
                path: file.path.clone(),
                splits,
            });
        }
        if limit_covered(certain_rows) {
            break 'files;
        }
    }

    Ok(ScanPlan {
        files,
        pruning_report: PruningReport {
            files_total: snapshot.files.len() as u64,
            files_skipped,
        },
        filter,
    })
}

/// False only when `stats` prove that no row of the file can match `predicate`.
fn can_match(predicate: &Predicate, stats: &[ColumnStats]) -> bool {
    match predicate {
        Predicate::Compare { column, op, value } => {
            bounds(stats, column).is_none_or(|(min, max)| may_hold(*op, min, max, value))
        }
        Predicate::In { column, values } => bounds(stats, column)
            .is_none_or(|(min, max)| values.iter().any(|v| may_hold(CmpOp::Eq, min, max, v))),
        Predicate::IsNull { column } => stats
            .iter()
            .find(|s| s.name == *column)
            .is_none_or(|s| s.null_count > 0),
        Predicate::And(parts) => parts.iter().all(|p| can_match(p, stats)),
        Predicate::Or(parts) => parts.iter().any(|p| can_match(p, stats)),
        // Negation can't be pruned safely from min and max alone, so keep the file.
        Predicate::Not(_) => true,
        Predicate::True => true,
    }
}

fn bounds<'a>(stats: &'a [ColumnStats], column: &str) -> Option<(&'a Literal, &'a Literal)> {
    let stat = stats.iter().find(|s| s.name == column)?;
    Some((stat.min.as_ref()?, stat.max.as_ref()?))
}

/// Whether `x op value` can hold for some `x` in `min..=max`. True when the types don't compare.
fn may_hold(op: CmpOp, min: &Literal, max: &Literal, value: &Literal) -> bool {
    let min_vs_value = min.compare(value);
    let max_vs_value = max.compare(value);
    match op {
        CmpOp::Lt => min_vs_value.is_none_or(Ordering::is_lt),
        CmpOp::Le => min_vs_value.is_none_or(Ordering::is_le),
        CmpOp::Gt => max_vs_value.is_none_or(Ordering::is_gt),
        CmpOp::Ge => max_vs_value.is_none_or(Ordering::is_ge),
        CmpOp::Eq => {
            min_vs_value.is_none_or(Ordering::is_le) && max_vs_value.is_none_or(Ordering::is_ge)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFile;
    use crate::test_util::sample_batch;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn file(id: u64, row_count: u64) -> TableFile {
        TableFile {
            id,
            path: PathBuf::from(format!("{id}.vortex")),
            row_count,
            size_bytes: 0,
            stats: Vec::new(),
        }
    }

    fn file_with_id(id: u64, min: i64, max: i64, nulls: u64) -> TableFile {
        TableFile {
            stats: vec![ColumnStats {
                name: "id".to_string(),
                min: Some(Literal::Int64(min)),
                max: Some(Literal::Int64(max)),
                null_count: nulls,
            }],
            ..file(id, 3)
        }
    }

    fn snapshot(files: Vec<TableFile>) -> Snapshot {
        Snapshot {
            version: 0,
            schema: sample_batch().schema(),
            files,
        }
    }

    fn plan_with(files: Vec<TableFile>, predicate: &Predicate, limit: Option<usize>) -> ScanPlan {
        plan(&snapshot(files), &Projection::new(["id"]), predicate, limit).unwrap()
    }

    fn id_equals_one() -> Predicate {
        Predicate::compare("id", CmpOp::Eq, 1i64)
    }

    fn id(op: CmpOp, value: i64) -> Predicate {
        Predicate::compare("id", op, value)
    }

    fn split_count(plan: &ScanPlan) -> usize {
        plan.files.iter().map(|f| f.splits.len()).sum()
    }

    #[test]
    fn small_files_are_one_split_each() {
        let plan = plan_with(vec![file(1, 3), file(2, 5)], &id_equals_one(), None);

        assert_eq!(split_count(&plan), 2);
        assert_eq!(plan.files[1].path, PathBuf::from("2.vortex"));
        assert_eq!(plan.files[1].splits, vec![0..5]);
        assert_eq!(plan.pruning_report.files_total, 2);
        assert_eq!(plan.pruning_report.files_skipped, 0);
    }

    #[test]
    fn large_file_splits_into_131072_row_ranges() {
        let plan = plan_with(vec![file(1, 300_000)], &id_equals_one(), None);
        assert_eq!(
            plan.files[0].splits,
            vec![0..131_072, 131_072..262_144, 262_144..300_000]
        );
    }

    #[test]
    fn file_of_exactly_one_split_is_one_split() {
        let plan = plan_with(vec![file(1, 131_072)], &id_equals_one(), None);
        assert_eq!(split_count(&plan), 1);
    }

    #[test]
    fn split_starts_and_inner_ends_align_to_blocks() {
        let plan = plan_with(vec![file(1, 1_000_000)], &id_equals_one(), None);
        for split in &plan.files[0].splits {
            assert_eq!(split.start % 8_192, 0);
            if split.end != 1_000_000 {
                assert_eq!(split.end % 8_192, 0);
            }
        }
    }

    #[test]
    fn empty_file_is_not_planned() {
        let plan = plan_with(vec![file(1, 0)], &Predicate::True, None);
        assert!(plan.files.is_empty());
    }

    #[test]
    fn true_predicate_plans_no_filter() {
        let plan = plan_with(vec![file(1, 3)], &Predicate::True, None);
        assert!(plan.filter.is_none());
    }

    #[test]
    fn limit_cuts_splits_inside_a_large_file() {
        let plan = plan_with(vec![file(1, 300_000)], &Predicate::True, Some(1));
        assert_eq!(split_count(&plan), 1);
    }

    #[test]
    fn limit_cuts_splits_when_no_filter() {
        let plan = plan_with(
            vec![file(1, 3), file(2, 5), file(3, 4)],
            &Predicate::True,
            Some(6),
        );
        assert_eq!(split_count(&plan), 2);
    }

    #[test]
    fn limit_keeps_all_splits_when_filtered() {
        let plan = plan_with(vec![file(1, 3), file(2, 5)], &id_equals_one(), Some(1));
        assert_eq!(split_count(&plan), 2);
    }

    #[test]
    fn limit_zero_plans_nothing_even_with_a_filter() {
        let plan = plan_with(vec![file(1, 3), file(2, 5)], &id_equals_one(), Some(0));
        assert!(plan.files.is_empty());
    }

    #[test]
    fn plan_rejects_unknown_projection_column() {
        let result = plan(
            &snapshot(vec![file(1, 3)]),
            &Projection::new(["missing"]),
            &id_equals_one(),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn plan_rejects_bad_predicate() {
        let result = plan(
            &snapshot(vec![file(1, 3)]),
            &Projection::new(["id"]),
            &Predicate::compare("name", CmpOp::Eq, 1i64),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn plan_empty_snapshot_has_no_splits() {
        let plan = plan_with(vec![], &id_equals_one(), None);
        assert!(plan.files.is_empty());
        assert_eq!(plan.pruning_report.files_total, 0);
    }

    /// For each operator, the values that skip a file holding ids 1..=3 and the values that
    /// keep it.
    #[test]
    fn each_operator_prunes_by_the_right_bound() {
        let cases = [
            (CmpOp::Eq, vec![0, 4], vec![1, 2, 3]),
            (CmpOp::Lt, vec![0, 1], vec![2, 4]),
            (CmpOp::Le, vec![0], vec![1, 4]),
            (CmpOp::Gt, vec![3, 4], vec![0, 2]),
            (CmpOp::Ge, vec![4], vec![0, 3]),
        ];
        for (op, skips, keeps) in cases {
            for v in skips {
                let plan = plan_with(vec![file_with_id(1, 1, 3, 0)], &id(op, v), None);
                assert_eq!(
                    plan.pruning_report.files_skipped, 1,
                    "id {op:?} {v} should skip"
                );
                assert!(plan.files.is_empty());
            }
            for v in keeps {
                let plan = plan_with(vec![file_with_id(1, 1, 3, 0)], &id(op, v), None);
                assert_eq!(
                    plan.pruning_report.files_skipped, 0,
                    "id {op:?} {v} should keep"
                );
            }
        }
    }

    #[test]
    fn in_list_keeps_file_if_any_value_may_match() {
        let in_list = |values: Vec<i64>| Predicate::In {
            column: "id".to_string(),
            values: values.into_iter().map(Literal::from).collect(),
        };
        let skipped = |p| {
            plan_with(vec![file_with_id(1, 1, 3, 0)], &p, None)
                .pruning_report
                .files_skipped
        };
        assert_eq!(skipped(in_list(vec![0, 4])), 1);
        assert_eq!(skipped(in_list(vec![0, 2])), 0);
    }

    #[test]
    fn is_null_skips_file_without_nulls() {
        let file = TableFile {
            stats: vec![ColumnStats {
                name: "name".to_string(),
                min: Some(Literal::from("a")),
                max: Some(Literal::from("c")),
                null_count: 0,
            }],
            ..file(1, 3)
        };
        let plan = plan_with(
            vec![file],
            &Predicate::IsNull {
                column: "name".to_string(),
            },
            None,
        );
        assert!(plan.files.is_empty());
        assert_eq!(plan.pruning_report.files_skipped, 1);
    }

    #[test]
    fn missing_stats_keep_file() {
        let plan = plan_with(vec![file(1, 3)], &id(CmpOp::Gt, 5), None);
        assert_eq!(split_count(&plan), 1);
    }

    #[test]
    fn narrow_filter_keeps_only_matching_file() {
        let plan = plan_with(
            vec![file_with_id(1, 1, 3, 0), file_with_id(2, 4, 6, 0)],
            &id(CmpOp::Eq, 5),
            None,
        );
        assert_eq!(plan.files.len(), 1);
        assert_eq!(plan.files[0].path, PathBuf::from("2.vortex"));
        assert_eq!(plan.pruning_report.files_total, 2);
        assert_eq!(plan.pruning_report.files_skipped, 1);
    }

    #[test]
    fn int32_stats_prune_file() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let file = TableFile {
            stats: vec![ColumnStats {
                name: "x".to_string(),
                min: Some(Literal::Int32(1)),
                max: Some(Literal::Int32(3)),
                null_count: 0,
            }],
            ..file(1, 3)
        };
        let snap = Snapshot {
            version: 0,
            schema,
            files: vec![file],
        };
        let predicate = Predicate::compare("x", CmpOp::Gt, 5i64);
        let plan = plan(&snap, &Projection::new(["x"]), &predicate, None).unwrap();
        assert!(plan.files.is_empty());
        assert_eq!(plan.pruning_report.files_skipped, 1);
    }
}
