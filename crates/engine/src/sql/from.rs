use sqlparser::ast::{ObjectName, ObjectNamePart, TableFactor, TableWithJoins};

use super::{normalize, not_supported, reject_present};
use crate::catalog::Catalog;
use crate::error::{EngineError, EngineResult};
use crate::logical::LogicalPlan;

pub(super) fn bind_from(
    from: Vec<TableWithJoins>,
    catalog: &dyn Catalog,
) -> EngineResult<LogicalPlan> {
    let count = from.len();
    let Ok([TableWithJoins { relation, joins }]) = <[TableWithJoins; 1]>::try_from(from) else {
        return Err(match count {
            0 => not_supported("SELECT without FROM"),
            _ => not_supported("more than one table in FROM"),
        });
    };
    if !joins.is_empty() {
        return Err(not_supported("JOIN"));
    }

    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_directives,
        inheritance,
    } = relation
    else {
        return Err(not_supported("FROM items other than a table name"));
    };
    let unsupported = [
        ("table aliases", alias.is_some()),
        ("table functions", args.is_some()),
        ("table hints", !with_hints.is_empty()),
        ("time travel (FOR SYSTEM_TIME)", version.is_some()),
        ("WITH ORDINALITY", with_ordinality),
        ("PARTITION", !partitions.is_empty()),
        ("JSON paths in FROM", json_path.is_some()),
        ("TABLESAMPLE", sample.is_some()),
        ("index hints", !index_directives.is_empty()),
        ("ONLY / table inheritance", inheritance.is_some()),
    ];
    reject_present(&unsupported)?;

    let table = table_name(name)?;
    let snapshot = catalog
        .table(&table)
        .ok_or_else(|| EngineError::Bind(format!("table {table} does not exist")))?;
    Ok(LogicalPlan::Scan { table, snapshot })
}

fn table_name(name: ObjectName) -> EngineResult<String> {
    let ObjectName(parts) = name;
    match <[ObjectNamePart; 1]>::try_from(parts) {
        Ok([ObjectNamePart::Identifier(ident)]) => Ok(normalize(&ident)),
        Ok([ObjectNamePart::Function(_)]) => Err(not_supported("computed table names")),
        Err(_) => Err(not_supported("qualified table names (schema.table)")),
    }
}
