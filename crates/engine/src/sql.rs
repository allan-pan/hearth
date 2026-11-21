//! Binds sqlparser's AST into a `LogicalPlan`.
use arrow::datatypes::Schema;
use sqlparser::ast::{
    self, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectFlavor, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, WildcardAdditionalOptions,
};

use crate::catalog::Catalog;
use crate::error::{EngineError, EngineResult};
use crate::expr::Expr;
use crate::logical::LogicalPlan;

pub fn bind(statements: Vec<Statement>, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let count = statements.len();
    let Ok([statement]) = <[Statement; 1]>::try_from(statements) else {
        return Err(not_supported(&format!(
            "running {count} statements at once (exactly one is supported)"
        )));
    };
    match statement {
        Statement::Query(query) => bind_query(*query, catalog),
        _ => Err(not_supported("statements other than SELECT")),
    }
}

fn bind_query(query: Query, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let Query {
        with,
        body,
        order_by,
        limit_clause,
        fetch,
        locks,
        for_clause,
        settings,
        format_clause,
        pipe_operators,
    } = query;

    let unsupported = [
        ("WITH", with.is_some()),
        ("ORDER BY", order_by.is_some()),
        ("LIMIT", limit_clause.is_some()),
        ("FETCH", fetch.is_some()),
        ("FOR UPDATE/SHARE", !locks.is_empty()),
        ("FOR XML/JSON", for_clause.is_some()),
        ("SETTINGS", settings.is_some()),
        ("FORMAT", format_clause.is_some()),
        ("pipe operators", !pipe_operators.is_empty()),
    ];
    reject_present(&unsupported)?;

    match *body {
        SetExpr::Select(select) => bind_select(*select, catalog),
        SetExpr::SetOperation { op, .. } => Err(not_supported(&op.to_string())),
        _ => Err(not_supported("query bodies other than SELECT")),
    }
}

fn bind_select(select: Select, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
    let Select {
        select_token: _,
        optimizer_hints,
        distinct,
        select_modifiers,
        top,
        top_before_distinct: _,
        projection,
        exclude,
        into,
        from,
        lateral_views,
        prewhere,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having,
        named_window,
        qualify,
        window_before_qualify: _,
        value_table_mode,
        flavor,
    } = select;

    // No GROUP BY parses as an empty `Expressions`, not as a None.
    let has_group_by = !matches!(
        &group_by,
        GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty()
    );
    let unsupported = [
        ("optimizer hints", !optimizer_hints.is_empty()),
        ("DISTINCT", distinct.is_some()),
        ("SELECT modifiers", select_modifiers.is_some()),
        ("TOP", top.is_some()),
        ("SELECT * EXCLUDE", exclude.is_some()),
        ("SELECT INTO", into.is_some()),
        ("LATERAL VIEW", !lateral_views.is_empty()),
        ("PREWHERE", prewhere.is_some()),
        ("WHERE", selection.is_some()),
        ("CONNECT BY", !connect_by.is_empty()),
        ("GROUP BY", has_group_by),
        ("CLUSTER BY", !cluster_by.is_empty()),
        ("DISTRIBUTE BY", !distribute_by.is_empty()),
        ("SORT BY", !sort_by.is_empty()),
        ("HAVING", having.is_some()),
        ("WINDOW", !named_window.is_empty()),
        ("QUALIFY", qualify.is_some()),
        ("SELECT AS VALUE/STRUCT", value_table_mode.is_some()),
        (
            "FROM-first syntax",
            !matches!(flavor, SelectFlavor::Standard),
        ),
    ];
    reject_present(&unsupported)?;

    let input = bind_from(from, catalog)?;
    let exprs = bind_projection(projection, &input.schema())?;
    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs,
    })
}

fn bind_from(from: Vec<TableWithJoins>, catalog: &dyn Catalog) -> EngineResult<LogicalPlan> {
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

fn bind_projection(
    projection: Vec<SelectItem>,
    schema: &Schema,
) -> EngineResult<Vec<(Expr, String)>> {
    let mut exprs = Vec::with_capacity(projection.len());
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let expr = bind_expr(expr, schema)?;
                let name = output_name(&expr);
                exprs.push((expr, name));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                exprs.push((bind_expr(expr, schema)?, normalize(&alias)));
            }
            SelectItem::Wildcard(options) => {
                check_wildcard_options(options)?;
                for (index, field) in schema.fields().iter().enumerate() {
                    let column = Expr::Column {
                        index,
                        name: field.name().clone(),
                    };
                    exprs.push((column, field.name().clone()));
                }
            }
            SelectItem::QualifiedWildcard(..) => return Err(not_supported("table.*")),
            SelectItem::ExprWithAliases { .. } => {
                return Err(not_supported("multiple aliases for one expression"));
            }
        }
    }
    Ok(exprs)
}

fn check_wildcard_options(options: WildcardAdditionalOptions) -> EngineResult<()> {
    let WildcardAdditionalOptions {
        wildcard_token: _,
        opt_ilike,
        opt_exclude,
        opt_except,
        opt_replace,
        opt_rename,
        opt_alias,
    } = options;
    let unsupported = [
        ("* ILIKE", opt_ilike.is_some()),
        ("* EXCLUDE", opt_exclude.is_some()),
        ("* EXCEPT", opt_except.is_some()),
        ("* REPLACE", opt_replace.is_some()),
        ("* RENAME", opt_rename.is_some()),
        ("* AS", opt_alias.is_some()),
    ];
    reject_present(&unsupported)
}

fn output_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
    }
}

fn bind_expr(expr: ast::Expr, input: &Schema) -> EngineResult<Expr> {
    match expr {
        ast::Expr::Identifier(ident) => bind_column(&ident, input),
        other => Err(not_supported(&format!("the expression {other}"))),
    }
}

fn bind_column(ident: &Ident, input: &Schema) -> EngineResult<Expr> {
    let name = normalize(ident);
    let index = input
        .fields()
        .iter()
        .position(|f| *f.name() == name)
        .ok_or_else(|| EngineError::Bind(format!("column {name} does not exist")))?;
    Ok(Expr::Column {
        index,
        name: input.field(index).name().clone(),
    })
}

/// Unquoted names fold to lowercase; quoted names keep their case.
fn normalize(ident: &Ident) -> String {
    match ident.quote_style {
        None => ident.value.to_lowercase(),
        Some(_) => ident.value.clone(),
    }
}

fn reject_present(clauses: &[(&str, bool)]) -> EngineResult<()> {
    match clauses.iter().find(|(_, present)| *present) {
        Some((clause, _)) => Err(not_supported(clause)),
        None => Ok(()),
    }
}

fn not_supported(what: &str) -> EngineError {
    EngineError::NotSupported(format!("{what} is not supported yet"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use storage::table::Snapshot;

    use super::*;
    use crate::catalog::MemoryCatalog;

    /// Binds `sql` against a catalog holding `people(id Int64, name Utf8View)`.
    fn bind_sql(sql: &str) -> EngineResult<LogicalPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let mut catalog = MemoryCatalog::new();
        catalog.register(
            "people",
            Snapshot {
                version: 0,
                files: vec![],
                schema,
            },
        );
        let statements = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        bind(statements, &catalog)
    }

    fn plan_text(sql: &str) -> String {
        bind_sql(sql).unwrap().to_string()
    }

    fn not_supported_message(sql: &str) -> String {
        match bind_sql(sql) {
            Err(EngineError::NotSupported(msg)) => msg,
            other => panic!("expected NotSupported for {sql:?}, got {other:?}"),
        }
    }

    fn bind_error_message(sql: &str) -> String {
        match bind_sql(sql) {
            Err(EngineError::Bind(msg)) => msg,
            other => panic!("expected a Bind error for {sql:?}, got {other:?}"),
        }
    }

    #[test]
    fn binds_columns_in_select_order() {
        assert_eq!(
            plan_text("select name, id from people"),
            "Project: #1 name, #0 id\n  Scan: people\n"
        );
    }

    #[test]
    fn wildcard_expands_to_every_column_in_schema_order() {
        assert_eq!(
            plan_text("select * from people"),
            "Project: #0 id, #1 name\n  Scan: people\n"
        );
    }

    #[test]
    fn alias_names_the_output_column_and_is_case_folded() {
        assert_eq!(
            plan_text("select id as Key from people"),
            "Project: #0 id AS key\n  Scan: people\n"
        );
    }

    #[test]
    fn unquoted_names_are_case_insensitive() {
        assert_eq!(
            plan_text("SELECT NAME FROM People"),
            "Project: #1 name\n  Scan: people\n"
        );
    }

    #[test]
    fn quoted_names_keep_their_case() {
        assert_eq!(
            plan_text(r#"select "name" from "people""#),
            "Project: #1 name\n  Scan: people\n"
        );
        assert!(bind_error_message(r#"select id from "People""#).contains("People"));
        assert!(bind_error_message(r#"select "Name" from people"#).contains("Name"));
    }

    #[test]
    fn unknown_names_are_bind_errors_that_name_them() {
        assert!(bind_error_message("select nme from people").contains("nme"));
        assert!(bind_error_message("select id from persons").contains("persons"));
    }

    #[test]
    fn unsupported_clauses_are_rejected_by_name() {
        let cases = [
            ("select id from people where id = 1", "WHERE"),
            ("select distinct id from people", "DISTINCT"),
            ("select id from people group by id", "GROUP BY"),
            ("select id from people order by id", "ORDER BY"),
            ("select id from people limit 1", "LIMIT"),
            ("with p as (select id from people) select id from p", "WITH"),
            ("select id from people union select id from people", "UNION"),
        ];
        for (sql, clause) in cases {
            let msg = not_supported_message(sql);
            assert!(msg.contains(clause), "{sql:?} gave {msg:?}");
        }
    }

    #[test]
    fn unsupported_from_items_are_rejected() {
        let cases = [
            ("select 1", "without FROM"),
            ("select id from people, people", "more than one table"),
            ("select id from people join people on true", "JOIN"),
            ("select id from people p", "alias"),
            ("select id from myschema.people", "qualified"),
            (
                "select id from (select id from people)",
                "other than a table",
            ),
        ];
        for (sql, expected) in cases {
            let msg = not_supported_message(sql);
            assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
        }
    }

    #[test]
    fn unsupported_select_items_are_rejected() {
        let cases = [
            ("select id + 1 from people", "expression"),
            ("select people.* from people", "table.*"),
            ("select * exclude (id) from people", "EXCLUDE"),
        ];
        for (sql, expected) in cases {
            let msg = not_supported_message(sql);
            assert!(msg.contains(expected), "{sql:?} gave {msg:?}");
        }
    }

    #[test]
    fn only_a_single_query_statement_is_accepted() {
        assert!(
            not_supported_message("select id from people; select id from people")
                .contains("2 statements")
        );
        assert!(not_supported_message("delete from people").contains("other than SELECT"));
    }
}
