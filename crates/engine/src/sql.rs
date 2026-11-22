//! Binds sqlparser's AST into a `LogicalPlan`.
use arrow::datatypes::{DataType, Schema};
use sqlparser::ast::{
    self, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectFlavor, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins, WildcardAdditionalOptions,
};

use crate::catalog::Catalog;
use crate::error::{EngineError, EngineResult};
use crate::expr::{BinaryOp, Expr, ScalarValue};
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

    let scan = bind_from(from, catalog)?;
    let input = match selection {
        Some(predicate) => {
            let schema = scan.schema();
            let predicate = bind_expr(predicate, &schema)?;
            require_boolean(&predicate, &schema, "WHERE")?;
            LogicalPlan::Filter {
                input: Box::new(scan),
                predicate,
            }
        }
        None => scan,
    };
    let exprs = bind_projection(projection, &input.schema())?;

    Ok(LogicalPlan::Project {
        input: Box::new(input),
        exprs,
    })
}

fn require_boolean(expr: &Expr, schema: &Schema, clause: &str) -> EngineResult<()> {
    let datatype = expr.data_type(schema);
    if datatype != DataType::Boolean {
        Err(EngineError::Bind(format!(
            "{clause} needs boolean expression, got {expr} of type {datatype}",
        )))
    } else {
        Ok(())
    }
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
        _ => expr.to_string(),
    }
}
fn bind_expr(expr: ast::Expr, input: &Schema) -> EngineResult<Expr> {
    match expr {
        ast::Expr::Identifier(ident) => bind_column(&ident, input),
        ast::Expr::Value(value) => bind_literal(value.into()),
        ast::Expr::BinaryOp { left, op, right } => bind_binary(*left, op, *right, input),
        ast::Expr::Nested(inner) => bind_expr(*inner, input),
        ast::Expr::UnaryOp {
            op: ast::UnaryOperator::Not,
            expr,
        } => {
            let expr = bind_expr(*expr, input)?;
            require_boolean(&expr, input, "NOT")?;
            Ok(Expr::Not(Box::new(expr)))
        }
        ast::Expr::IsNull(e) => {
            let e = bind_expr(*e, input)?;
            Ok(Expr::IsNull {
                expr: Box::new(e),
                negated: false,
            })
        }
        ast::Expr::IsNotNull(e) => {
            let e = bind_expr(*e, input)?;
            Ok(Expr::IsNull {
                expr: Box::new(e),
                negated: true,
            })
        }
        ast::Expr::In {
            expr,
            target: ast::InTarget::List(items),
            negated,
            global: false,
        } => {
            let expr = bind_expr(*expr, input)?;
            let items: Vec<Expr> = items
                .into_iter()
                .map(|item| bind_expr(item, input))
                .collect::<EngineResult<Vec<_>>>()?;
            for item in &items {
                require_same_type(&expr, item, input, "IN")?;
            }
            Ok(Expr::InList {
                expr: Box::new(expr),
                list: items,
                negated,
            })
        }
        ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => bind_between(*expr, negated, *low, *high, input),
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

fn bind_literal(value: ast::Value) -> EngineResult<Expr> {
    match value {
        ast::Value::Number(s, _) => {
            let n = s.parse::<i64>().map_err(|_| {
                not_supported(&format!("the literal {s} (only integers are supported)"))
            })?;
            Ok(Expr::Literal(ScalarValue::Int64(n)))
        }
        ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(ScalarValue::Utf8(s))),
        ast::Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(b))),
        other => Err(not_supported(&format!("the literal {other}"))),
    }
}

fn bind_binary(
    left: ast::Expr,
    op: ast::BinaryOperator,
    right: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
    let left = bind_expr(left, input)?;
    let right = bind_expr(right, input)?;
    let op = match op {
        ast::BinaryOperator::Eq => BinaryOp::Eq,
        ast::BinaryOperator::NotEq => BinaryOp::NotEq,
        ast::BinaryOperator::Lt => BinaryOp::Lt,
        ast::BinaryOperator::LtEq => BinaryOp::LtEq,
        ast::BinaryOperator::Gt => BinaryOp::Gt,
        ast::BinaryOperator::GtEq => BinaryOp::GtEq,
        ast::BinaryOperator::And => BinaryOp::And,
        ast::BinaryOperator::Or => BinaryOp::Or,
        other => return Err(not_supported(&format!("the binary operator {other}"))),
    };
    match op {
        BinaryOp::And | BinaryOp::Or => {
            require_boolean(&left, input, &op.to_string())?;
            require_boolean(&right, input, &op.to_string())?;
        }
        _ => {
            require_same_type(&left, &right, input, &op.to_string())?;
        }
    }
    Ok(Expr::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn require_same_type(left: &Expr, right: &Expr, input: &Schema, context: &str) -> EngineResult<()> {
    let l = left.data_type(input);
    let r = right.data_type(input);
    if l != r {
        Err(not_supported(&format!(
            "the {context} comparison between two different types unsupported, got {left} of type {l} and {right} of type {r}"
        )))
    } else {
        Ok(())
    }
}

fn bind_between(
    expr: ast::Expr,
    negated: bool,
    low: ast::Expr,
    high: ast::Expr,
    input: &Schema,
) -> EngineResult<Expr> {
    let expr = bind_expr(expr, input)?;
    let low = bind_expr(low, input)?;
    let high = bind_expr(high, input)?;
    require_same_type(&expr, &low, input, "BETWEEN")?;
    require_same_type(&expr, &high, input, "BETWEEN")?;
    let (low_op, join_op, high_op) = if negated {
        (BinaryOp::Lt, BinaryOp::Or, BinaryOp::Gt)
    } else {
        (BinaryOp::GtEq, BinaryOp::And, BinaryOp::LtEq)
    };

    Ok(Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(expr.clone()),
            op: low_op,
            right: Box::new(low),
        }),
        op: join_op,
        right: Box::new(Expr::Binary {
            left: Box::new(expr),
            op: high_op,
            right: Box::new(high),
        }),
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
            ("select id from people having id = 1", "HAVING"),
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
            ("select id + 1 from people", "+"),
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

    /// The plan text of `select name from people where <predicate>`, without the projection
    /// and scan lines.
    fn filter_text(predicate: &str) -> String {
        let plan = plan_text(&format!("select name from people where {predicate}"));
        let filter = plan
            .lines()
            .nth(1)
            .unwrap_or_else(|| panic!("no Filter line in:\n{plan}"));
        filter
            .trim()
            .strip_prefix("Filter: ")
            .unwrap_or_else(|| panic!("second line is not a Filter:\n{plan}"))
            .to_string()
    }

    fn where_not_supported(predicate: &str) -> String {
        not_supported_message(&format!("select name from people where {predicate}"))
    }

    fn where_bind_error(predicate: &str) -> String {
        bind_error_message(&format!("select name from people where {predicate}"))
    }

    #[test]
    fn where_becomes_a_filter_between_scan_and_project() {
        // `id` is filtered on but not selected, so the filter must sit below the projection.
        assert_eq!(
            plan_text("select name from people where id = 1"),
            "Project: #1 name\n  Filter: #0 id = 1\n    Scan: people\n"
        );
    }

    #[test]
    fn binds_every_comparison_operator() {
        let cases = [
            ("id = 1", "#0 id = 1"),
            ("id <> 1", "#0 id <> 1"),
            ("id != 1", "#0 id <> 1"),
            ("id < 1", "#0 id < 1"),
            ("id <= 1", "#0 id <= 1"),
            ("id > 1", "#0 id > 1"),
            ("id >= 1", "#0 id >= 1"),
            ("1 = id", "1 = #0 id"),
        ];
        for (sql, expected) in cases {
            assert_eq!(filter_text(sql), expected, "WHERE {sql}");
        }
    }

    #[test]
    fn binds_string_and_boolean_literals() {
        assert_eq!(filter_text("name = 'bob'"), "#1 name = 'bob'");
        assert_eq!(filter_text("name = 'it''s'"), "#1 name = 'it''s'");
        assert_eq!(filter_text("id = 1 and true"), "#0 id = 1 AND true");
    }

    #[test]
    fn and_or_keep_sql_precedence() {
        // AND binds tighter than OR, so no parentheses are needed or added.
        assert_eq!(
            filter_text("id = 1 or id = 2 and id = 3"),
            "#0 id = 1 OR #0 id = 2 AND #0 id = 3"
        );
        assert_eq!(
            filter_text("(id = 1 or id = 2) and id = 3"),
            "(#0 id = 1 OR #0 id = 2) AND #0 id = 3"
        );
    }

    #[test]
    fn redundant_parentheses_disappear() {
        assert_eq!(filter_text("((id = 1))"), "#0 id = 1");
        assert_eq!(filter_text("(id) = (1)"), "#0 id = 1");
    }

    #[test]
    fn binds_not() {
        // NOT binds more loosely than `=`, so both spellings are NOT applied to the comparison.
        assert_eq!(filter_text("not id = 1"), "NOT (#0 id = 1)");
        assert_eq!(filter_text("not (id = 1)"), "NOT (#0 id = 1)");
    }

    #[test]
    fn binds_null_tests() {
        assert_eq!(filter_text("name is null"), "#1 name IS NULL");
        assert_eq!(filter_text("name is not null"), "#1 name IS NOT NULL");
    }

    #[test]
    fn binds_in_lists() {
        assert_eq!(filter_text("id in (1, 2, 3)"), "#0 id IN (1, 2, 3)");
        assert_eq!(filter_text("id not in (1, 2)"), "#0 id NOT IN (1, 2)");
        assert_eq!(filter_text("name in ('a', 'b')"), "#1 name IN ('a', 'b')");
    }

    #[test]
    fn between_becomes_two_comparisons() {
        assert_eq!(
            filter_text("id between 5 and 7"),
            "#0 id >= 5 AND #0 id <= 7"
        );
    }

    #[test]
    fn not_between_becomes_an_or_storage_can_prune() {
        assert_eq!(
            filter_text("id not between 5 and 7"),
            "#0 id < 5 OR #0 id > 7"
        );
    }

    #[test]
    fn between_inside_and_keeps_its_meaning() {
        // NOT BETWEEN's OR must stay grouped when it sits under an AND.
        assert_eq!(
            filter_text("id not between 5 and 7 and name is null"),
            "(#0 id < 5 OR #0 id > 7) AND #1 name IS NULL"
        );
    }

    #[test]
    fn where_must_be_boolean() {
        where_bind_error("id");
        where_bind_error("name");
        where_bind_error("1");
    }

    #[test]
    fn logical_operators_need_boolean_operands() {
        where_bind_error("id and id = 1");
        where_bind_error("id = 1 or name");
        where_bind_error("not id");
    }

    #[test]
    fn comparing_different_types_is_not_supported_yet() {
        where_not_supported("name = 1");
        where_not_supported("id = 'a'");
        where_not_supported("id in (1, 'a')");
        where_not_supported("id between 1 and 'z'");
    }

    #[test]
    fn unsupported_literals_are_rejected() {
        where_not_supported("id = 1.5");
        where_not_supported("id = 1e3");
        where_not_supported("name = null");
        // Larger than i64::MAX.
        where_not_supported("id = 99999999999999999999");
    }

    #[test]
    fn unsupported_predicates_are_rejected() {
        where_not_supported("name like 'a%'");
        where_not_supported("id in (select id from people)");
        where_not_supported("id + 1 = 2");
        where_not_supported("id is true");
    }

    #[test]
    fn unknown_column_in_where_is_a_bind_error() {
        assert!(where_bind_error("nme = 'bob'").contains("nme"));
    }
}
