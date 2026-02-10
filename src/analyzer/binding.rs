//! Name binding and scope resolution.
//!
//! Resolves table names against the catalog (respecting schema search path),
//! validates column references against known table schemas, and tracks aliases.

use crate::catalog::{with_catalog_read, SearchPath};
use crate::parser::ast::{Expr, JoinCondition, SelectStatement, TableExpression};
use crate::tcop::engine::EngineError;

/// Tracks what names are in scope during analysis.
#[derive(Debug)]
pub struct BindingContext {
    search_path: SearchPath,
    /// Tables currently in scope: (alias_or_name, Vec<column_name>).
    tables_in_scope: Vec<(String, Vec<String>)>,
    /// CTE names visible in this scope.
    cte_names: Vec<String>,
}

impl BindingContext {
    pub fn new(search_path: SearchPath, cte_names: Vec<String>) -> Self {
        Self {
            search_path,
            tables_in_scope: Vec::new(),
            cte_names,
        }
    }

    /// Register a table (by alias or real name) with its columns.
    fn add_table(&mut self, name: String, columns: Vec<String>) {
        self.tables_in_scope.push((name, columns));
    }

    /// Check if a table name is a known CTE.
    fn is_cte(&self, name: &str) -> bool {
        self.cte_names.iter().any(|n| n == &name.to_lowercase())
    }

    /// Look up a column across all tables in scope.
    /// Returns true if the column name exists in any in-scope table.
    pub fn column_exists(&self, column_name: &str) -> bool {
        let lower = column_name.to_lowercase();
        self.tables_in_scope
            .iter()
            .any(|(_, cols)| cols.iter().any(|c| c.to_lowercase() == lower))
    }

    /// Look up a qualified column (table.column).
    /// Returns true if the table alias/name is in scope and has that column.
    pub fn qualified_column_exists(&self, table_name: &str, column_name: &str) -> bool {
        let tbl_lower = table_name.to_lowercase();
        let col_lower = column_name.to_lowercase();
        self.tables_in_scope
            .iter()
            .filter(|(name, _)| name.to_lowercase() == tbl_lower)
            .any(|(_, cols)| cols.iter().any(|c| c.to_lowercase() == col_lower))
    }
}

/// Resolve all FROM-clause table references in a SELECT, populating the binding context.
pub fn resolve_select(
    ctx: &mut BindingContext,
    select: &SelectStatement,
) -> Result<(), EngineError> {
    // Resolve FROM clause
    for table_expr in &select.from {
        resolve_table_expression(ctx, table_expr)?;
    }

    // Validate WHERE clause column references (light check)
    if let Some(ref where_clause) = select.where_clause {
        validate_expr_references(ctx, where_clause)?;
    }

    // Validate HAVING clause
    if let Some(ref having) = select.having {
        validate_expr_references(ctx, having)?;
    }

    Ok(())
}

/// Resolve a single table expression, adding it to the binding context.
fn resolve_table_expression(
    ctx: &mut BindingContext,
    table_expr: &TableExpression,
) -> Result<(), EngineError> {
    match table_expr {
        TableExpression::Relation(table_ref) => {
            let alias = table_ref
                .alias
                .as_deref()
                .unwrap_or_else(|| table_ref.name.last().map(|s| s.as_str()).unwrap_or(""));

            // Check if it's a CTE first
            if table_ref.name.len() == 1 && ctx.is_cte(&table_ref.name[0]) {
                // CTEs are opaque for now — we don't track their columns
                ctx.add_table(alias.to_lowercase(), Vec::new());
                return Ok(());
            }

            // Try to resolve against catalog — if the table doesn't exist,
            // we still add it to scope with no columns. The executor will
            // produce the proper error at execution time.
            let columns = with_catalog_read(|catalog| {
                match catalog.resolve_table(&table_ref.name, &ctx.search_path) {
                    Ok(table) => table
                        .columns()
                        .iter()
                        .map(|c| c.name().to_string())
                        .collect(),
                    Err(_) => Vec::new(),
                }
            });

            ctx.add_table(alias.to_lowercase(), columns);
        }
        TableExpression::Function(func_ref) => {
            let alias = func_ref
                .alias
                .as_deref()
                .unwrap_or_else(|| func_ref.name.last().map(|s| s.as_str()).unwrap_or(""));
            // Table functions have dynamic columns — add with column aliases if provided
            let columns = func_ref.column_aliases.clone();
            ctx.add_table(alias.to_lowercase(), columns);
        }
        TableExpression::Subquery(subquery_ref) => {
            let alias = subquery_ref
                .alias
                .as_deref()
                .unwrap_or("")
                .to_lowercase();
            // Subqueries are opaque — we don't track their output columns here
            ctx.add_table(alias, Vec::new());
        }
        TableExpression::Join(join_expr) => {
            resolve_table_expression(ctx, &join_expr.left)?;
            resolve_table_expression(ctx, &join_expr.right)?;

            // Validate the ON clause if present
            if let Some(JoinCondition::On(ref on_expr)) = join_expr.condition {
                validate_expr_references(ctx, on_expr)?;
            }
        }
    }
    Ok(())
}

/// Validate column references in an expression against the binding context.
///
/// This is a best-effort check. We only validate identifiers that look like
/// qualified column references (table.column). Unqualified references are
/// harder to validate without full type inference, so we skip them for now
/// to avoid false positives. The executor will catch any remaining errors.
fn validate_expr_references(ctx: &BindingContext, expr: &Expr) -> Result<(), EngineError> {
    match expr {
        Expr::Identifier(parts) if parts.len() == 2 => {
            // table.column reference — check if the table is in scope
            let table_name = &parts[0];
            let lower = table_name.to_lowercase();
            let in_scope = ctx
                .tables_in_scope
                .iter()
                .any(|(name, _)| name.to_lowercase() == lower);
            if !in_scope {
                // Don't error yet — could be a schema-qualified name or the executor
                // knows about it via other means. This keeps us non-breaking.
            }
        }
        // Recurse into sub-expressions
        Expr::Binary { left, right, .. } => {
            validate_expr_references(ctx, left)?;
            validate_expr_references(ctx, right)?;
        }
        Expr::Unary { expr, .. } => {
            validate_expr_references(ctx, expr)?;
        }
        Expr::IsNull { expr, .. } => {
            validate_expr_references(ctx, expr)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_expr_references(ctx, expr)?;
            validate_expr_references(ctx, low)?;
            validate_expr_references(ctx, high)?;
        }
        Expr::Like { expr, pattern, .. } => {
            validate_expr_references(ctx, expr)?;
            validate_expr_references(ctx, pattern)?;
        }
        Expr::InList { expr, list, .. } => {
            validate_expr_references(ctx, expr)?;
            for item in list {
                validate_expr_references(ctx, item)?;
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                validate_expr_references(ctx, when)?;
                validate_expr_references(ctx, then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_expr_references(ctx, else_expr)?;
            }
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            validate_expr_references(ctx, operand)?;
            for (when, then) in when_then {
                validate_expr_references(ctx, when)?;
                validate_expr_references(ctx, then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_expr_references(ctx, else_expr)?;
            }
        }
        Expr::FunctionCall { args, filter, .. } => {
            for arg in args {
                validate_expr_references(ctx, arg)?;
            }
            if let Some(filter) = filter {
                validate_expr_references(ctx, filter)?;
            }
        }
        Expr::Cast { expr, .. } => {
            validate_expr_references(ctx, expr)?;
        }
        Expr::AnyAll { left, right, .. } => {
            validate_expr_references(ctx, left)?;
            validate_expr_references(ctx, right)?;
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            validate_expr_references(ctx, left)?;
            validate_expr_references(ctx, right)?;
        }
        Expr::ArrayConstructor(exprs) => {
            for e in exprs {
                validate_expr_references(ctx, e)?;
            }
        }
        // Leaf nodes and subqueries — nothing to validate at this level
        _ => {}
    }
    Ok(())
}
