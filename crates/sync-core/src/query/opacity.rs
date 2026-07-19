use crate::error::EngineError;
use crate::schema::{ColumnUse, Tables};

use super::ast::{Ast, Condition, CorrelatedSubquery, RightVal, ValueRef};

pub fn validate_encrypted_column_usage(tables: &Tables, ast: &Ast) -> Result<(), EngineError> {
    validate_ast(tables, ast)
}

fn validate_ast(tables: &Tables, ast: &Ast) -> Result<(), EngineError> {
    if let Some(condition) = &ast.where_ {
        validate_condition(tables, &ast.table, condition)?;
    }

    let ordering_use = if ast.start.is_some() {
        ColumnUse::Cursor
    } else {
        ColumnUse::Order
    };
    for order in &ast.order_by {
        tables.validate_column_usage(&ast.table, &order.column, ordering_use)?;
    }

    for related in &ast.related {
        validate_related(tables, &ast.table, related)?;
    }
    Ok(())
}

fn validate_condition(
    tables: &Tables,
    table: &str,
    condition: &Condition,
) -> Result<(), EngineError> {
    match condition {
        Condition::Simple { left, right, .. } => {
            if let ValueRef::Column(column) = left {
                tables.validate_column_usage(table, column, ColumnUse::Predicate)?;
            }
            if let RightVal::Column(column) = right {
                tables.validate_column_usage(table, column, ColumnUse::Predicate)?;
            }
        }
        Condition::And(conditions) | Condition::Or(conditions) => {
            for condition in conditions {
                validate_condition(tables, table, condition)?;
            }
        }
        Condition::Exists { related, .. } => validate_related(tables, table, related)?,
    }
    Ok(())
}

fn validate_related(
    tables: &Tables,
    parent_table: &str,
    related: &CorrelatedSubquery,
) -> Result<(), EngineError> {
    for parent_field in &related.parent_field {
        tables.validate_column_usage(parent_table, parent_field, ColumnUse::Correlation)?;
    }
    for child_field in &related.child_field {
        tables.validate_column_usage(
            &related.subquery.table,
            child_field,
            ColumnUse::Correlation,
        )?;
    }
    validate_ast(tables, &related.subquery)
}
