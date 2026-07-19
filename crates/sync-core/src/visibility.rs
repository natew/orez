use serde::Deserialize;
use serde_json::Value;

use crate::db::SqlValue;
use crate::error::EngineError;
use crate::pull::VisibleFilter;
use crate::schema::{ColumnUse, Tables, is_valid_identifier, quote_ident};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum VisibilityExpression {
    Column {
        table: String,
        column: String,
        qualifier: Option<String>,
    },
    Value {
        value: Value,
    },
    Comparison {
        operator: String,
        left: Box<VisibilityExpression>,
        right: Box<VisibilityExpression>,
    },
    And {
        conditions: Vec<VisibilityExpression>,
    },
    Or {
        conditions: Vec<VisibilityExpression>,
    },
    Exists {
        table: String,
        qualifier: Option<String>,
        #[serde(rename = "where")]
        where_: Box<VisibilityExpression>,
    },
}

struct Scope {
    table: String,
    qualifier: String,
}

struct Compiler<'a> {
    tables: &'a Tables,
    scopes: Vec<Scope>,
    params: Vec<SqlValue>,
}

impl Compiler<'_> {
    fn compile_condition(
        &mut self,
        expression: &VisibilityExpression,
    ) -> Result<String, EngineError> {
        match expression {
            VisibilityExpression::Comparison {
                operator,
                left,
                right,
            } => {
                let operator = match operator.as_str() {
                    "=" | "!=" | "<" | ">" | "<=" | ">=" | "IS" | "IS NOT" => operator.as_str(),
                    _ => {
                        return Err(EngineError::bad_request(format!(
                            "unsupported visibility comparison operator '{operator}'"
                        )));
                    }
                };
                let left = self.compile_operand(left)?;
                let right = self.compile_operand(right)?;
                Ok(format!("{left} {operator} {right}"))
            }
            VisibilityExpression::And { conditions } => self.compile_junction("AND", conditions),
            VisibilityExpression::Or { conditions } => self.compile_junction("OR", conditions),
            VisibilityExpression::Exists {
                table,
                qualifier,
                where_,
            } => self.compile_exists(table, qualifier.as_deref(), where_),
            VisibilityExpression::Column { .. } | VisibilityExpression::Value { .. } => Err(
                EngineError::bad_request("visibility condition must be a comparison or predicate"),
            ),
        }
    }

    fn compile_junction(
        &mut self,
        operator: &str,
        conditions: &[VisibilityExpression],
    ) -> Result<String, EngineError> {
        if conditions.is_empty() {
            return Err(EngineError::bad_request(format!(
                "visibility {operator} requires at least one condition"
            )));
        }
        let conditions = conditions
            .iter()
            .map(|condition| self.compile_condition(condition))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("({})", conditions.join(&format!(" {operator} "))))
    }

    fn compile_exists(
        &mut self,
        table: &str,
        qualifier: Option<&str>,
        where_: &VisibilityExpression,
    ) -> Result<String, EngineError> {
        let logical_table = self
            .tables
            .logical_name(table)
            .ok_or_else(|| EngineError::bad_request(format!("unknown table '{table}'")))?
            .to_string();
        let qualifier = qualifier.unwrap_or(&logical_table);
        if !is_valid_identifier(qualifier) {
            return Err(EngineError::bad_request(format!(
                "visibility qualifier '{qualifier}' is not a valid identifier"
            )));
        }
        if self
            .scopes
            .iter()
            .any(|scope| scope.qualifier.eq_ignore_ascii_case(qualifier))
        {
            return Err(EngineError::bad_request(format!(
                "visibility qualifier '{qualifier}' is already in scope"
            )));
        }
        self.scopes.push(Scope {
            table: logical_table.clone(),
            qualifier: qualifier.to_string(),
        });
        let predicate = self.compile_condition(where_);
        self.scopes.pop();
        Ok(format!(
            "EXISTS (SELECT 1 FROM {} AS {} WHERE {})",
            quote_ident(&logical_table),
            quote_ident(qualifier),
            predicate?
        ))
    }

    fn compile_operand(
        &mut self,
        expression: &VisibilityExpression,
    ) -> Result<String, EngineError> {
        match expression {
            VisibilityExpression::Column {
                table,
                column,
                qualifier,
            } => {
                let resolved =
                    self.tables
                        .validate_column_usage(table, column, ColumnUse::Visibility)?;
                let scope = self.scopes.iter().rev().find(|scope| {
                    scope.table == resolved.logical_table
                        && qualifier
                            .as_ref()
                            .is_none_or(|qualifier| scope.qualifier.eq_ignore_ascii_case(qualifier))
                });
                let scope = scope.ok_or_else(|| {
                    EngineError::bad_request(format!(
                        "visibility column '{}.{}' is not in scope",
                        resolved.logical_table, resolved.logical_column
                    ))
                })?;
                Ok(format!(
                    "{}.{}",
                    quote_ident(&scope.qualifier),
                    quote_ident(resolved.logical_column)
                ))
            }
            VisibilityExpression::Value { value } => {
                self.params.push(scalar(value)?);
                Ok("?".to_string())
            }
            VisibilityExpression::Comparison { .. }
            | VisibilityExpression::And { .. }
            | VisibilityExpression::Or { .. }
            | VisibilityExpression::Exists { .. } => Err(EngineError::bad_request(
                "visibility comparison operands must be columns or scalar values",
            )),
        }
    }
}

fn scalar(value: &Value) -> Result<SqlValue, EngineError> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(value) => Ok(SqlValue::Integer(i64::from(*value))),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqlValue::Integer(value))
            } else {
                value
                    .as_f64()
                    .map(SqlValue::Real)
                    .ok_or_else(|| EngineError::bad_request("visibility value is not finite"))
            }
        }
        Value::String(value) => Ok(SqlValue::Text(value.clone())),
        _ => Err(EngineError::bad_request(format!(
            "visibility value must be a scalar, got {value}"
        ))),
    }
}

pub fn compile_visibility_filter(
    tables: &Tables,
    root_table: &str,
    expression: &VisibilityExpression,
) -> Result<VisibleFilter, EngineError> {
    let root_table = tables
        .logical_name(root_table)
        .ok_or_else(|| EngineError::bad_request(format!("unknown table '{root_table}'")))?
        .to_string();
    let mut compiler = Compiler {
        tables,
        scopes: vec![Scope {
            qualifier: root_table.clone(),
            table: root_table,
        }],
        params: Vec::new(),
    };
    let sql = compiler.compile_condition(expression)?;
    Ok(VisibleFilter {
        sql,
        params: compiler.params,
    })
}
