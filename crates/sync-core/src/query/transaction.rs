use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::db::SqlValue;
use crate::error::EngineError;
use crate::schema::quote_ident;
use crate::value::ZeroColumnType;

use super::ast::{
    Ast, Condition, CorrelatedSubquery, OrderPart, RightVal, Scalar, SimpleOp, ValueRef,
};

#[derive(Debug, Clone)]
pub struct QueryColumn {
    pub name: String,
    pub column_type: ZeroColumnType,
}

#[derive(Debug, Clone)]
struct QueryColumnSpec {
    logical_name: String,
    physical_name: String,
    column_type: ZeroColumnType,
}

#[derive(Debug, Clone)]
struct QueryTableSpec {
    physical_name: String,
    columns: Vec<QueryColumnSpec>,
    primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QuerySchema(Vec<(String, QueryTableSpec)>);

impl QuerySchema {
    fn table(&self, name: &str) -> Result<&QueryTableSpec, EngineError> {
        self.0
            .iter()
            .find(|(logical_name, _)| logical_name == name)
            .map(|(_, table)| table)
            .ok_or_else(|| reject(format!("unknown table '{name}'")))
    }

    fn column<'a>(&'a self, table: &str, column: &str) -> Result<&'a QueryColumnSpec, EngineError> {
        self.table(table)?
            .columns
            .iter()
            .find(|spec| spec.logical_name == column)
            .ok_or_else(|| reject(format!("unknown column '{table}.{column}'")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryFormat {
    pub singular: bool,
    pub relationships: BTreeMap<String, QueryFormat>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryBinding {
    Literal(SqlValue),
    ParentField(String),
}

#[derive(Debug, Clone)]
pub struct CompiledRelationship {
    pub name: String,
    pub node: CompiledQueryNode,
}

#[derive(Debug, Clone)]
pub struct CompiledQueryNode {
    pub table: String,
    pub singular: bool,
    pub sql: String,
    pub bindings: Vec<QueryBinding>,
    pub columns: Vec<QueryColumn>,
    pub relationships: Vec<CompiledRelationship>,
}

#[derive(Debug, Clone)]
pub struct CompiledQueryPlan {
    pub root_table: String,
    pub plan_hash: String,
    pub root: CompiledQueryNode,
}

fn reject(message: impl Into<String>) -> EngineError {
    EngineError::bad_request(message)
}

fn object<'a>(
    value: &'a Value,
    context: &str,
) -> Result<&'a serde_json::Map<String, Value>, EngineError> {
    value
        .as_object()
        .ok_or_else(|| reject(format!("{context} must be an object")))
}

fn string_field(
    object: &serde_json::Map<String, Value>,
    field: &str,
    context: &str,
) -> Result<Option<String>, EngineError> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(reject(format!("{context}.{field} must be a string"))),
    }
}

fn valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(character) if character.is_ascii_alphabetic() || character == '_' => {}
        _ => return false,
    }
    chars.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn validate_identifier(name: &str, context: &str) -> Result<(), EngineError> {
    if valid_identifier(name) {
        Ok(())
    } else {
        Err(reject(format!(
            "{context} '{name}' is not a valid identifier"
        )))
    }
}

fn parse_column_type(value: &Value, context: &str) -> Result<ZeroColumnType, EngineError> {
    match value.as_str() {
        Some("string") => Ok(ZeroColumnType::String),
        Some("number") => Ok(ZeroColumnType::Number),
        Some("boolean") => Ok(ZeroColumnType::Boolean),
        Some("json") => Ok(ZeroColumnType::Json),
        Some("null") => Ok(ZeroColumnType::Null),
        Some(column_type) => Err(reject(format!(
            "unsupported schema type '{column_type}' for {context}"
        ))),
        None => Err(reject(format!("{context}.type must be a string"))),
    }
}

pub fn parse_query_schema(value: &Value) -> Result<QuerySchema, EngineError> {
    let schema = object(value, "schema")?;
    let tables = schema
        .get("tables")
        .ok_or_else(|| reject("schema.tables is required"))?;
    let tables = object(tables, "schema.tables")?;
    let mut parsed = Vec::with_capacity(tables.len());
    let mut physical_tables = BTreeSet::new();

    for (logical_name, value) in tables {
        validate_identifier(logical_name, "logical table name")?;
        let table = object(value, &format!("table '{logical_name}'"))?;
        let table_name = string_field(table, "name", &format!("table '{logical_name}'"))?;
        let server_name = string_field(table, "serverName", &format!("table '{logical_name}'"))?;
        let physical_name = server_name
            .or(table_name)
            .unwrap_or_else(|| logical_name.clone());
        validate_identifier(
            &physical_name,
            &format!("physical table for '{logical_name}'"),
        )?;
        if !physical_tables.insert(physical_name.to_ascii_lowercase()) {
            return Err(reject(format!(
                "duplicate physical table mapping '{physical_name}'"
            )));
        }

        let columns_value = table
            .get("columns")
            .ok_or_else(|| reject(format!("table '{logical_name}'.columns is required")))?;
        let columns_value = object(columns_value, &format!("table '{logical_name}'.columns"))?;
        let mut columns = Vec::with_capacity(columns_value.len());
        let mut physical_columns = BTreeSet::new();
        for (column_name, value) in columns_value {
            validate_identifier(column_name, &format!("column in table '{logical_name}'"))?;
            let column = object(value, &format!("column '{logical_name}.{column_name}'"))?;
            let physical_column = string_field(
                column,
                "serverName",
                &format!("column '{logical_name}.{column_name}'"),
            )?
            .unwrap_or_else(|| column_name.clone());
            validate_identifier(
                &physical_column,
                &format!("physical column for '{logical_name}.{column_name}'"),
            )?;
            if !physical_columns.insert(physical_column.to_ascii_lowercase()) {
                return Err(reject(format!(
                    "duplicate physical column mapping '{logical_name}.{physical_column}'"
                )));
            }
            let column_type = parse_column_type(
                column.get("type").ok_or_else(|| {
                    reject(format!(
                        "column '{logical_name}.{column_name}'.type is required"
                    ))
                })?,
                &format!("column '{logical_name}.{column_name}'"),
            )?;
            columns.push(QueryColumnSpec {
                logical_name: column_name.clone(),
                physical_name: physical_column,
                column_type,
            });
        }

        let primary_key = table
            .get("primaryKey")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                reject(format!(
                    "table '{logical_name}'.primaryKey must be an array"
                ))
            })?;
        if primary_key.is_empty() {
            return Err(reject(format!(
                "table '{logical_name}'.primaryKey must not be empty"
            )));
        }
        let mut parsed_primary_key = Vec::with_capacity(primary_key.len());
        let mut seen_primary_key = BTreeSet::new();
        for value in primary_key {
            let column = value.as_str().ok_or_else(|| {
                reject(format!(
                    "table '{logical_name}'.primaryKey entries must be strings"
                ))
            })?;
            if !seen_primary_key.insert(column) {
                return Err(reject(format!(
                    "table '{logical_name}'.primaryKey contains duplicate '{column}'"
                )));
            }
            if !columns.iter().any(|spec| spec.logical_name == column) {
                return Err(reject(format!(
                    "table '{logical_name}'.primaryKey references unknown column '{column}'"
                )));
            }
            parsed_primary_key.push(column.to_string());
        }

        parsed.push((
            logical_name.clone(),
            QueryTableSpec {
                physical_name,
                columns,
                primary_key: parsed_primary_key,
            },
        ));
    }

    Ok(QuerySchema(parsed))
}

pub fn parse_query_format(value: &Value) -> Result<QueryFormat, EngineError> {
    fn parse(value: &Value, path: &str) -> Result<QueryFormat, EngineError> {
        let format = object(value, path)?;
        for key in format.keys() {
            if !matches!(key.as_str(), "singular" | "relationships") {
                return Err(reject(format!("unsupported {path} field '{key}'")));
            }
        }
        let singular = format
            .get("singular")
            .and_then(Value::as_bool)
            .ok_or_else(|| reject(format!("{path}.singular must be a boolean")))?;
        let relationships = format
            .get("relationships")
            .ok_or_else(|| reject(format!("{path}.relationships is required")))?;
        let relationships = object(relationships, &format!("{path}.relationships"))?;
        let relationships = relationships
            .iter()
            .map(|(name, value)| {
                if name.is_empty() {
                    return Err(reject(format!(
                        "{path}.relationships contains an empty name"
                    )));
                }
                Ok((name.clone(), parse(value, &format!("{path}.{name}"))?))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(QueryFormat {
            singular,
            relationships,
        })
    }

    parse(value, "format")
}

fn scalar_to_sql(value: &Scalar) -> SqlValue {
    match value {
        Scalar::Null => SqlValue::Null,
        Scalar::Bool(value) => SqlValue::Integer(if *value { 1 } else { 0 }),
        Scalar::Int(value) => SqlValue::Integer(*value),
        Scalar::Float(value) => SqlValue::Real(*value),
        Scalar::Text(value) => SqlValue::Text(value.clone()),
    }
}

fn binary_operator(operator: &SimpleOp) -> &'static str {
    match operator {
        SimpleOp::Eq => "=",
        SimpleOp::Ne => "!=",
        SimpleOp::Is => "IS",
        SimpleOp::IsNot => "IS NOT",
        SimpleOp::Lt => "<",
        SimpleOp::Gt => ">",
        SimpleOp::Le => "<=",
        SimpleOp::Ge => ">=",
        _ => unreachable!("binary_operator called for a non-binary operator"),
    }
}

pub(crate) fn postgres_like_to_glob(pattern: &str) -> Result<String, EngineError> {
    let mut output = String::with_capacity(pattern.len());
    let mut escaped = false;
    for character in pattern.chars() {
        if escaped {
            match character {
                '*' => output.push_str("[*]"),
                '?' => output.push_str("[?]"),
                '[' => output.push_str("[[]"),
                other => output.push(other),
            }
            escaped = false;
            continue;
        }
        match character {
            '\\' => escaped = true,
            '%' => output.push('*'),
            '_' => output.push('?'),
            '*' => output.push_str("[*]"),
            '?' => output.push_str("[?]"),
            '[' => output.push_str("[[]"),
            other => output.push(other),
        }
    }
    if escaped {
        return Err(reject("LIKE pattern must not end with an escape character"));
    }
    Ok(output)
}

struct SqlCompiler<'a> {
    schema: &'a QuerySchema,
    bindings: Vec<QueryBinding>,
    next_alias: usize,
}

impl<'a> SqlCompiler<'a> {
    fn new(schema: &'a QuerySchema) -> Self {
        Self {
            schema,
            bindings: Vec::new(),
            next_alias: 0,
        }
    }

    fn alias(&mut self) -> String {
        let alias = format!("q{}", self.next_alias);
        self.next_alias += 1;
        alias
    }

    fn column_sql(&self, table: &str, column: &str, alias: &str) -> Result<String, EngineError> {
        let column = self.schema.column(table, column)?;
        Ok(format!(
            "{}.{}",
            quote_ident(alias),
            quote_ident(&column.physical_name)
        ))
    }

    fn compile_condition(
        &mut self,
        condition: &Condition,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        match condition {
            Condition::Simple { op, left, right } => {
                self.compile_simple(op, left, right, table, alias)
            }
            Condition::And(conditions) => {
                self.compile_junction(conditions, "AND", "1", table, alias)
            }
            Condition::Or(conditions) => self.compile_junction(conditions, "OR", "0", table, alias),
            Condition::Exists { negated, related } => {
                self.compile_exists(*negated, related, table, alias)
            }
        }
    }

    fn compile_junction(
        &mut self,
        conditions: &[Condition],
        joiner: &str,
        empty: &str,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        if conditions.is_empty() {
            return Ok(empty.to_string());
        }
        let conditions = conditions
            .iter()
            .map(|condition| self.compile_condition(condition, table, alias))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("({})", conditions.join(&format!(" {joiner} "))))
    }

    fn push_literal(&mut self, value: &Scalar) {
        self.bindings
            .push(QueryBinding::Literal(scalar_to_sql(value)));
    }

    fn compile_simple(
        &mut self,
        operator: &SimpleOp,
        left: &ValueRef,
        right: &RightVal,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        let left = match left {
            ValueRef::Column(column) => self.column_sql(table, column, alias)?,
            ValueRef::Literal(value) => {
                self.push_literal(value);
                "?".to_string()
            }
        };

        match operator {
            SimpleOp::In | SimpleOp::NotIn => {
                let RightVal::List(values) = right else {
                    return Err(reject("IN/NOT IN requires an array operand"));
                };
                let negated = matches!(operator, SimpleOp::NotIn);
                if values.is_empty() {
                    return Ok(if negated { "1" } else { "0" }.to_string());
                }
                for value in values {
                    self.push_literal(value);
                }
                Ok(format!(
                    "{left} {} ({})",
                    if negated { "NOT IN" } else { "IN" },
                    vec!["?"; values.len()].join(", ")
                ))
            }
            SimpleOp::Like | SimpleOp::NotLike | SimpleOp::ILike | SimpleOp::NotILike => {
                let right = match right {
                    RightVal::Scalar(value) => {
                        let pattern = match value {
                            Scalar::Text(value) => SqlValue::Text(postgres_like_to_glob(value)?),
                            other => scalar_to_sql(other),
                        };
                        self.bindings.push(QueryBinding::Literal(pattern));
                        "?".to_string()
                    }
                    RightVal::Column(column) => self.column_sql(table, column, alias)?,
                    RightVal::List(_) => return Err(reject("LIKE requires a scalar operand")),
                };
                let comparison = if matches!(operator, SimpleOp::ILike | SimpleOp::NotILike) {
                    format!("LOWER({left}) GLOB LOWER({right})")
                } else {
                    format!("{left} GLOB {right}")
                };
                if matches!(operator, SimpleOp::NotLike | SimpleOp::NotILike) {
                    Ok(format!("NOT ({comparison})"))
                } else {
                    Ok(comparison)
                }
            }
            _ => {
                let right = match right {
                    RightVal::Scalar(value) => {
                        self.push_literal(value);
                        "?".to_string()
                    }
                    RightVal::Column(column) => self.column_sql(table, column, alias)?,
                    RightVal::List(_) => {
                        return Err(reject("operator requires a scalar operand"));
                    }
                };
                Ok(format!("{left} {} {right}", binary_operator(operator)))
            }
        }
    }

    fn compile_exists(
        &mut self,
        negated: bool,
        related: &CorrelatedSubquery,
        parent_table: &str,
        parent_alias: &str,
    ) -> Result<String, EngineError> {
        let child = &related.subquery;
        let child_table = self.schema.table(&child.table)?;
        if child.limit == Some(0) {
            return Ok(if negated { "1" } else { "0" }.to_string());
        }
        let child_alias = self.alias();
        let mut predicates = Vec::with_capacity(related.parent_field.len() + 2);
        for (parent_field, child_field) in
            related.parent_field.iter().zip(related.child_field.iter())
        {
            let parent = self.column_sql(parent_table, parent_field, parent_alias)?;
            let child = self.column_sql(&child.table, child_field, &child_alias)?;
            predicates.push(format!("{child} = {parent}"));
        }
        if let Some(condition) = &child.where_ {
            predicates.push(self.compile_condition(condition, &child.table, &child_alias)?);
        }
        if let Some(start) = self.compile_start(child, &child_alias)? {
            predicates.push(start);
        }
        Ok(format!(
            "{} (SELECT 1 FROM {} AS {} WHERE {})",
            if negated { "NOT EXISTS" } else { "EXISTS" },
            quote_ident(&child_table.physical_name),
            quote_ident(&child_alias),
            predicates.join(" AND ")
        ))
    }

    fn order_terms(&self, ast: &Ast, alias: &str) -> Result<Vec<String>, EngineError> {
        let table = self.schema.table(&ast.table)?;
        let mut terms = Vec::new();
        let mut seen = BTreeSet::new();
        for OrderPart { column, desc } in &ast.order_by {
            seen.insert(column.as_str());
            terms.push(format!(
                "{} {}",
                self.column_sql(&ast.table, column, alias)?,
                if *desc { "DESC" } else { "ASC" }
            ));
        }
        for primary_key in &table.primary_key {
            if !seen.contains(primary_key.as_str()) {
                terms.push(format!(
                    "{} ASC",
                    self.column_sql(&ast.table, primary_key, alias)?
                ));
            }
        }
        Ok(terms)
    }

    fn compile_order(&self, ast: &Ast, alias: &str) -> Result<String, EngineError> {
        let terms = self.order_terms(ast, alias)?;
        Ok(if terms.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", terms.join(", "))
        })
    }

    fn compile_start(&mut self, ast: &Ast, alias: &str) -> Result<Option<String>, EngineError> {
        let Some(bound) = &ast.start else {
            return Ok(None);
        };
        let table = self.schema.table(&ast.table)?;
        let mut keys = Vec::new();
        let mut seen = BTreeSet::new();
        for OrderPart { column, desc } in &ast.order_by {
            self.schema.column(&ast.table, column)?;
            keys.push((column.clone(), *desc));
            seen.insert(column.as_str());
        }
        for primary_key in &table.primary_key {
            if !seen.contains(primary_key.as_str()) {
                keys.push((primary_key.clone(), false));
            }
        }
        let value_for = |column: &str| {
            bound
                .row
                .iter()
                .find(|(name, _)| name == column)
                .map(|(_, value)| value)
        };
        for (column, _) in &keys {
            if value_for(column).is_none() {
                return Err(reject(format!("start.row missing ordering key '{column}'")));
            }
        }

        let mut clauses = Vec::with_capacity(keys.len());
        for index in 0..keys.len() {
            let mut predicates = Vec::new();
            for (column, _) in keys.iter().take(index) {
                let column_sql = self.column_sql(&ast.table, column, alias)?;
                match value_for(column).expect("cursor key validated") {
                    Scalar::Null => predicates.push(format!("{column_sql} IS NULL")),
                    value => {
                        predicates.push(format!("{column_sql} = ?"));
                        self.push_literal(value);
                    }
                }
            }
            let (column, descending) = &keys[index];
            let column_sql = self.column_sql(&ast.table, column, alias)?;
            let value = value_for(column).expect("cursor key validated");
            let inclusive = index == keys.len() - 1 && !bound.exclusive;
            let comparison = match (*descending, inclusive, matches!(value, Scalar::Null)) {
                (false, false, true) => format!("{column_sql} IS NOT NULL"),
                (false, true, true) => "1".to_string(),
                (false, false, false) => {
                    self.push_literal(value);
                    format!("{column_sql} > ?")
                }
                (false, true, false) => {
                    self.push_literal(value);
                    format!("{column_sql} >= ?")
                }
                (true, false, true) => "0".to_string(),
                (true, true, true) => format!("{column_sql} IS NULL"),
                (true, false, false) => {
                    self.push_literal(value);
                    format!("({column_sql} < ? OR {column_sql} IS NULL)")
                }
                (true, true, false) => {
                    self.push_literal(value);
                    format!("({column_sql} <= ? OR {column_sql} IS NULL)")
                }
            };
            predicates.push(comparison);
            clauses.push(format!("({})", predicates.join(" AND ")));
        }
        Ok(Some(format!("({})", clauses.join(" OR "))))
    }
}

fn node_columns(table: &QueryTableSpec) -> Vec<QueryColumn> {
    table
        .columns
        .iter()
        .map(|column| QueryColumn {
            name: column.logical_name.clone(),
            column_type: column.column_type,
        })
        .collect()
}

fn selected_columns(table: &QueryTableSpec, alias: &str) -> String {
    table
        .columns
        .iter()
        .map(|column| {
            format!(
                "{}.{} AS {}",
                quote_ident(alias),
                quote_ident(&column.physical_name),
                quote_ident(&column.logical_name)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn effective_limit(ast: &Ast, singular: bool) -> Option<i64> {
    match (ast.limit, singular) {
        (Some(0), _) => Some(0),
        (_, true) => Some(1),
        (limit, false) => limit,
    }
}

fn relationship_alias(relationship: &CorrelatedSubquery) -> Result<&str, EngineError> {
    relationship
        .subquery
        .alias
        .as_deref()
        .filter(|alias| !alias.is_empty())
        .ok_or_else(|| reject("visible related subquery requires a non-empty alias"))
}

fn validate_format_tree(
    schema: &QuerySchema,
    ast: &Ast,
    format: &QueryFormat,
) -> Result<Vec<String>, EngineError> {
    let table = schema.table(&ast.table)?;
    let mut aliases = Vec::with_capacity(ast.related.len());
    let mut seen = BTreeSet::new();
    for relationship in &ast.related {
        let alias = relationship_alias(relationship)?;
        if !seen.insert(alias.to_string()) {
            return Err(reject(format!("duplicate related alias '{alias}'")));
        }
        if table
            .columns
            .iter()
            .any(|column| column.logical_name == alias)
        {
            return Err(reject(format!(
                "related alias '{alias}' conflicts with column '{0}.{alias}'",
                ast.table
            )));
        }
        aliases.push(alias.to_string());
    }
    let format_aliases = format
        .relationships
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let ast_aliases = aliases.iter().cloned().collect::<BTreeSet<_>>();
    if format_aliases != ast_aliases {
        return Err(reject(format!(
            "format relationships do not exactly match query relationships for table '{}'",
            ast.table
        )));
    }
    Ok(aliases)
}

fn compile_regular_node(
    schema: &QuerySchema,
    ast: &Ast,
    format: &QueryFormat,
    correlation: Option<(&str, &[String], &[String])>,
) -> Result<CompiledQueryNode, EngineError> {
    let table = schema.table(&ast.table)?;
    let aliases = validate_format_tree(schema, ast, format)?;
    let mut compiler = SqlCompiler::new(schema);
    let alias = compiler.alias();
    let mut predicates = Vec::new();

    if let Some((parent_table, parent_fields, child_fields)) = correlation {
        if parent_fields.len() != child_fields.len() || parent_fields.is_empty() {
            return Err(reject("invalid relationship correlation arity"));
        }
        for (parent_field, child_field) in parent_fields.iter().zip(child_fields.iter()) {
            schema.column(parent_table, parent_field)?;
            predicates.push(format!(
                "{} = ?",
                compiler.column_sql(&ast.table, child_field, &alias)?
            ));
            compiler
                .bindings
                .push(QueryBinding::ParentField(parent_field.clone()));
        }
    }
    if let Some(condition) = &ast.where_ {
        predicates.push(compiler.compile_condition(condition, &ast.table, &alias)?);
    }
    if let Some(start) = compiler.compile_start(ast, &alias)? {
        predicates.push(start);
    }

    let mut sql = format!(
        "SELECT {} FROM {} AS {}",
        selected_columns(table, &alias),
        quote_ident(&table.physical_name),
        quote_ident(&alias)
    );
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    sql.push_str(&compiler.compile_order(ast, &alias)?);
    if let Some(limit) = effective_limit(ast, format.singular) {
        sql.push_str(" LIMIT ?");
        compiler
            .bindings
            .push(QueryBinding::Literal(SqlValue::Integer(limit)));
    }

    let relationships = ast
        .related
        .iter()
        .zip(aliases)
        .map(|(relationship, name)| {
            let related_format = format
                .relationships
                .get(&name)
                .expect("format aliases validated");
            let node = if relationship.hidden {
                compile_hidden_node(schema, &ast.table, relationship, related_format)?
            } else {
                compile_regular_node(
                    schema,
                    &relationship.subquery,
                    related_format,
                    Some((
                        &ast.table,
                        &relationship.parent_field,
                        &relationship.child_field,
                    )),
                )?
            };
            Ok(CompiledRelationship { name, node })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;

    Ok(CompiledQueryNode {
        table: ast.table.clone(),
        singular: format.singular,
        sql,
        bindings: compiler.bindings,
        columns: node_columns(table),
        relationships,
    })
}

fn compile_hidden_node(
    schema: &QuerySchema,
    parent_table: &str,
    relationship: &CorrelatedSubquery,
    format: &QueryFormat,
) -> Result<CompiledQueryNode, EngineError> {
    let junction_ast = &relationship.subquery;
    if junction_ast.where_.is_some()
        || junction_ast.start.is_some()
        || junction_ast.limit.is_some()
        || !junction_ast.order_by.is_empty()
        || junction_ast.related.len() != 1
    {
        return Err(reject(
            "hidden relationship must use Zero's exact two-hop junction shape",
        ));
    }
    let destination_edge = &junction_ast.related[0];
    if destination_edge.hidden {
        return Err(reject(
            "hidden relationship destination edge must be visible",
        ));
    }
    let destination_ast = &destination_edge.subquery;
    let junction = schema.table(&junction_ast.table)?;
    let destination = schema.table(&destination_ast.table)?;
    let aliases = validate_format_tree(schema, destination_ast, format)?;
    let mut compiler = SqlCompiler::new(schema);
    let junction_alias = compiler.alias();
    let destination_alias = compiler.alias();

    if relationship.parent_field.len() != relationship.child_field.len()
        || relationship.parent_field.is_empty()
        || destination_edge.parent_field.len() != destination_edge.child_field.len()
        || destination_edge.parent_field.is_empty()
    {
        return Err(reject("invalid hidden relationship correlation arity"));
    }

    let mut join = Vec::new();
    for (junction_field, destination_field) in destination_edge
        .parent_field
        .iter()
        .zip(destination_edge.child_field.iter())
    {
        join.push(format!(
            "{} = {}",
            compiler.column_sql(&junction_ast.table, junction_field, &junction_alias)?,
            compiler.column_sql(
                &destination_ast.table,
                destination_field,
                &destination_alias
            )?
        ));
    }

    let mut predicates = Vec::new();
    for (parent_field, junction_field) in relationship
        .parent_field
        .iter()
        .zip(relationship.child_field.iter())
    {
        schema.column(parent_table, parent_field)?;
        schema.column(&junction_ast.table, junction_field)?;
        predicates.push(format!(
            "{} = ?",
            compiler.column_sql(&junction_ast.table, junction_field, &junction_alias)?
        ));
        compiler
            .bindings
            .push(QueryBinding::ParentField(parent_field.clone()));
    }
    if let Some(condition) = &destination_ast.where_ {
        predicates.push(compiler.compile_condition(
            condition,
            &destination_ast.table,
            &destination_alias,
        )?);
    }
    if let Some(start) = compiler.compile_start(destination_ast, &destination_alias)? {
        predicates.push(start);
    }

    let mut sql = format!(
        "SELECT {} FROM {} AS {} JOIN {} AS {} ON {}",
        selected_columns(destination, &destination_alias),
        quote_ident(&junction.physical_name),
        quote_ident(&junction_alias),
        quote_ident(&destination.physical_name),
        quote_ident(&destination_alias),
        join.join(" AND ")
    );
    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    sql.push_str(&compiler.compile_order(destination_ast, &destination_alias)?);
    if let Some(limit) = effective_limit(destination_ast, format.singular) {
        sql.push_str(" LIMIT ?");
        compiler
            .bindings
            .push(QueryBinding::Literal(SqlValue::Integer(limit)));
    }

    let relationships = destination_ast
        .related
        .iter()
        .zip(aliases)
        .map(|(related, name)| {
            let related_format = format
                .relationships
                .get(&name)
                .expect("format aliases validated");
            let node = if related.hidden {
                compile_hidden_node(schema, &destination_ast.table, related, related_format)?
            } else {
                compile_regular_node(
                    schema,
                    &related.subquery,
                    related_format,
                    Some((
                        &destination_ast.table,
                        &related.parent_field,
                        &related.child_field,
                    )),
                )?
            };
            Ok(CompiledRelationship { name, node })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;

    Ok(CompiledQueryNode {
        table: destination_ast.table.clone(),
        singular: format.singular,
        sql,
        bindings: compiler.bindings,
        columns: node_columns(destination),
        relationships,
    })
}

fn hash_bytes(state: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *state ^= u64::from(*byte);
        *state = state.wrapping_mul(0x100000001b3);
    }
}

fn hash_node(state: &mut u64, node: &CompiledQueryNode) {
    hash_bytes(state, node.table.as_bytes());
    hash_bytes(state, &[u8::from(node.singular)]);
    hash_bytes(state, node.sql.as_bytes());
    for binding in &node.bindings {
        match binding {
            QueryBinding::ParentField(field) => {
                hash_bytes(state, b"parent");
                hash_bytes(state, field.as_bytes());
            }
            QueryBinding::Literal(value) => {
                hash_bytes(state, b"literal");
                match value {
                    SqlValue::Null => hash_bytes(state, b"null"),
                    SqlValue::Integer(value) => hash_bytes(state, &value.to_le_bytes()),
                    SqlValue::Real(value) => hash_bytes(state, &value.to_bits().to_le_bytes()),
                    SqlValue::Text(value) => hash_bytes(state, value.as_bytes()),
                    SqlValue::Blob(value) => hash_bytes(state, value),
                }
            }
        }
    }
    for column in &node.columns {
        hash_bytes(state, column.name.as_bytes());
        hash_bytes(state, format!("{:?}", column.column_type).as_bytes());
    }
    for relationship in &node.relationships {
        hash_bytes(state, relationship.name.as_bytes());
        hash_node(state, &relationship.node);
    }
}

pub fn compile_transaction_query(
    schema: &QuerySchema,
    ast: &Ast,
    format: &QueryFormat,
) -> Result<CompiledQueryPlan, EngineError> {
    let root = compile_regular_node(schema, ast, format, None)?;
    let mut hash = 0xcbf29ce484222325;
    hash_node(&mut hash, &root);
    Ok(CompiledQueryPlan {
        root_table: ast.table.clone(),
        plan_hash: format!("{hash:016x}"),
        root,
    })
}
