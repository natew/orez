// the Zero v51 query AST subset the engine supports, transcribed from
// ~/github/mono packages/zero-protocol/src/ast.ts. the engine receives the
// TRANSFORMED AST: the consumer host has already resolved the query name +
// args, applied auth/permission transformations, and mapped names, so static
// parameters are already resolved to literals. the engine still VALIDATES the
// AST (invariant: trust nothing), accepting EXACTLY the plan's supported subset
// and rejecting everything else deterministically.
//
// supported: simple conditions (= != IS "IS NOT" < > <= >= LIKE "NOT LIKE"
// ILIKE "NOT ILIKE" IN "NOT IN") with a column or literal on the left and a
// literal (or array, for IN) on the right; and/or; correlated EXISTS/NOT EXISTS
// subqueries; related subqueries; orderBy (with a stable pk tie-breaker added at
// compile time); limit; start cursor. unsupported (static params, cross-table
// column refs, unknown ops/fields/tables/columns) is a 400.

use serde_json::Value;

use crate::error::EngineError;

#[derive(Debug, Clone, PartialEq)]
pub struct Ast {
    pub table: String,
    pub alias: Option<String>,
    pub where_: Option<Condition>,
    pub related: Vec<CorrelatedSubquery>,
    pub order_by: Vec<OrderPart>,
    pub limit: Option<i64>,
    pub start: Option<Bound>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderPart {
    pub column: String,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    // ordered (column, value) pairs matching orderBy, plus the pk tie-breaker
    pub row: Vec<(String, Scalar)>,
    pub exclusive: bool,
}

// a literal scalar value (LiteralReference.value, minus arrays which are IN-only)
#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SimpleOp {
    Eq,
    Ne,
    Is,
    IsNot,
    Lt,
    Gt,
    Le,
    Ge,
    Like,
    NotLike,
    ILike,
    NotILike,
    In,
    NotIn,
}

impl SimpleOp {
    fn parse(op: &str) -> Option<SimpleOp> {
        Some(match op {
            "=" => SimpleOp::Eq,
            "!=" => SimpleOp::Ne,
            "IS" => SimpleOp::Is,
            "IS NOT" => SimpleOp::IsNot,
            "<" => SimpleOp::Lt,
            ">" => SimpleOp::Gt,
            "<=" => SimpleOp::Le,
            ">=" => SimpleOp::Ge,
            "LIKE" => SimpleOp::Like,
            "NOT LIKE" => SimpleOp::NotLike,
            "ILIKE" => SimpleOp::ILike,
            "NOT ILIKE" => SimpleOp::NotILike,
            "IN" => SimpleOp::In,
            "NOT IN" => SimpleOp::NotIn,
            _ => return None,
        })
    }

    // IN / NOT IN take an array operand; every other op takes a scalar
    pub fn takes_list(&self) -> bool {
        matches!(self, SimpleOp::In | SimpleOp::NotIn)
    }
}

// the right-hand operand of a simple condition: a scalar, or an array for IN
#[derive(Debug, Clone, PartialEq)]
pub enum RightVal {
    Scalar(Scalar),
    List(Vec<Scalar>),
}

// left of a simple condition: a column or a resolved literal (a cross-table
// column path is unsupported — `name` is a bare column only)
#[derive(Debug, Clone, PartialEq)]
pub enum ValueRef {
    Column(String),
    Literal(Scalar),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Simple {
        op: SimpleOp,
        left: ValueRef,
        right: RightVal,
    },
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Exists {
        negated: bool,
        related: CorrelatedSubquery,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorrelatedSubquery {
    pub parent_field: Vec<String>,
    pub child_field: Vec<String>,
    pub subquery: Box<Ast>,
    pub hidden: bool,
}

// every table a query's RESULT depends on: its root table, the tables of its
// correlated EXISTS subqueries (a permission flip on one changes membership),
// and its related-output subqueries (a child row appearing/leaving), all walked
// recursively. recomputation narrowing (plan algorithm step 2) recomputes a
// query only when the touched tables intersect this set.
pub fn collect_dependency_tables(ast: &Ast, out: &mut std::collections::BTreeSet<String>) {
    out.insert(ast.table.clone());
    if let Some(cond) = &ast.where_ {
        collect_condition_tables(cond, out);
    }
    for rel in &ast.related {
        collect_dependency_tables(&rel.subquery, out);
    }
}

fn collect_condition_tables(cond: &Condition, out: &mut std::collections::BTreeSet<String>) {
    match cond {
        Condition::Simple { .. } => {}
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                collect_condition_tables(c, out);
            }
        }
        Condition::Exists { related, .. } => collect_dependency_tables(&related.subquery, out),
    }
}

fn reject(msg: impl Into<String>) -> EngineError {
    EngineError::bad_request(msg)
}

// the set of keys an AST object may carry; anything else is a rejection
const AST_KEYS: &[&str] = &[
    "schema", "table", "alias", "where", "related", "limit", "orderBy", "start",
];

fn assert_only_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
    ctx: &str,
) -> Result<(), EngineError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(reject(format!("unsupported {ctx} field '{key}'")));
        }
    }
    Ok(())
}

pub fn parse_ast(value: &Value) -> Result<Ast, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("query AST must be an object"))?;
    assert_only_keys(obj, AST_KEYS, "query")?;

    // `schema` is accepted (single-namespace engine) but ignored.
    if obj.get("schema").is_some_and(|value| !value.is_string()) {
        return Err(reject("query schema must be a string"));
    }
    let table = obj
        .get("table")
        .and_then(Value::as_str)
        .ok_or_else(|| reject("query AST requires a string table"))?
        .to_string();
    let alias = match obj.get("alias") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err(reject("query alias must be a string")),
    };

    let where_ = match obj.get("where") {
        None | Some(Value::Null) => None,
        Some(w) => Some(parse_condition(w)?),
    };

    let related = match obj.get("related") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(parse_correlated_subquery)
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(reject("query related must be an array")),
    };

    let limit = match obj.get("limit") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => {
            let l = n
                .as_i64()
                .filter(|l| *l >= 0)
                .ok_or_else(|| reject("limit must be a non-negative integer"))?;
            Some(l)
        }
        Some(_) => return Err(reject("limit must be a number")),
    };

    let order_by = match obj.get("orderBy") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .map(parse_order_part)
            .collect::<Result<Vec<_>, _>>()?,
        Some(_) => return Err(reject("orderBy must be an array")),
    };

    let start = match obj.get("start") {
        None | Some(Value::Null) => None,
        Some(s) => Some(parse_bound(s, &order_by)?),
    };

    Ok(Ast {
        table,
        alias,
        where_,
        related,
        order_by,
        limit,
        start,
    })
}

fn parse_order_part(value: &Value) -> Result<OrderPart, EngineError> {
    let arr = value
        .as_array()
        .ok_or_else(|| reject("orderBy element must be a [column, dir] tuple"))?;
    if arr.len() != 2 {
        return Err(reject("orderBy element must be a [column, dir] tuple"));
    }
    let column = arr[0]
        .as_str()
        .ok_or_else(|| reject("orderBy column must be a string"))?
        .to_string();
    let desc = match arr[1].as_str() {
        Some("asc") => false,
        Some("desc") => true,
        _ => return Err(reject("orderBy direction must be 'asc' or 'desc'")),
    };
    Ok(OrderPart { column, desc })
}

fn parse_bound(value: &Value, order_by: &[OrderPart]) -> Result<Bound, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("start must be an object"))?;
    assert_only_keys(obj, &["row", "exclusive"], "start")?;
    let exclusive = obj
        .get("exclusive")
        .and_then(Value::as_bool)
        .ok_or_else(|| reject("start.exclusive must be a boolean"))?;
    let row_obj = obj
        .get("row")
        .and_then(Value::as_object)
        .ok_or_else(|| reject("start.row must be an object"))?;
    // the bound row must cover the order-by columns, but the compiler's effective
    // ordering key is orderBy + an appended primary-key tie-break, and the stock
    // builder emits a cursor carrying that PK too. capture EVERY field the cursor
    // supplies (orderBy columns validated present, plus the PK/other tie-break
    // columns), not just the explicit orderBy, so compile_start finds a value for
    // the full key set instead of 400ing on the implicit PK component.
    if order_by.is_empty() {
        return Err(reject("start cursor requires an orderBy"));
    }
    let mut row = Vec::with_capacity(row_obj.len());
    for part in order_by {
        let v = row_obj.get(&part.column).ok_or_else(|| {
            reject(format!(
                "start.row missing ordered column '{}'",
                part.column
            ))
        })?;
        row.push((part.column.clone(), parse_scalar(v)?));
    }
    // additional cursor fields (e.g. the appended PK tie-break) in a stable order.
    // a stock builder ships a full materialized row, which may carry non-ordering
    // columns of any type (json objects, arrays); only the ordering key + PK
    // (scalars) matter to compile_start, so capture the SCALAR extras and ignore
    // anything that is not a scalar operand rather than 400ing on it.
    for (col, v) in row_obj {
        if order_by.iter().any(|p| &p.column == col) {
            continue;
        }
        if let Ok(scalar) = parse_scalar(v) {
            row.push((col.clone(), scalar));
        }
    }
    Ok(Bound { row, exclusive })
}

fn parse_condition(value: &Value) -> Result<Condition, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("condition must be an object"))?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| reject("condition requires a type"))?;
    match ty {
        "simple" => {
            assert_only_keys(obj, &["type", "op", "left", "right"], "simple condition")?;
            let op_str = obj
                .get("op")
                .and_then(Value::as_str)
                .ok_or_else(|| reject("simple condition requires op"))?;
            let op = SimpleOp::parse(op_str)
                .ok_or_else(|| reject(format!("unsupported operator '{op_str}'")))?;
            let left = parse_value_ref(
                obj.get("left")
                    .ok_or_else(|| reject("simple condition requires left"))?,
            )?;
            let right = parse_right(
                obj.get("right")
                    .ok_or_else(|| reject("simple condition requires right"))?,
                op.takes_list(),
            )?;
            Ok(Condition::Simple { op, left, right })
        }
        "and" | "or" => {
            assert_only_keys(obj, &["type", "conditions"], "junction")?;
            let items = obj
                .get("conditions")
                .and_then(Value::as_array)
                .ok_or_else(|| reject("and/or requires a conditions array"))?;
            let conds = items
                .iter()
                .map(parse_condition)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if ty == "and" {
                Condition::And(conds)
            } else {
                Condition::Or(conds)
            })
        }
        "correlatedSubquery" => {
            assert_only_keys(
                obj,
                &["type", "related", "op", "flip", "scalar"],
                "correlated subquery",
            )?;
            let op_str = obj
                .get("op")
                .and_then(Value::as_str)
                .ok_or_else(|| reject("correlatedSubquery requires op"))?;
            let negated = match op_str {
                "EXISTS" => false,
                "NOT EXISTS" => true,
                _ => return Err(reject(format!("unsupported subquery operator '{op_str}'"))),
            };
            let related = parse_correlated_subquery(
                obj.get("related")
                    .ok_or_else(|| reject("correlatedSubquery requires related"))?,
            )?;
            if obj.get("flip").is_some_and(|value| !value.is_boolean()) {
                return Err(reject("correlatedSubquery flip must be a boolean"));
            }
            match obj.get("scalar") {
                Some(Value::Bool(true)) => {
                    return Err(reject("scalar correlated subqueries are unsupported"));
                }
                Some(Value::Bool(false)) | None => {}
                Some(_) => {
                    return Err(reject("correlatedSubquery scalar must be a boolean"));
                }
            }
            // `flip` is a planning hint and does not change SQL semantics.
            Ok(Condition::Exists { negated, related })
        }
        other => Err(reject(format!("unsupported condition type '{other}'"))),
    }
}

fn parse_correlated_subquery(value: &Value) -> Result<CorrelatedSubquery, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("related must be an object"))?;
    assert_only_keys(
        obj,
        &["correlation", "subquery", "hidden", "system"],
        "related",
    )?;
    let corr = obj
        .get("correlation")
        .and_then(Value::as_object)
        .ok_or_else(|| reject("related requires a correlation object"))?;
    assert_only_keys(corr, &["parentField", "childField"], "correlation")?;
    let parent_field = parse_compound_key(corr.get("parentField"))?;
    let child_field = parse_compound_key(corr.get("childField"))?;
    if parent_field.len() != child_field.len() {
        return Err(reject("correlation parentField/childField length mismatch"));
    }
    let subquery = Box::new(parse_ast(
        obj.get("subquery")
            .ok_or_else(|| reject("related requires a subquery"))?,
    )?);
    let hidden = match obj.get("hidden") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(reject("related hidden must be a boolean")),
    };
    match obj.get("system") {
        None => {}
        Some(Value::String(value))
            if matches!(value.as_str(), "permissions" | "client" | "test") => {}
        Some(_) => {
            return Err(reject(
                "related system must be permissions, client, or test",
            ));
        }
    }
    // `system` is metadata after the query has been transformed.
    Ok(CorrelatedSubquery {
        parent_field,
        child_field,
        subquery,
        hidden,
    })
}

fn parse_compound_key(value: Option<&Value>) -> Result<Vec<String>, EngineError> {
    let arr = value
        .and_then(Value::as_array)
        .ok_or_else(|| reject("compound key must be an array"))?;
    if arr.is_empty() {
        return Err(reject("compound key must be non-empty"));
    }
    arr.iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| reject("compound key entries must be strings"))
        })
        .collect()
}

fn parse_value_ref(value: &Value) -> Result<ValueRef, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("value position must be an object"))?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| reject("value position requires a type"))?;
    match ty {
        "column" => {
            assert_only_keys(obj, &["type", "name"], "column reference")?;
            let name = obj
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| reject("column reference requires a name"))?;
            Ok(ValueRef::Column(name.to_string()))
        }
        "literal" => Ok(ValueRef::Literal(parse_literal(obj)?)),
        "static" => Err(reject(
            "static parameters must be resolved before reaching the engine",
        )),
        other => Err(reject(format!("unsupported value position type '{other}'"))),
    }
}

// right side of a simple condition: a literal only (a column on the right is
// disallowed by the v51 SimpleCondition type). IN/NOT IN require an array
// literal; every other op requires a scalar.
fn parse_right(value: &Value, wants_list: bool) -> Result<RightVal, EngineError> {
    let obj = value
        .as_object()
        .ok_or_else(|| reject("condition right must be an object"))?;
    let ty = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| reject("condition right requires a type"))?;
    if ty == "static" {
        return Err(reject(
            "static parameters must be resolved before reaching the engine",
        ));
    }
    if ty != "literal" {
        return Err(reject(format!(
            "condition right must be a literal, got '{ty}'"
        )));
    }
    assert_only_keys(obj, &["type", "value"], "literal")?;
    let v = obj
        .get("value")
        .ok_or_else(|| reject("literal requires a value"))?;
    match (wants_list, v) {
        (true, Value::Array(items)) => {
            let list = items
                .iter()
                .map(parse_scalar)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(RightVal::List(list))
        }
        (true, _) => Err(reject("IN/NOT IN requires an array literal")),
        (false, _) => Ok(RightVal::Scalar(parse_scalar(v)?)),
    }
}

fn parse_literal(obj: &serde_json::Map<String, Value>) -> Result<Scalar, EngineError> {
    assert_only_keys(obj, &["type", "value"], "literal")?;
    let v = obj
        .get("value")
        .ok_or_else(|| reject("literal requires a value"))?;
    parse_scalar(v)
}

fn parse_scalar(v: &Value) -> Result<Scalar, EngineError> {
    match v {
        Value::Null => Ok(Scalar::Null),
        Value::Bool(b) => Ok(Scalar::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Scalar::Int(i))
            } else {
                Ok(Scalar::Float(n.as_f64().unwrap()))
            }
        }
        Value::String(s) => Ok(Scalar::Text(s.clone())),
        // a nested array (array-of-arrays) is not a valid scalar operand
        Value::Array(_) => Err(reject("nested array literals are unsupported")),
        Value::Object(_) => Err(reject("object literals are unsupported")),
    }
}
