// compile a validated Zero v51 AST into a SQLite SELECT over the root table's
// rows, with positional `?` bindings ONLY — never any string interpolation of a
// value. correlated EXISTS/NOT EXISTS become SQL EXISTS subqueries; orderBy gets
// a stable primary-key tie-breaker appended; limit and start cursor bound the
// result. columns and tables are validated against the schema (unknown ->
// rejection). related OUTPUT subqueries are handled by membership, not here;
// this compiles the ROOT row-set a query selects.

use std::collections::BTreeSet;

use crate::db::SqlValue;
use crate::error::EngineError;
use crate::schema::{Tables, quote_ident};

use super::ast::{
    Ast, Condition, CorrelatedSubquery, OrderPart, RightVal, Scalar, SimpleOp, ValueRef,
};

// the SQL operator for the binary comparison ops (not IN/LIKE, which compile
// to their own shapes)
fn binary_op_sql(op: &SimpleOp) -> &'static str {
    match op {
        SimpleOp::Eq => "=",
        SimpleOp::Ne => "!=",
        SimpleOp::Is => "IS",
        SimpleOp::IsNot => "IS NOT",
        SimpleOp::Lt => "<",
        SimpleOp::Gt => ">",
        SimpleOp::Le => "<=",
        SimpleOp::Ge => ">=",
        _ => unreachable!("binary_op_sql called on a non-binary op"),
    }
}

pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<SqlValue>,
    // every table the row-set depends on (root + EXISTS-subquery tables), sorted
    pub dependency_tables: Vec<String>,
    // the root table's primary key, for membership keying
    pub primary_key: Vec<String>,
}

fn reject(msg: impl Into<String>) -> EngineError {
    EngineError::bad_request(msg)
}

// related-output and correlated-EXISTS child subqueries are synced as row SETS
// (or evaluated as existence tests); that form cannot express a per-parent limit
// or start cursor, so dropping such a bound would silently widen membership (a
// desired single related row becomes every related row) or flip an EXISTS (a
// limit-0 subquery that should yield nothing tests as existing). a child carrying
// limit/start is therefore rejected rather than widened. orderBy on a child has
// no effect on the synced set or on existence and is accepted (ignored).
fn reject_unsupported_child_bounds(ast: &Ast) -> Result<(), EngineError> {
    if let Some(cond) = &ast.where_ {
        reject_bounds_in_condition(cond)?;
    }
    for rel in &ast.related {
        reject_child_bounds(&rel.subquery)?;
    }
    Ok(())
}

fn reject_bounds_in_condition(cond: &Condition) -> Result<(), EngineError> {
    match cond {
        Condition::Exists { related, .. } => reject_child_bounds(&related.subquery),
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                reject_bounds_in_condition(c)?;
            }
            Ok(())
        }
        Condition::Simple { .. } => Ok(()),
    }
}

fn reject_child_bounds(child: &Ast) -> Result<(), EngineError> {
    if child.limit.is_some() {
        return Err(reject(
            "a related/EXISTS subquery cannot carry a limit (per-parent bound unsupported)",
        ));
    }
    if child.start.is_some() {
        return Err(reject(
            "a related/EXISTS subquery cannot carry a start cursor (per-parent bound unsupported)",
        ));
    }
    reject_unsupported_child_bounds(child)
}

fn scalar_to_sql(s: &Scalar) -> SqlValue {
    match s {
        Scalar::Null => SqlValue::Null,
        // sqlite has no boolean type; booleans are stored 0/1 (matches the
        // engine's toZeroValue conversion on the way out)
        Scalar::Bool(b) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Scalar::Int(i) => SqlValue::Integer(*i),
        Scalar::Float(f) => SqlValue::Real(*f),
        Scalar::Text(t) => SqlValue::Text(t.clone()),
    }
}

struct Compiler<'a> {
    tables: &'a Tables,
    params: Vec<SqlValue>,
    deps: BTreeSet<String>,
    next_alias: usize,
}

impl<'a> Compiler<'a> {
    fn new(tables: &'a Tables) -> Self {
        Compiler {
            tables,
            params: Vec::new(),
            deps: BTreeSet::new(),
            next_alias: 0,
        }
    }

    fn alias(&mut self) -> String {
        let a = format!("q{}", self.next_alias);
        self.next_alias += 1;
        a
    }

    fn check_table(&mut self, table: &str) -> Result<(), EngineError> {
        if self.tables.get(table).is_none() {
            return Err(reject(format!("unknown table '{table}'")));
        }
        self.deps.insert(table.to_string());
        Ok(())
    }

    fn check_column(&self, table: &str, column: &str) -> Result<(), EngineError> {
        let spec = self
            .tables
            .get(table)
            .ok_or_else(|| reject(format!("unknown table '{table}'")))?;
        if spec.column_type(column).is_none() {
            return Err(reject(format!("unknown column '{table}.{column}'")));
        }
        Ok(())
    }

    // the root query: SELECT <alias>.* FROM "table" AS <alias> WHERE ...
    // ORDER BY ... LIMIT ?
    fn compile_root(&mut self, ast: &Ast) -> Result<String, EngineError> {
        self.check_table(&ast.table)?;
        let alias = self.alias();
        let mut sql = format!(
            "SELECT {a}.* FROM {t} AS {a}",
            a = quote_ident(&alias),
            t = quote_ident(&ast.table)
        );

        // validate ordering columns up front so start-cursor / order-by can rely
        // on them existing
        for part in &ast.order_by {
            self.check_column(&ast.table, &part.column)?;
        }

        let mut wheres: Vec<String> = Vec::new();
        if let Some(cond) = &ast.where_ {
            wheres.push(self.compile_condition(cond, &ast.table, &alias)?);
        }
        if let Some(cursor) = self.compile_start(ast, &alias)? {
            wheres.push(cursor);
        }
        if !wheres.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&wheres.join(" AND "));
        }

        sql.push_str(&self.compile_order_by(ast, &alias)?);

        if let Some(limit) = ast.limit {
            sql.push_str(" LIMIT ?");
            self.params.push(SqlValue::Integer(limit));
        }
        Ok(sql)
    }

    // orderBy with a stable primary-key tie-breaker appended
    fn compile_order_by(&self, ast: &Ast, alias: &str) -> Result<String, EngineError> {
        let spec = self.tables.get(&ast.table).unwrap();
        let mut parts: Vec<String> = Vec::new();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for OrderPart { column, desc } in &ast.order_by {
            self.check_column(&ast.table, column)?;
            seen.insert(column.as_str());
            parts.push(format!(
                "{}.{} {}",
                quote_ident(alias),
                quote_ident(column),
                if *desc { "DESC" } else { "ASC" }
            ));
        }
        // stable tie-breaker: append pk columns not already ordered, ascending
        for pk in &spec.primary_key {
            if !seen.contains(pk.as_str()) {
                parts.push(format!("{}.{} ASC", quote_ident(alias), quote_ident(pk)));
            }
        }
        if parts.is_empty() {
            Ok(String::new())
        } else {
            Ok(format!(" ORDER BY {}", parts.join(", ")))
        }
    }

    // keyset cursor: rows strictly (or inclusively) after the bound, in the
    // ordering (orderBy columns then pk). built as a lexicographic OR-of-ANDs so
    // it uses only positional binds.
    fn compile_start(&mut self, ast: &Ast, alias: &str) -> Result<Option<String>, EngineError> {
        let Some(bound) = &ast.start else {
            return Ok(None);
        };
        let spec = self.tables.get(&ast.table).unwrap();
        // the ordering keys = orderBy columns + pk tie-breaker (all ascending
        // unless the orderBy says otherwise)
        let mut keys: Vec<(String, bool)> = Vec::new(); // (column, desc)
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for OrderPart { column, desc } in &ast.order_by {
            keys.push((column.clone(), *desc));
            seen.insert(column.as_str());
        }
        for pk in &spec.primary_key {
            if !seen.contains(pk.as_str()) {
                keys.push((pk.clone(), false));
            }
        }
        // the bound row must carry a value for every ordering key
        let value_for = |col: &str| bound.row.iter().find(|(c, _)| c == col).map(|(_, v)| v);
        for (col, _) in &keys {
            if value_for(col).is_none() {
                return Err(reject(format!("start.row missing ordering key '{col}'")));
            }
        }

        // lexicographic "after": OR over prefixes — for key i, all earlier keys
        // equal and key i strictly past the bound (direction-aware). the last
        // clause uses >=/<= when the bound is inclusive.
        let mut clauses: Vec<String> = Vec::new();
        for i in 0..keys.len() {
            let mut ands: Vec<String> = Vec::new();
            for (col, _) in keys.iter().take(i) {
                ands.push(format!("{}.{} = ?", quote_ident(alias), quote_ident(col)));
                self.params.push(scalar_to_sql(value_for(col).unwrap()));
            }
            let (col, desc) = &keys[i];
            let last = i == keys.len() - 1;
            let op = match (desc, last && !bound.exclusive) {
                (false, false) => ">",
                (false, true) => ">=",
                (true, false) => "<",
                (true, true) => "<=",
            };
            ands.push(format!(
                "{}.{} {op} ?",
                quote_ident(alias),
                quote_ident(col)
            ));
            self.params.push(scalar_to_sql(value_for(col).unwrap()));
            clauses.push(format!("({})", ands.join(" AND ")));
        }
        Ok(Some(format!("({})", clauses.join(" OR "))))
    }

    fn compile_condition(
        &mut self,
        cond: &Condition,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        match cond {
            Condition::Simple { op, left, right } => {
                self.compile_simple(op, left, right, table, alias)
            }
            Condition::And(conds) => self.compile_junction(conds, "AND", "1", table, alias),
            Condition::Or(conds) => self.compile_junction(conds, "OR", "0", table, alias),
            Condition::Exists { negated, related } => {
                self.compile_exists(*negated, related, table, alias)
            }
        }
    }

    fn compile_junction(
        &mut self,
        conds: &[Condition],
        joiner: &str,
        empty: &str,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        if conds.is_empty() {
            return Ok(empty.to_string());
        }
        let parts = conds
            .iter()
            .map(|c| self.compile_condition(c, table, alias))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(format!("({})", parts.join(&format!(" {joiner} "))))
    }

    fn compile_simple(
        &mut self,
        op: &SimpleOp,
        left: &ValueRef,
        right: &RightVal,
        table: &str,
        alias: &str,
    ) -> Result<String, EngineError> {
        let left_sql = match left {
            ValueRef::Column(col) => {
                self.check_column(table, col)?;
                format!("{}.{}", quote_ident(alias), quote_ident(col))
            }
            ValueRef::Literal(s) => {
                self.params.push(scalar_to_sql(s));
                "?".to_string()
            }
        };
        match op {
            SimpleOp::In | SimpleOp::NotIn => {
                let RightVal::List(list) = right else {
                    return Err(reject("IN/NOT IN requires an array operand"));
                };
                let negate = matches!(op, SimpleOp::NotIn);
                if list.is_empty() {
                    // `x IN ()` is a syntax error; empty IN is false, empty NOT IN is true
                    return Ok(if negate { "1" } else { "0" }.to_string());
                }
                let holes = vec!["?"; list.len()].join(", ");
                for s in list {
                    self.params.push(scalar_to_sql(s));
                }
                let kw = if negate { "NOT IN" } else { "IN" };
                Ok(format!("{left_sql} {kw} ({holes})"))
            }
            SimpleOp::Like | SimpleOp::NotLike | SimpleOp::ILike | SimpleOp::NotILike => {
                let RightVal::Scalar(s) = right else {
                    return Err(reject("LIKE requires a scalar operand"));
                };
                self.params.push(scalar_to_sql(s));
                let negate = matches!(op, SimpleOp::NotLike | SimpleOp::NotILike);
                let kw = if negate { "NOT LIKE" } else { "LIKE" };
                if matches!(op, SimpleOp::ILike | SimpleOp::NotILike) {
                    // ILIKE: case-insensitive regardless of the host's
                    // case_sensitive_like pragma, by folding both operands with
                    // LOWER. SQLite LOWER folds ASCII only, matching Postgres
                    // ILIKE for ASCII; unicode case differences are not folded
                    // (documented caveat — search on non-ASCII case pairs can
                    // diverge from stock zero-cache/Postgres).
                    Ok(format!("LOWER({left_sql}) {kw} LOWER(?)"))
                } else {
                    // LIKE inherits SQLite's LIKE case-folding (ASCII
                    // case-insensitive by default). a host wanting Postgres's
                    // case-sensitive LIKE sets `PRAGMA case_sensitive_like = ON`,
                    // under which the ILIKE branch above stays case-insensitive.
                    Ok(format!("{left_sql} {kw} ?"))
                }
            }
            _ => {
                let RightVal::Scalar(s) = right else {
                    return Err(reject("operator requires a scalar operand"));
                };
                self.params.push(scalar_to_sql(s));
                Ok(format!("{left_sql} {} ?", binary_op_sql(op)))
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
        self.check_table(&child.table)?;
        let child_alias = self.alias();

        // correlation predicate: child.childField[i] = parent.parentField[i]
        let mut corr: Vec<String> = Vec::new();
        for (pf, cf) in related.parent_field.iter().zip(related.child_field.iter()) {
            self.check_column(parent_table, pf)?;
            self.check_column(&child.table, cf)?;
            corr.push(format!(
                "{}.{} = {}.{}",
                quote_ident(&child_alias),
                quote_ident(cf),
                quote_ident(parent_alias),
                quote_ident(pf)
            ));
        }

        let mut wheres = corr;
        if let Some(cond) = &child.where_ {
            wheres.push(self.compile_condition(cond, &child.table, &child_alias)?);
        }
        let kw = if negated { "NOT EXISTS" } else { "EXISTS" };
        Ok(format!(
            "{kw} (SELECT 1 FROM {t} AS {a} WHERE {w})",
            t = quote_ident(&child.table),
            a = quote_ident(&child_alias),
            w = wheres.join(" AND ")
        ))
    }
}

// a relevance probe: does a single primary-key row of the root table match the
// query's predicate (EXISTS conditions included), ignoring order/limit? returns
// the SQL with the root predicate binds plus a trailing `?` per pk column, and
// the pk column order the caller binds after the predicate params. touched-pk
// narrowing (plan optimization: "narrow recomputation using touched primary
// keys") uses this to skip a query when no touched root row is a member or a
// match.
pub fn compile_predicate_probe(
    ast: &Ast,
    tables: &Tables,
) -> Result<(String, Vec<SqlValue>, Vec<String>), EngineError> {
    let mut c = Compiler::new(tables);
    c.check_table(&ast.table)?;
    let spec = tables
        .get(&ast.table)
        .ok_or_else(|| reject(format!("unknown table '{}'", ast.table)))?;
    let alias = c.alias();
    let mut wheres: Vec<String> = Vec::new();
    if let Some(cond) = &ast.where_ {
        wheres.push(c.compile_condition(cond, &ast.table, &alias)?);
    }
    for col in &spec.primary_key {
        wheres.push(format!("{}.{} = ?", quote_ident(&alias), quote_ident(col)));
    }
    let sql = format!(
        "SELECT 1 FROM {} AS {} WHERE {} LIMIT 1",
        quote_ident(&ast.table),
        quote_ident(&alias),
        wheres.join(" AND ")
    );
    Ok((sql, c.params, spec.primary_key.clone()))
}

pub fn compile(ast: &Ast, tables: &Tables) -> Result<CompiledQuery, EngineError> {
    // reject child subqueries carrying per-parent bounds before compiling; this is
    // the single validation gate (register_query and every recompute call it).
    reject_unsupported_child_bounds(ast)?;
    let mut c = Compiler::new(tables);
    let sql = c.compile_root(ast)?;
    let primary_key = tables
        .get(&ast.table)
        .ok_or_else(|| reject(format!("unknown table '{}'", ast.table)))?
        .primary_key
        .clone();
    Ok(CompiledQuery {
        sql,
        params: c.params,
        dependency_tables: c.deps.into_iter().collect(),
        primary_key,
    })
}

// a related-output child row-set: the child rows belonging to the root query's
// matching rows (a `related` subquery emits these into the query result).
pub struct CompiledRelated {
    pub sql: String,
    pub params: Vec<SqlValue>,
    pub child_table: String,
    pub primary_key: Vec<String>,
}

// compile one related subquery into the child rows it contributes: child rows
// correlated (child.childField = parent.parentField) to a PARENT row-set given
// as an already-compiled `(parent_sql)` subquery. `parent_table` names the
// parent rows' table so the parentField columns can be validated. this composes
// recursively: the returned child SQL is itself a valid parent_sql for the
// child's own related subqueries, which is how nested related-of-related is
// walked. `depth` keeps the correlation aliases unique across nesting levels.
pub fn compile_related_of(
    parent_sql: &str,
    parent_params: &[SqlValue],
    parent_table: &str,
    rel: &CorrelatedSubquery,
    tables: &Tables,
    depth: usize,
) -> Result<CompiledRelated, EngineError> {
    let mut c = Compiler::new(tables);
    let child = &rel.subquery;
    c.check_table(&child.table)?;

    let rc = format!("rc{depth}");
    let rp = format!("rp{depth}");
    let mut on: Vec<String> = Vec::new();
    for (pf, cf) in rel.parent_field.iter().zip(rel.child_field.iter()) {
        c.check_column(parent_table, pf)?;
        c.check_column(&child.table, cf)?;
        on.push(format!(
            "{}.{} = {}.{}",
            quote_ident(&rc),
            quote_ident(cf),
            quote_ident(&rp),
            quote_ident(pf)
        ));
    }

    // params in SQL order: the parent subquery (in the JOIN) binds first, then
    // the child filter in the WHERE.
    let mut params = parent_params.to_vec();
    let where_sql = match &child.where_ {
        Some(cond) => format!(" WHERE {}", c.compile_condition(cond, &child.table, &rc)?),
        None => String::new(),
    };
    params.extend(c.params);

    let sql = format!(
        "SELECT DISTINCT {rc}.* FROM {ct} AS {rc} JOIN ({parent_sql}) AS {rp} ON {on}{where_sql}",
        ct = quote_ident(&child.table),
        on = on.join(" AND "),
    );
    let primary_key = tables
        .get(&child.table)
        .ok_or_else(|| reject(format!("unknown table '{}'", child.table)))?
        .primary_key
        .clone();
    Ok(CompiledRelated {
        sql,
        params,
        child_table: child.table.clone(),
        primary_key,
    })
}
