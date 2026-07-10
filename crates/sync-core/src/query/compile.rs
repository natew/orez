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
                // note: SQLite LIKE is ASCII-case-insensitive by default, so both
                // LIKE and ILIKE map to LIKE here; that differs from Postgres's
                // case-sensitive LIKE and matches ILIKE. refine with a collation
                // if a conformance lane needs case-sensitive LIKE.
                let kw = if matches!(op, SimpleOp::NotLike | SimpleOp::NotILike) {
                    "NOT LIKE"
                } else {
                    "LIKE"
                };
                Ok(format!("{left_sql} {kw} ?"))
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

pub fn compile(ast: &Ast, tables: &Tables) -> Result<CompiledQuery, EngineError> {
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
