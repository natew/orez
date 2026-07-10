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

    // the ORDER BY terms ("alias.col DIR") for an ast: its orderBy columns plus a
    // stable primary-key tie-breaker. shared by the root ORDER BY and the
    // per-parent window's ROW_NUMBER ordering so ranking is deterministic.
    fn order_by_terms(&self, ast: &Ast, alias: &str) -> Result<Vec<String>, EngineError> {
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
        Ok(parts)
    }

    // orderBy with a stable primary-key tie-breaker appended
    fn compile_order_by(&self, ast: &Ast, alias: &str) -> Result<String, EngineError> {
        let parts = self.order_by_terms(ast, alias)?;
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
        // clause is inclusive when the bound is inclusive. NULL is the smallest
        // value (stock ZQL compareValues: null === null, null < anything; SQLite
        // ASC-nulls-first), so a null cursor component uses IS NULL / IS NOT NULL
        // branches rather than `col {><} NULL`, which SQLite evaluates to NULL and
        // would drop every later row.
        let mut clauses: Vec<String> = Vec::new();
        for i in 0..keys.len() {
            let mut ands: Vec<String> = Vec::new();
            // earlier keys equal the cursor (null === null -> IS NULL)
            for (col, _) in keys.iter().take(i) {
                let colq = format!("{}.{}", quote_ident(alias), quote_ident(col));
                match value_for(col).unwrap() {
                    Scalar::Null => ands.push(format!("{colq} IS NULL")),
                    v => {
                        ands.push(format!("{colq} = ?"));
                        self.params.push(scalar_to_sql(v));
                    }
                }
            }
            // key i strictly (or inclusively) past the cursor, null-aware
            let (col, desc) = &keys[i];
            let colq = format!("{}.{}", quote_ident(alias), quote_ident(col));
            let inclusive = i == keys.len() - 1 && !bound.exclusive;
            let v = value_for(col).unwrap();
            let cmp = match (*desc, inclusive, matches!(v, Scalar::Null)) {
                // ascending (null smallest): col sorts after the cursor value
                (false, false, true) => format!("{colq} IS NOT NULL"),
                (false, true, true) => "1".to_string(),
                (false, false, false) => {
                    self.params.push(scalar_to_sql(v));
                    format!("{colq} > ?")
                }
                (false, true, false) => {
                    self.params.push(scalar_to_sql(v));
                    format!("{colq} >= ?")
                }
                // descending (null largest in traversal): col sorts before the value
                (true, false, true) => "0".to_string(),
                (true, true, true) => format!("{colq} IS NULL"),
                (true, false, false) => {
                    self.params.push(scalar_to_sql(v));
                    format!("({colq} < ? OR {colq} IS NULL)")
                }
                (true, true, false) => {
                    self.params.push(scalar_to_sql(v));
                    format!("({colq} <= ? OR {colq} IS NULL)")
                }
            };
            ands.push(cmp);
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

        // a bounded correlated EXISTS: a limit of 0 makes the subquery empty, so
        // existence is a constant (false for EXISTS, true for NOT EXISTS); a limit
        // >= 1 does not change whether ANY row exists and is ignored; a start
        // cursor restricts existence to rows past the cursor; orderBy has no effect
        // on existence.
        if child.limit == Some(0) {
            return Ok(if negated { "1" } else { "0" }.to_string());
        }

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
        if let Some(cursor) = self.compile_start(child, &child_alias)? {
            wheres.push(cursor);
        }
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
    let mut corr: Vec<String> = Vec::new();
    for (pf, cf) in rel.parent_field.iter().zip(rel.child_field.iter()) {
        c.check_column(parent_table, pf)?;
        c.check_column(&child.table, cf)?;
        corr.push(format!(
            "{}.{} = {}.{}",
            quote_ident(&rc),
            quote_ident(cf),
            quote_ident(&rp),
            quote_ident(pf)
        ));
    }
    let ct = quote_ident(&child.table);
    let child_spec = tables
        .get(&child.table)
        .ok_or_else(|| reject(format!("unknown table '{}'", child.table)))?;
    let primary_key = child_spec.primary_key.clone();

    // an UNBOUNDED related child is a row SET: a plain correlated join (row order
    // is immaterial, the client re-sorts). params: the parent subquery (in the
    // JOIN) binds first, then the child filter.
    if child.limit.is_none() && child.start.is_none() {
        let mut params = parent_params.to_vec();
        let where_sql = match &child.where_ {
            Some(cond) => format!(" WHERE {}", c.compile_condition(cond, &child.table, &rc)?),
            None => String::new(),
        };
        params.extend(c.params);
        let sql = format!(
            "SELECT DISTINCT {rc}.* FROM {ct} AS {rc} JOIN ({parent_sql}) AS {rp} ON {on}{where_sql}",
            on = corr.join(" AND "),
        );
        return Ok(CompiledRelated {
            sql,
            params,
            child_table: child.table.clone(),
            primary_key,
        });
    }

    // a BOUNDED related child carries a per-parent limit and/or start cursor
    // (e.g. `.related('tasks', q => q.orderBy('rank','desc').limit(3))` or a
    // `.one()`). correlate each child to the parent row-set with EXISTS, apply the
    // child WHERE + start cursor, then for a limit rank rows WITHIN each
    // parent-correlation partition and keep the top N. params in textual order:
    // parent (EXISTS) -> start cursor -> child where -> limit.
    let mut params = parent_params.to_vec();
    let exists = format!(
        "EXISTS (SELECT 1 FROM ({parent_sql}) AS {rp} WHERE {})",
        corr.join(" AND ")
    );
    let mut inner: Vec<String> = vec![exists];
    if let Some(cursor) = c.compile_start(child, &rc)? {
        inner.push(cursor);
    }
    if let Some(cond) = &child.where_ {
        inner.push(c.compile_condition(cond, &child.table, &rc)?);
    }
    let where_clause = inner.join(" AND ");

    // build the window ordering (no binds) BEFORE consuming c.params for a limit.
    let window = match child.limit {
        Some(limit) => {
            let partition: Vec<String> = rel
                .child_field
                .iter()
                .map(|cf| format!("{}.{}", quote_ident(&rc), quote_ident(cf)))
                .collect();
            let order_terms = c.order_by_terms(child, &rc)?;
            // the ROW_NUMBER rank alias must NOT collide with a real child column,
            // else `{rc}_w.<alias>` resolves to the application column and the
            // `<= limit` filter compares the wrong value (GAP-2c). derive one proven
            // absent from the child's schema.
            let mut rank_alias = String::from("_zsync_rn");
            while child_spec.columns.iter().any(|(col, _)| *col == rank_alias) {
                rank_alias.push('_');
            }
            Some((limit, partition, order_terms, rank_alias))
        }
        None => None,
    };
    params.extend(c.params);

    let sql = if let Some((limit, partition, order_terms, rank_alias)) = window {
        // per-parent window: PARTITION BY the correlation child columns, ORDER BY
        // the child's orderBy + pk tie-breaker, keep rank <= limit. the extra rank
        // column is ignored by the row reader (it reads only schema columns).
        let order_sql = if order_terms.is_empty() {
            String::new()
        } else {
            format!(" ORDER BY {}", order_terms.join(", "))
        };
        params.push(SqlValue::Integer(limit));
        format!(
            "SELECT * FROM (SELECT {rc}.*, ROW_NUMBER() OVER (PARTITION BY {part}{order}) AS {rank_alias} \
             FROM {ct} AS {rc} WHERE {where_clause}) AS {rc}_w WHERE {rc}_w.{rank_alias} <= ?",
            part = partition.join(", "),
            order = order_sql,
        )
    } else {
        // start cursor with no limit: the cursor already bounds the set, so a
        // deduped correlated select suffices.
        format!("SELECT DISTINCT {rc}.* FROM {ct} AS {rc} WHERE {where_clause}")
    };

    Ok(CompiledRelated {
        sql,
        params,
        child_table: child.table.clone(),
        primary_key,
    })
}
