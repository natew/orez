// wake targeting: which client groups can a batch of committed changes affect?
//
// a wake is advisory. it tells a client to pull now instead of at its next
// safety interval, and the pull itself recomputes and authorizes exactly as it
// always does. so the only property this module must hold is SUPERSET: every
// group whose next pull would carry a row change for these writes is in the
// answer. naming extra groups costs one wasted pull each; missing one delays
// that group to its safety pull.
//
// waking every socket on every commit made arrivals cost the sync object
// O(online x commits) pulls, nearly all of them empty: one user's sign-up woke
// every other connected user. this answers the question per changed row
// instead, from facts that do not grow with the number of groups:
//
// - holders: groups whose membership holds the row now (refcount > 0). any
//   update or delete of a delivered row reaches them. rows a query depends on
//   through a related output or a positive EXISTS are members too, so a change
//   that makes a parent leave is covered by the child that carried it.
// - anchors: a query node whose predicate is an AND containing `column =
//   literal` can only be matched by a row carrying those exact values. each
//   registered query stores one key per anchored node, so a row that enters a
//   result finds its groups with an indexed point lookup on its own values.
// - recipes: a node without an anchor is reached through its correlations.
//   `via` follows a positive EXISTS to child rows and evaluates the child's
//   recipe on them (a project row reaches the grant rows naming its users).
//   `parent` walks from an unanchored related child to its parent rows and
//   wakes their holders. recipes depend on query SHAPE, not on literal values,
//   so every group running the same query shares one stored recipe.
// - `all`: a node none of the above can bound registers its group under the
//   key `all` for its table, and any change to that table wakes the group. so
//   does every node below a NOT EXISTS, whose leaving rows are not members.
//
// cost per ingest is O(changed rows x recipes for their tables) point lookups,
// independent of how many groups are connected.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::db::{Row, SqlValue, SyncDb};
use crate::error::EngineError;
use crate::schema::{Tables, quote_ident};
use crate::value::ZeroColumnType;

use super::ast::{Ast, Condition, CorrelatedSubquery, RightVal, Scalar, SimpleOp, ValueRef};
use super::parse_ast;

// bump when the plan a query produces changes, so every stored plan is rebuilt
// from `_zsync_queries` on the next targeting call.
pub(crate) const WAKE_SCHEMA_VERSION: i64 = 1;

const ALL: &str = "all";
// a recipe nests one level per correlation it follows
const MAX_RECIPE_DEPTH: usize = 4;
// a batch this large is a snapshot or a bulk rewrite; everyone pulls anyway
const MAX_CHANGED_ROWS: usize = 512;
// a correlation fanning out wider than this is not worth bounding row by row
const MAX_CORRELATED_ROWS: usize = 256;

fn text(s: impl Into<String>) -> SqlValue {
    SqlValue::Text(s.into())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "k")]
enum Recipe {
    // the row's own values for these columns name the groups
    #[serde(rename = "a")]
    Anchor { cols: Vec<String> },
    // rows of `table` whose `cf` equals this row's `pf`, evaluated with `then`
    #[serde(rename = "v")]
    Via {
        table: String,
        pf: Vec<String>,
        cf: Vec<String>,
        then: Box<Recipe>,
    },
    #[serde(rename = "u")]
    Any { of: Vec<Recipe> },
    // parent rows of `table` whose `pf` equals this row's `cf`: their holders,
    // plus `then` on them when this row can make a parent enter (EXISTS)
    #[serde(rename = "p")]
    Parent {
        table: String,
        pf: Vec<String>,
        cf: Vec<String>,
        then: Option<Box<Recipe>>,
    },
}

// the keys and recipes one registered query contributes
#[derive(Debug, Default, PartialEq, Eq)]
pub struct WakePlan {
    // (table, key): an anchor key, or `all`
    pub keys: BTreeSet<(String, String)>,
    // (table, recipe json), shared by every group with the same query shape
    pub recipes: BTreeSet<(String, String)>,
}

// canonical form of a value under SQLite `=`: numbers compare numerically
// (1 = 1.0), booleans are stored 0/1, text compares bytewise
fn canonical_number(f: f64) -> String {
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
        format!("n{}", f as i64)
    } else {
        format!("n{f:?}")
    }
}

fn canonical_scalar(ty: ZeroColumnType, value: &Scalar) -> Option<String> {
    match (ty, value) {
        (ZeroColumnType::String, Scalar::Text(s)) => Some(format!("s{s}")),
        (ZeroColumnType::Number, Scalar::Int(i)) => Some(format!("n{i}")),
        (ZeroColumnType::Number, Scalar::Float(f)) => Some(canonical_number(*f)),
        (ZeroColumnType::Boolean, Scalar::Bool(b)) => Some(format!("n{}", u8::from(*b))),
        _ => None,
    }
}

fn canonical_stored(ty: ZeroColumnType, value: &SqlValue) -> Option<String> {
    match (ty, value) {
        (ZeroColumnType::String, SqlValue::Text(s)) => Some(format!("s{s}")),
        (ZeroColumnType::Number | ZeroColumnType::Boolean, SqlValue::Integer(i)) => {
            Some(format!("n{i}"))
        }
        (ZeroColumnType::Number, SqlValue::Real(f)) => Some(canonical_number(*f)),
        _ => None,
    }
}

fn anchor_key(cols: &[String], values: &[String]) -> String {
    json!([cols, values]).to_string()
}

// the column type a `column = literal` conjunct can be anchored on: known,
// not encrypted (its stored bytes are ciphertext), and not json
fn anchor_type(tables: &Tables, table: &str, col: &str) -> Option<ZeroColumnType> {
    let spec = tables.get(table)?;
    if spec.encrypted_columns.contains(col) {
        return None;
    }
    match spec.column_type(col)? {
        ZeroColumnType::Json | ZeroColumnType::Null => None,
        ty => Some(ty),
    }
}

// top-level AND conjuncts, nested ANDs flattened
fn conjuncts(cond: &Condition) -> Vec<&Condition> {
    match cond {
        Condition::And(parts) => parts.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

// `column = literal` with a literal of the column's own type
fn equality(tables: &Tables, table: &str, cond: &Condition) -> Option<(String, String)> {
    let Condition::Simple {
        op: SimpleOp::Eq,
        left: ValueRef::Column(col),
        right: RightVal::Scalar(value),
    } = cond
    else {
        return None;
    };
    let ty = anchor_type(tables, table, col)?;
    Some((col.clone(), canonical_scalar(ty, value)?))
}

// `column IN (literals)`, every literal of the column's own type
fn membership(tables: &Tables, table: &str, cond: &Condition) -> Option<(String, Vec<String>)> {
    let Condition::Simple {
        op: SimpleOp::In,
        left: ValueRef::Column(col),
        right: RightVal::List(values),
    } = cond
    else {
        return None;
    };
    let ty = anchor_type(tables, table, col)?;
    let values = values
        .iter()
        .map(|value| canonical_scalar(ty, value))
        .collect::<Option<Vec<_>>>()?;
    Some((col.clone(), values))
}

// a recipe for rows of one node's table, and the anchor keys on that same
// table it relies on (with this node's literals)
type Resolved = (Recipe, Vec<String>);

// an anchor built from a set of conjuncts: every `=` column together, or failing
// that the first IN (one key per listed value)
fn anchor_of(tables: &Tables, table: &str, conds: &[&Condition]) -> Option<Resolved> {
    let mut eqs: BTreeMap<String, String> = BTreeMap::new();
    for cond in conds {
        if let Some((col, value)) = equality(tables, table, cond) {
            eqs.entry(col).or_insert(value);
        }
    }
    if !eqs.is_empty() {
        let cols: Vec<String> = eqs.keys().cloned().collect();
        let values: Vec<String> = eqs.into_values().collect();
        let key = anchor_key(&cols, &values);
        return Some((Recipe::Anchor { cols }, vec![key]));
    }
    for cond in conds {
        if let Some((col, values)) = membership(tables, table, cond) {
            let cols = vec![col];
            let keys = values
                .iter()
                .map(|value| anchor_key(&cols, std::slice::from_ref(value)))
                .collect();
            return Some((Recipe::Anchor { cols }, keys));
        }
    }
    None
}

// a necessary condition for a row of `table` to satisfy `conds` (an AND)
fn resolve_conjunction(
    tables: &Tables,
    table: &str,
    conds: &[&Condition],
    depth: usize,
) -> Option<Resolved> {
    if let Some(anchor) = anchor_of(tables, table, conds) {
        return Some(anchor);
    }
    conds
        .iter()
        .find_map(|cond| resolve_condition(tables, table, cond, depth))
}

fn resolve_condition(
    tables: &Tables,
    table: &str,
    cond: &Condition,
    depth: usize,
) -> Option<Resolved> {
    match cond {
        Condition::Exists {
            negated: false,
            related,
        } => {
            let (then, _) = resolve_node(tables, &related.subquery, depth + 1)?;
            Some((
                Recipe::Via {
                    table: related.subquery.table.clone(),
                    pf: related.parent_field.clone(),
                    cf: related.child_field.clone(),
                    then: Box::new(then),
                },
                Vec::new(),
            ))
        }
        // every branch must be bounded: the row satisfies at least one
        Condition::Or(branches) => {
            let mut of = Vec::new();
            let mut keys = Vec::new();
            for branch in branches {
                let (recipe, branch_keys) =
                    resolve_conjunction(tables, table, &conjuncts(branch), depth)?;
                of.push(recipe);
                keys.extend(branch_keys);
            }
            if of.is_empty() {
                return None;
            }
            Some((Recipe::Any { of }, keys))
        }
        Condition::And(_) => resolve_conjunction(tables, table, &conjuncts(cond), depth),
        _ => None,
    }
}

fn resolve_node(tables: &Tables, node: &Ast, depth: usize) -> Option<Resolved> {
    if depth > MAX_RECIPE_DEPTH {
        return None;
    }
    let cond = node.where_.as_ref()?;
    resolve_conjunction(tables, &node.table, &conjuncts(cond), depth)
}

struct ParentLink {
    table: String,
    pf: Vec<String>,
    cf: Vec<String>,
    // an EXISTS child can make its parent enter; a related output cannot
    filter: bool,
    // the parent's own recipe, for a filter child that is itself unbounded
    recipe: Option<Recipe>,
}

fn plan_node(
    tables: &Tables,
    node: &Ast,
    negated: bool,
    parent: Option<ParentLink>,
    plan: &mut WakePlan,
) {
    let resolved = if negated {
        None
    } else {
        resolve_node(tables, node, 0)
    };
    let recipe = resolved.as_ref().map(|(recipe, _)| recipe.clone());
    match (&resolved, negated, &parent) {
        (_, true, _) => {
            plan.keys.insert((node.table.clone(), ALL.to_string()));
        }
        (Some((recipe, keys)), false, _) => {
            plan.recipes
                .insert((node.table.clone(), serde_json::to_string(recipe).unwrap()));
            for key in keys {
                plan.keys.insert((node.table.clone(), key.clone()));
            }
        }
        (None, false, Some(link)) if !link.filter || link.recipe.is_some() => {
            let then = if link.filter {
                link.recipe.clone().map(Box::new)
            } else {
                None
            };
            let recipe = Recipe::Parent {
                table: link.table.clone(),
                pf: link.pf.clone(),
                cf: link.cf.clone(),
                then,
            };
            plan.recipes
                .insert((node.table.clone(), serde_json::to_string(&recipe).unwrap()));
        }
        (None, false, _) => {
            plan.keys.insert((node.table.clone(), ALL.to_string()));
        }
    }
    let link = |sub: &CorrelatedSubquery, filter: bool| ParentLink {
        table: node.table.clone(),
        pf: sub.parent_field.clone(),
        cf: sub.child_field.clone(),
        filter,
        recipe: recipe.clone(),
    };
    if let Some(cond) = &node.where_ {
        let mut exists: Vec<(bool, &CorrelatedSubquery)> = Vec::new();
        collect_exists(cond, &mut exists);
        for (sub_negated, sub) in exists {
            let child_negated = negated || sub_negated;
            plan_node(
                tables,
                &sub.subquery,
                child_negated,
                Some(link(sub, true)),
                plan,
            );
        }
    }
    for sub in &node.related {
        plan_node(tables, &sub.subquery, negated, Some(link(sub, false)), plan);
    }
}

fn collect_exists<'a>(cond: &'a Condition, out: &mut Vec<(bool, &'a CorrelatedSubquery)>) {
    match cond {
        Condition::Exists { negated, related } => out.push((*negated, related)),
        Condition::And(parts) | Condition::Or(parts) => {
            for part in parts {
                collect_exists(part, out);
            }
        }
        Condition::Simple { .. } => {}
    }
}

// the wake plan for one transformed query AST
pub fn plan_query(tables: &Tables, ast: &Ast) -> WakePlan {
    let mut plan = WakePlan::default();
    plan_node(tables, ast, false, None, &mut plan);
    plan
}

pub(crate) fn init_wake_schema(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_wake_keys (
            clientGroupID TEXT NOT NULL,
            hash TEXT NOT NULL,
            wakeTable TEXT NOT NULL,
            wakeKey TEXT NOT NULL,
            PRIMARY KEY (clientGroupID, hash, wakeTable, wakeKey)
        )",
        &[],
    )?;
    db.exec(
        "CREATE INDEX IF NOT EXISTS _zsync_wake_keys_lookup
         ON _zsync_wake_keys (wakeTable, wakeKey)",
        &[],
    )?;
    // shape-level and insert-only: a recipe no query uses any more costs one
    // empty evaluation, and there are only as many as there are query shapes
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_wake_recipes (
            wakeTable TEXT NOT NULL,
            recipe TEXT NOT NULL,
            PRIMARY KEY (wakeTable, recipe)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_wake_meta (
            lock INTEGER PRIMARY KEY CHECK (lock = 1),
            version INTEGER NOT NULL
        )",
        &[],
    )?;
    // who holds a row: the holders lookup a targeting pass makes per change
    db.exec(
        "CREATE INDEX IF NOT EXISTS _zsync_row_refs_holders
         ON _zsync_row_refs (rowTable, rowPk) WHERE refcount > 0",
        &[],
    )?;
    Ok(())
}

// replace one query's stored plan. called when its AST or transform version
// changes, never for an unchanged re-registration.
pub(crate) fn store_plan(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    hash: &str,
    ast: &Ast,
) -> Result<(), EngineError> {
    forget_query(db, group, Some(hash))?;
    let plan = plan_query(tables, ast);
    for (table, key) in &plan.keys {
        db.exec(
            "INSERT OR IGNORE INTO _zsync_wake_keys (clientGroupID, hash, wakeTable, wakeKey)
             VALUES (?, ?, ?, ?)",
            &[text(group), text(hash), text(table), text(key)],
        )?;
    }
    for (table, recipe) in &plan.recipes {
        db.exec(
            "INSERT OR IGNORE INTO _zsync_wake_recipes (wakeTable, recipe) VALUES (?, ?)",
            &[text(table), text(recipe)],
        )?;
    }
    Ok(())
}

// drop the keys of one query, or of every query in the group
pub(crate) fn forget_query(
    db: &mut dyn SyncDb,
    group: &str,
    hash: Option<&str>,
) -> Result<(), EngineError> {
    match hash {
        Some(hash) => db.exec(
            "DELETE FROM _zsync_wake_keys WHERE clientGroupID = ? AND hash = ?",
            &[text(group), text(hash)],
        )?,
        None => db.exec(
            "DELETE FROM _zsync_wake_keys WHERE clientGroupID = ?",
            &[text(group)],
        )?,
    }
    Ok(())
}

// rebuild every stored plan once per WAKE_SCHEMA_VERSION, so queries
// registered before this engine (or under an older plan format) are targeted
fn ensure_plans(db: &mut dyn SyncDb, tables: &Tables) -> Result<(), EngineError> {
    let current = db
        .query(
            "SELECT CAST(version AS TEXT) AS v FROM _zsync_wake_meta WHERE lock = 1",
            &[],
        )?
        .first()
        .and_then(|row| match row.get("v") {
            Some(SqlValue::Text(v)) => v.parse::<i64>().ok(),
            _ => None,
        });
    if current == Some(WAKE_SCHEMA_VERSION) {
        return Ok(());
    }
    db.exec("DELETE FROM _zsync_wake_keys", &[])?;
    db.exec("DELETE FROM _zsync_wake_recipes", &[])?;
    let rows = db.query("SELECT clientGroupID, hash, ast FROM _zsync_queries", &[])?;
    for row in &rows {
        let (Some(SqlValue::Text(group)), Some(SqlValue::Text(hash)), Some(SqlValue::Text(ast))) =
            (row.get("clientGroupID"), row.get("hash"), row.get("ast"))
        else {
            continue;
        };
        let ast: Value = serde_json::from_str(ast)
            .map_err(|e| EngineError::internal(format!("stored query ast is not json: {e}")))?;
        store_plan(db, tables, group, hash, &parse_ast(&ast)?)?;
    }
    db.exec(
        "INSERT INTO _zsync_wake_meta (lock, version) VALUES (1, ?)
         ON CONFLICT (lock) DO UPDATE SET version = excluded.version",
        &[SqlValue::Integer(WAKE_SCHEMA_VERSION)],
    )?;
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum WakeTargets {
    // every connected client
    All,
    // the clients of the groups a change can reach
    Clients(BTreeSet<String>),
}

struct Targeting<'a> {
    db: &'a mut dyn SyncDb,
    tables: &'a Tables,
    groups: BTreeSet<String>,
    all: bool,
}

impl Targeting<'_> {
    fn add_groups(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), EngineError> {
        for row in self.db.query(sql, params)? {
            if let Some(SqlValue::Text(group)) = row.get("clientGroupID") {
                self.groups.insert(group.clone());
            }
        }
        Ok(())
    }

    fn keyed(&mut self, table: &str, key: &str) -> Result<(), EngineError> {
        self.add_groups(
            "SELECT DISTINCT clientGroupID FROM _zsync_wake_keys
             WHERE wakeTable = ? AND wakeKey = ?",
            &[text(table), text(key)],
        )
    }

    fn holders(&mut self, table: &str, pk: &str) -> Result<(), EngineError> {
        self.add_groups(
            "SELECT DISTINCT clientGroupID FROM _zsync_row_refs
             WHERE rowTable = ? AND rowPk = ? AND refcount > 0",
            &[text(table), text(pk)],
        )
    }

    fn physical_column<'b>(&'b self, table: &str, col: &'b str) -> &'b str {
        self.tables.physical_column(table, col).unwrap_or(col)
    }

    fn value<'r>(&self, table: &str, row: &'r Row, col: &str) -> Option<&'r SqlValue> {
        row.get(self.physical_column(table, col))
            .filter(|value| !matches!(value, SqlValue::Null))
    }

    // rows of `table` whose `cols` equal `values`, bounded; None when wider
    fn correlated(
        &mut self,
        table: &str,
        cols: &[String],
        values: Vec<SqlValue>,
    ) -> Result<Option<Vec<Row>>, EngineError> {
        let Some(physical) = self.tables.physical_name(table) else {
            return Ok(Some(Vec::new()));
        };
        let wheres = cols
            .iter()
            .map(|col| format!("{} = ?", quote_ident(self.physical_column(table, col))))
            .collect::<Vec<_>>()
            .join(" AND ");
        let rows = self.db.query(
            &format!(
                "SELECT * FROM {} WHERE {wheres} LIMIT {}",
                quote_ident(physical),
                MAX_CORRELATED_ROWS + 1
            ),
            &values,
        )?;
        Ok((rows.len() <= MAX_CORRELATED_ROWS).then_some(rows))
    }

    fn bind(&self, table: &str, row: &Row, cols: &[String]) -> Option<Vec<SqlValue>> {
        cols.iter()
            .map(|col| self.value(table, row, col).cloned())
            .collect()
    }

    fn evaluate(&mut self, recipe: &Recipe, table: &str, row: &Row) -> Result<(), EngineError> {
        if self.all {
            return Ok(());
        }
        match recipe {
            Recipe::Anchor { cols } => {
                let Some(spec) = self.tables.get(table) else {
                    return Ok(());
                };
                let values = cols
                    .iter()
                    .map(|col| {
                        let ty = spec.column_type(col)?;
                        canonical_stored(ty, self.value(table, row, col)?)
                    })
                    .collect::<Option<Vec<_>>>();
                // a null or untyped value cannot equal the anchor's literal
                if let Some(values) = values {
                    self.keyed(table, &anchor_key(cols, &values))?;
                }
            }
            Recipe::Via {
                table: child,
                pf,
                cf,
                then,
            } => {
                let Some(values) = self.bind(table, row, pf) else {
                    return Ok(());
                };
                match self.correlated(child, cf, values)? {
                    Some(rows) => {
                        for child_row in &rows {
                            self.evaluate(then, child, child_row)?;
                        }
                    }
                    None => self.all = true,
                }
            }
            Recipe::Any { of } => {
                for recipe in of {
                    self.evaluate(recipe, table, row)?;
                }
            }
            Recipe::Parent {
                table: parent,
                pf,
                cf,
                then,
            } => {
                let Some(values) = self.bind(table, row, cf) else {
                    return Ok(());
                };
                let Some(rows) = self.correlated(parent, pf, values)? else {
                    self.all = true;
                    return Ok(());
                };
                let Some(spec) = self.tables.get(parent) else {
                    return Ok(());
                };
                for parent_row in &rows {
                    let mut pk = serde_json::Map::new();
                    for col in &spec.primary_key {
                        let value = match parent_row.get(self.physical_column(parent, col)) {
                            Some(SqlValue::Integer(i)) => json!(i),
                            Some(SqlValue::Real(f)) => crate::value::f64_to_json(*f),
                            Some(SqlValue::Text(s)) => json!(s),
                            _ => Value::Null,
                        };
                        pk.insert(col.clone(), value);
                    }
                    let key = crate::value::canonical_pk(spec, &Value::Object(pk));
                    self.holders(parent, &key)?;
                    if let Some(then) = then {
                        self.evaluate(then, parent, parent_row)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn read_row(&mut self, table: &str, pk: &Value) -> Result<Option<Row>, EngineError> {
        let (Some(spec), Some(physical)) =
            (self.tables.get(table), self.tables.physical_name(table))
        else {
            return Ok(None);
        };
        let mut wheres = Vec::new();
        let mut params = Vec::new();
        for col in &spec.primary_key {
            wheres.push(format!(
                "{} = ?",
                quote_ident(self.physical_column(table, col))
            ));
            params.push(super::membership::json_pk_to_sql(pk.get(col)));
        }
        let mut rows = self.db.query(
            &format!(
                "SELECT * FROM {} WHERE {} LIMIT 1",
                quote_ident(physical),
                wheres.join(" AND ")
            ),
            &params,
        )?;
        Ok(rows.pop())
    }
}

// the clients to wake for every change committed after `since`. the host
// calls this with a transaction open, after an ingest applied its batch.
pub fn wake_targets(
    db: &mut dyn SyncDb,
    tables: &Tables,
    since: i64,
) -> Result<WakeTargets, EngineError> {
    ensure_plans(db, tables)?;
    let scanned = crate::ledger::scan_since(db, since)?;
    if scanned.reset || scanned.changes.len() > MAX_CHANGED_ROWS {
        return Ok(WakeTargets::All);
    }
    let mut recipes: BTreeMap<String, Vec<Recipe>> = BTreeMap::new();
    for row in db.query("SELECT wakeTable, recipe FROM _zsync_wake_recipes", &[])? {
        let (Some(SqlValue::Text(table)), Some(SqlValue::Text(recipe))) =
            (row.get("wakeTable"), row.get("recipe"))
        else {
            continue;
        };
        // a recipe written by another plan format is rebuilt with the version
        // bump; one this build cannot read is skipped rather than guessed at
        if let Ok(recipe) = serde_json::from_str::<Recipe>(recipe) {
            recipes.entry(table.clone()).or_default().push(recipe);
        }
    }
    let mut targeting = Targeting {
        db,
        tables,
        groups: BTreeSet::new(),
        all: false,
    };
    for (table, pk_text) in &scanned.changes {
        let Some(spec) = tables.get(table) else {
            continue;
        };
        let canonical = crate::value::canonical_pk_text(spec, pk_text);
        targeting.holders(table, &canonical)?;
        targeting.keyed(table, ALL)?;
        let Some(table_recipes) = recipes.get(table) else {
            continue;
        };
        let pk: Value = serde_json::from_str(pk_text).unwrap_or(Value::Null);
        // a deleted row enters nothing; its holders were named above
        let Some(row) = targeting.read_row(table, &pk)? else {
            continue;
        };
        for recipe in table_recipes {
            targeting.evaluate(recipe, table, &row)?;
        }
        if targeting.all {
            return Ok(WakeTargets::All);
        }
    }
    let groups = targeting.groups;
    let mut clients = BTreeSet::new();
    if !groups.is_empty() {
        let list = serde_json::to_string(&groups).unwrap();
        for row in db.query(
            "SELECT clientID FROM _zsync_clients
             WHERE clientGroupID IN (SELECT value FROM json_each(?))",
            &[text(list)],
        )? {
            if let Some(SqlValue::Text(client)) = row.get("clientID") {
                clients.insert(client.clone());
            }
        }
    }
    Ok(WakeTargets::Clients(clients))
}
