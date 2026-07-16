// shared test host: an in-memory rusqlite SyncDb + Transactor, plus the
// `item` fixture schema and mutators the reference delta suite drives. this is
// the synchronous "host" that composes the push steps exactly the way the
// native host does, so the same driver proves both the engine and the intended
// host composition. rusqlite is a DEV-dependency only.
#![allow(dead_code)]

use rusqlite::Connection;
use rusqlite::types::{Value as Sqlite, ValueRef};
use serde_json::{Value, json};

use sync_core::pull::Caps;
use sync_core::{
    DbError, MutateError, Mutator, Row, SqlValue, SyncDb, Tables, Transactor, Visibility,
    handle_pull, handle_push, init_schema,
};

pub struct TestDb {
    pub conn: Connection,
}

impl TestDb {
    pub fn memory() -> Self {
        TestDb {
            conn: Connection::open_in_memory().expect("open in-memory sqlite"),
        }
    }
}

fn to_sqlite(v: &SqlValue) -> Sqlite {
    match v {
        SqlValue::Null => Sqlite::Null,
        SqlValue::Integer(i) => Sqlite::Integer(*i),
        SqlValue::Real(f) => Sqlite::Real(*f),
        SqlValue::Text(s) => Sqlite::Text(s.clone()),
        SqlValue::Blob(b) => Sqlite::Blob(b.clone()),
    }
}

fn from_ref(v: ValueRef<'_>) -> SqlValue {
    match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(i) => SqlValue::Integer(i),
        ValueRef::Real(f) => SqlValue::Real(f),
        ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

impl SyncDb for TestDb {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError> {
        let binds: Vec<Sqlite> = params.iter().map(to_sqlite).collect();
        self.conn
            .execute(sql, rusqlite::params_from_iter(binds.iter()))
            .map(|_| ())
            .map_err(|e| DbError(format!("{e}: {sql}")))
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        let binds: Vec<Sqlite> = params.iter().map(to_sqlite).collect();
        let mut stmt = self
            .conn
            .prepare(sql)
            .map_err(|e| DbError(format!("{e}: {sql}")))?;
        let columns: std::sync::Arc<[String]> =
            stmt.column_names().iter().map(|s| s.to_string()).collect();
        let ncols = columns.len();
        let mut rows_out = Vec::new();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(binds.iter()))
            .map_err(|e| DbError(e.to_string()))?;
        while let Some(row) = rows.next().map_err(|e| DbError(e.to_string()))? {
            let mut values = Vec::with_capacity(ncols);
            for i in 0..ncols {
                values.push(from_ref(
                    row.get_ref(i).map_err(|e| DbError(e.to_string()))?,
                ));
            }
            rows_out.push(Row {
                columns: columns.clone(),
                values,
            });
        }
        Ok(rows_out)
    }
}

impl Transactor for TestDb {
    fn transaction<T, E>(
        &mut self,
        body: impl FnOnce(&mut dyn SyncDb) -> Result<T, E>,
    ) -> Result<T, E> {
        self.conn.execute_batch("BEGIN").expect("BEGIN");
        let result = body(self);
        match &result {
            Ok(_) => self.conn.execute_batch("COMMIT").expect("COMMIT"),
            Err(_) => self.conn.execute_batch("ROLLBACK").expect("ROLLBACK"),
        }
        result
    }
}

// the `item` table from the reference delta suite (sync-server.test.ts)
pub fn item_tables() -> Tables {
    Tables::from_zero_schema(&json!({
        "tables": {
            "item": {
                "serverName": "item_record",
                "columns": {
                    "id": { "type": "string", "serverName": "item_id" },
                    "label": { "type": "string", "serverName": "item_label" },
                    "rank": { "type": "number", "serverName": "sort_rank" },
                    "done": { "type": "boolean", "serverName": "is_done" },
                    "meta": { "type": "json", "serverName": "metadata_json" },
                },
                "primaryKey": ["id"],
            }
        }
    }))
    .unwrap()
}

// mutators mirroring the reference suite: item.put upserts, item.del deletes,
// item.reject writes then app-errors.
pub struct ItemMutator;

impl Mutator for ItemMutator {
    fn mutate(
        &self,
        db: &mut dyn SyncDb,
        name: &str,
        args: &Value,
        _user_id: &str,
    ) -> Result<(), MutateError> {
        match name {
            "item.put" => {
                let a = args;
                let done = if a.get("done").and_then(Value::as_bool).unwrap_or(false) {
                    SqlValue::Integer(1)
                } else {
                    SqlValue::Integer(0)
                };
                let meta = match a.get("meta") {
                    None | Some(Value::Null) => SqlValue::Null,
                    Some(v) => SqlValue::Text(serde_json::to_string(v).unwrap()),
                };
                db.exec(
                    "INSERT INTO item_record
                     (item_id, item_label, sort_rank, is_done, metadata_json)
                     VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (item_id) DO UPDATE SET
                     item_label = excluded.item_label, sort_rank = excluded.sort_rank,
                     is_done = excluded.is_done, metadata_json = excluded.metadata_json",
                    &[
                        json_to_sql(a.get("id")),
                        json_to_sql(a.get("label")),
                        json_to_sql(a.get("rank")),
                        done,
                        meta,
                    ],
                )
                .map_err(|e| MutateError::Other(e.0))
            }
            "item.del" => db
                .exec(
                    "DELETE FROM item_record WHERE item_id = ?",
                    &[json_to_sql(args.get("id"))],
                )
                .map_err(|e| MutateError::Other(e.0)),
            "item.reject" => {
                db.exec(
                    "INSERT INTO item_record (item_id, item_label, sort_rank, is_done)
                     VALUES ('rejected', 'x', 0, 0)",
                    &[],
                )
                .map_err(|e| MutateError::Other(e.0))?;
                Err(MutateError::app("nope"))
            }
            other => Err(MutateError::Other(format!("unknown mutator {other}"))),
        }
    }
}

// convert a JSON arg value to a bound SQLite value (string/number/null)
pub fn json_to_sql(v: Option<&Value>) -> SqlValue {
    match v {
        None | Some(Value::Null) => SqlValue::Null,
        Some(Value::Bool(b)) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else {
                SqlValue::Real(n.as_f64().unwrap())
            }
        }
        Some(Value::String(s)) => SqlValue::Text(s.clone()),
        Some(other) => SqlValue::Text(other.to_string()),
    }
}

// ---- a full test host: schema init + convenience pull/push --------------

pub struct Host {
    pub db: TestDb,
    pub tables: Tables,
    pub retain: i64,
    pub caps: Caps,
}

impl Host {
    pub fn new(create_item: bool) -> Host {
        let mut db = TestDb::memory();
        if create_item {
            db.exec(
                "CREATE TABLE item_record (item_id TEXT PRIMARY KEY, item_label TEXT NOT NULL,
                 sort_rank REAL NOT NULL, is_done INTEGER NOT NULL, metadata_json TEXT)",
                &[],
            )
            .unwrap();
        }
        Host {
            db,
            tables: item_tables(),
            retain: 4096,
            caps: Caps::default(),
        }
    }

    // run arbitrary sql outside the sync path (seed rows / upstream writes)
    pub fn exec(&mut self, sql: &str) {
        self.db.exec(&item_sql(sql), &[]).unwrap();
    }

    pub fn init(&mut self) {
        init_schema(&mut self.db, &self.tables).unwrap();
    }

    // a pull inside one host transaction, returning the response JSON
    pub fn pull(&mut self, cookie: Value, user: &str) -> Result<Value, sync_core::EngineError> {
        self.pull_as("c1", "g1", cookie, None, user)
    }

    pub fn pull_vis(
        &mut self,
        cookie: Value,
        visible: Option<&Visibility>,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        self.pull_as("c1", "g1", cookie, visible, user)
    }

    pub fn pull_as(
        &mut self,
        client: &str,
        group: &str,
        cookie: Value,
        visible: Option<&Visibility>,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        let body = json!({ "clientID": client, "clientGroupID": group, "cookie": cookie });
        let tables = self.tables.clone();
        let retain = self.retain;
        let caps = self.caps;
        self.db
            .transaction(|db| handle_pull(db, &tables, retain, visible, caps, &body, user))
    }

    // a push through the native convenience driver (composes the push steps)
    pub fn push(
        &mut self,
        mutations: Value,
        group: &str,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        let body = json!({
            "clientGroupID": group,
            "mutations": mutations,
            "pushVersion": 1,
            "requestID": "r",
        });
        let tables = self.tables.clone();
        let retain = self.retain;
        handle_push(&mut self.db, &tables, retain, &ItemMutator, &body, user)
    }

    // push a single custom mutation, returning the pushResponse JSON
    pub fn push_one(
        &mut self,
        name: &str,
        args: Value,
        client: &str,
        group: &str,
        id: i64,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        let mutations = json!([{
            "type": "custom", "id": id, "clientID": client,
            "name": name, "args": [args], "timestamp": 0,
        }]);
        self.push(mutations, group, user)
    }

    // drive a raw push body through the convenience driver
    pub fn push_from_body(
        &mut self,
        body: &Value,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        let tables = self.tables.clone();
        let retain = self.retain;
        handle_push(&mut self.db, &tables, retain, &ItemMutator, body, user)
    }

    // convenience: item.put / item.del as client c1 in group g1 as user u1
    pub fn put(&mut self, _id: &str, args: Value, mutation_id: i64) {
        self.push_one("item.put", args, "c1", "g1", mutation_id, "u1")
            .unwrap();
    }

    pub fn del(&mut self, id: &str, mutation_id: i64) {
        self.push_one(
            "item.del",
            json!({ "id": id }),
            "c1",
            "g1",
            mutation_id,
            "u1",
        )
        .unwrap();
    }

    pub fn watermark(&mut self) -> i64 {
        self.db.transaction(|db| sync_core::watermark(db)).unwrap()
    }

    pub fn floor(&mut self) -> i64 {
        self.db
            .transaction(|db| sync_core::pull::floor(db))
            .unwrap()
    }

    pub fn change_count(&mut self) -> i64 {
        let rows = self
            .db
            .query(
                "SELECT CAST(COUNT(*) AS TEXT) AS n FROM _zsync_changes",
                &[],
            )
            .unwrap();
        match rows.first().and_then(|r| r.values.first()) {
            Some(SqlValue::Text(s)) => s.parse().unwrap_or(0),
            Some(SqlValue::Integer(i)) => *i,
            _ => 0,
        }
    }

    // a live item row as a plain JSON object (None if absent)
    pub fn query_item(&mut self, id: &str) -> Option<Value> {
        let rows = self
            .db
            .query(
                "SELECT item_id AS id, item_label AS label, sort_rank AS rank,
                 is_done AS done, metadata_json AS meta
                 FROM item_record WHERE item_id = ?",
                &[SqlValue::Text(id.to_string())],
            )
            .unwrap();
        let row = rows.first()?;
        let mut obj = serde_json::Map::new();
        for (i, col) in row.columns.iter().enumerate() {
            obj.insert(col.clone(), sql_to_plain_json(&row.values[i]));
        }
        Some(Value::Object(obj))
    }
}

pub fn item_sql(sql: &str) -> String {
    let mut mapped = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;
    while let Some(character) = chars.next() {
        if character == '\'' {
            mapped.push(character);
            if in_string && chars.peek() == Some(&'\'') {
                mapped.push(chars.next().expect("peeked quote"));
            } else {
                in_string = !in_string;
            }
            continue;
        }
        if !in_string && (character.is_ascii_alphabetic() || character == '_') {
            let mut identifier = String::from(character);
            while chars
                .peek()
                .is_some_and(|next| next.is_ascii_alphanumeric() || *next == '_')
            {
                identifier.push(chars.next().expect("peeked identifier character"));
            }
            mapped.push_str(match identifier.as_str() {
                "item" => "item_record",
                "id" => "item_id",
                "label" => "item_label",
                "rank" => "sort_rank",
                "done" => "is_done",
                "meta" => "metadata_json",
                _ => &identifier,
            });
        } else {
            mapped.push(character);
        }
    }
    mapped
}

fn sql_to_plain_json(v: &SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => json!(i),
        SqlValue::Real(f) => json!(f),
        SqlValue::Text(s) => json!(s),
        SqlValue::Blob(b) => json!(String::from_utf8_lossy(b)),
    }
}
