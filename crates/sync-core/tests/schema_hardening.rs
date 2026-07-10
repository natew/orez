// HIGH-3 regression: DDL injection via trigger identifiers. table/column names
// reach CREATE TRIGGER statements, so a hostile name must be neutralized two
// ways: the trigger identifier is quoted/escaped (so no name can break out of
// the DDL), and from_zero_schema rejects names that are not plain identifiers at
// ingest.
mod common;

use common::TestDb;
use serde_json::json;

use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SyncDb, Tables, init_schema, trigger_ddl};

// the classic breakout payload: a table name that, unescaped, closes the trigger
// name and appends an injected trigger that deletes from a victim table.
const INJECT: &str = r#"x" AFTER INSERT ON "victim" BEGIN DELETE FROM "victim"; END; --"#;

#[test]
fn from_zero_schema_rejects_injection_shaped_table_name() {
    let schema = json!({
        "tables": {
            INJECT: {
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"],
            }
        }
    });
    let err = Tables::from_zero_schema(&schema).unwrap_err();
    assert!(
        err.contains("not a valid identifier"),
        "expected identifier rejection, got: {err}"
    );
}

#[test]
fn from_zero_schema_rejects_injection_shaped_column_name() {
    let schema = json!({
        "tables": {
            "issue": {
                "columns": { "id\" , 1); DROP TABLE issue; --": { "type": "string" } },
                "primaryKey": ["id"],
            }
        }
    });
    let err = Tables::from_zero_schema(&schema).unwrap_err();
    assert!(err.contains("not a valid identifier"), "got: {err}");
}

#[test]
fn from_zero_schema_accepts_normal_camelcase_schema() {
    // the guard must not reject legitimate Zero schema names (camelCase tables +
    // columns, underscores).
    let schema = json!({
        "tables": {
            "serverMember": {
                "columns": {
                    "id": { "type": "string" },
                    "serverId": { "type": "string" },
                    "user_id": { "type": "string" },
                },
                "primaryKey": ["id"],
            }
        }
    });
    let tables = Tables::from_zero_schema(&schema).unwrap();
    assert!(tables.get("serverMember").is_some());
}

#[test]
fn trigger_name_is_quoted_so_a_hostile_table_cannot_inject() {
    // even when Tables is built directly (bypassing from_zero_schema), the trigger
    // NAME is quote-escaped, so installing triggers for a hostile table name can
    // never create a rogue trigger on another table. we prove it at runtime: the
    // `victim` table keeps its rows after an INSERT that a successful injection
    // would have turned into a DELETE.
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE victim (id TEXT PRIMARY KEY)", &[])
        .unwrap();
    db.exec("INSERT INTO victim VALUES ('keep')", &[]).unwrap();

    let tables = Tables::new().with(
        INJECT,
        TableSpec {
            columns: vec![("id".into(), ZeroColumnType::String)],
            primary_key: vec!["id".into()],
        },
    );
    // the hostile name resolves to one escaped identifier naming a table that does
    // not exist, so init_schema may error; what matters is that no injected
    // trigger on `victim` is installed.
    let _ = init_schema(&mut db, &tables);

    db.exec("INSERT INTO victim VALUES ('after')", &[]).unwrap();
    let rows = db.query("SELECT id FROM victim", &[]).unwrap();
    assert_eq!(
        rows.len(),
        2,
        "an injected AFTER INSERT trigger on victim would have deleted its rows"
    );

    // and the generated DDL escapes the embedded quote in the trigger identifier
    // (doubled), never leaving the vulnerable bare-quote breakout. the fixed form
    // is `_zsync_tr_x"" AFTER`; the vulnerable form was `_zsync_tr_x" AFTER`.
    let ddl = trigger_ddl(&tables).join("\n");
    assert!(
        !ddl.contains("_zsync_tr_x\" AFTER"),
        "trigger name broke out of its quotes:\n{ddl}"
    );
    assert!(
        ddl.contains("_zsync_tr_x\"\" AFTER"),
        "expected doubled quote"
    );
}
