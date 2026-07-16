// HIGH-3 regression: DDL injection via trigger identifiers. table/column names
// reach CREATE TRIGGER statements, so a hostile name must be neutralized two
// ways: the trigger identifier is quoted/escaped (so no name can break out of
// the DDL), and from_zero_schema rejects names that are not plain identifiers at
// ingest.
mod common;

use common::TestDb;
use serde_json::json;

use sync_core::pull::{Caps, Visibility, VisibleFilter};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables, Transactor, handle_pull, init_schema, trigger_ddl};

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
fn initializes_triggers_for_soot_server_names() {
    let schema = json!({
        "tables": {
            "userState": {
                "name": "userState",
                "serverName": "user_state",
                "columns": {
                    "userId": {
                        "type": "string",
                        "serverName": "user_id",
                    },
                    "monthlyTokens": {
                        "type": "number",
                        "serverName": "monthly_tokens",
                    },
                },
                "primaryKey": ["userId"],
            }
        }
    });
    let tables = Tables::from_zero_schema(&schema).unwrap();
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE user_state (user_id TEXT PRIMARY KEY, monthly_tokens REAL NOT NULL)",
        &[],
    )
    .unwrap();

    init_schema(&mut db, &tables).unwrap();
    db.exec("INSERT INTO user_state VALUES ('u1', 42)", &[])
        .unwrap();

    let changes = db
        .query("SELECT tableName, pk FROM _zsync_changes", &[])
        .unwrap();
    assert_eq!(changes.len(), 1);
    assert!(matches!(
        changes[0].get("tableName"),
        Some(sync_core::SqlValue::Text(table)) if table == "userState"
    ));
    assert!(matches!(
        changes[0].get("pk"),
        Some(sync_core::SqlValue::Text(pk)) if pk == r#"{"userId":"u1"}"#
    ));
}

#[test]
fn server_names_fall_back_to_logical_names_and_reject_physical_collisions() {
    let identity = Tables::from_zero_schema(&json!({
        "tables": {
            "userState": {
                "columns": { "userId": { "type": "string" } },
                "primaryKey": ["userId"],
            }
        }
    }))
    .unwrap();
    assert_eq!(identity.physical_name("userState"), Some("userState"));
    assert_eq!(
        identity.physical_column("userState", "userId"),
        Some("userId")
    );

    let duplicate_tables = Tables::from_zero_schema(&json!({
        "tables": {
            "first": {
                "serverName": "record",
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"],
            },
            "second": {
                "serverName": "RECORD",
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"],
            },
        }
    }))
    .unwrap_err();
    assert!(duplicate_tables.contains("duplicate physical table mapping"));

    let duplicate_columns = Tables::from_zero_schema(&json!({
        "tables": {
            "record": {
                "columns": {
                    "first": { "type": "string", "serverName": "value" },
                    "second": { "type": "string", "serverName": "VALUE" },
                },
                "primaryKey": ["first"],
            }
        }
    }))
    .unwrap_err();
    assert!(duplicate_columns.contains("duplicate physical column mapping"));
}

#[test]
fn mapped_visibility_fragments_use_logical_tables_and_columns() {
    let tables = Tables::from_zero_schema(&json!({
        "tables": {
            "project": {
                "serverName": "project_record",
                "columns": {
                    "id": { "type": "string", "serverName": "project_id" },
                    "ownerId": { "type": "string", "serverName": "owner_id" },
                },
                "primaryKey": ["id"],
            },
            "member": {
                "serverName": "project_member",
                "columns": {
                    "id": { "type": "string", "serverName": "member_id" },
                    "projectId": { "type": "string", "serverName": "project_id" },
                    "userId": { "type": "string", "serverName": "user_id" },
                },
                "primaryKey": ["id"],
            },
        }
    }))
    .unwrap();
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE project_record (project_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE project_member (
            member_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            user_id TEXT NOT NULL
        )",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO project_record VALUES ('owned', 'u1'), ('shared', 'u2'), ('hidden', 'u2')",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO project_member VALUES ('m1', 'shared', 'u1')",
        &[],
    )
    .unwrap();
    init_schema(&mut db, &tables).unwrap();
    let visibility = Visibility {
        row_local: false,
        filter: Box::new(|table, user| {
            (table == "project").then(|| VisibleFilter {
                sql: "project.ownerId = ? OR EXISTS (
                    SELECT 1 FROM member access
                    WHERE access.projectId = project.id AND access.userId = ?
                )"
                .into(),
                params: vec![
                    SqlValue::Text(user.to_string()),
                    SqlValue::Text(user.to_string()),
                ],
            })
        }),
    };
    let response = db
        .transaction(|database| {
            handle_pull(
                database,
                &tables,
                4096,
                Some(&visibility),
                Caps::default(),
                &json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": null }),
                "u1",
            )
        })
        .unwrap();
    let mut project_ids = response["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|operation| operation["op"] == "put" && operation["tableName"] == "project")
        .map(|operation| operation["value"]["id"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    project_ids.sort();
    assert_eq!(project_ids, vec!["owned", "shared"]);
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
