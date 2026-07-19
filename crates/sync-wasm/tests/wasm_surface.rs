#![cfg(target_arch = "wasm32")]

use js_sys::Reflect;
use serde::Serialize;
use serde_json::{Value, json};
use sync_wasm::{
    JsSyncDb, engine_assemble_push_response, engine_finalize, engine_handle_pull,
    engine_init_schema, engine_preflight, engine_push_validate,
};
use wasm_bindgen::{JsCast, JsValue, prelude::wasm_bindgen};
use wasm_bindgen_test::*;

#[wasm_bindgen(module = "/tests/node_sqlite.js")]
extern "C" {
    #[wasm_bindgen(js_name = createDb)]
    fn create_db() -> JsValue;

    #[wasm_bindgen(js_name = execSql)]
    fn exec_sql(db: &JsSyncDb, sql: &str);

    #[wasm_bindgen(js_name = querySql)]
    fn query_sql(db: &JsSyncDb, sql: &str) -> JsValue;
}

fn db() -> JsSyncDb {
    create_db().unchecked_into()
}

fn to_js(value: &impl Serialize) -> JsValue {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .unwrap()
}

fn from_js(value: JsValue) -> Value {
    serde_wasm_bindgen::from_value(value).unwrap()
}

fn schema() -> Value {
    json!({
        "tables": {
            "item": {
                "columns": {
                    "id": { "type": "string" },
                    "label": { "type": "string" },
                },
                "primaryKey": ["id"],
            },
        },
    })
}

fn initialize(db: &JsSyncDb) {
    exec_sql(
        db,
        "CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL)",
    );
    engine_init_schema(db, to_js(&schema())).unwrap();
}

fn status(error: &JsValue) -> u16 {
    Reflect::get(error, &JsValue::from_str("status"))
        .unwrap()
        .as_f64()
        .unwrap() as u16
}

fn message(error: &JsValue) -> String {
    Reflect::get(error, &JsValue::from_str("message"))
        .unwrap()
        .as_string()
        .unwrap()
}

#[wasm_bindgen_test]
fn push_and_pull_round_trip_through_wasm_exports() {
    let db = db();
    initialize(&db);
    let push = json!({
        "clientGroupID": "group-1",
        "mutations": [{
            "type": "custom",
            "id": 1,
            "clientID": "writer-1",
            "name": "item.create",
            "args": [{ "id": "i1", "label": "from wasm" }],
        }],
        "pushVersion": 1,
    });

    let plan = from_js(engine_push_validate(to_js(&push)).unwrap());
    assert_eq!(plan["kind"], "process");
    assert_eq!(plan["mutations"][0]["id"], "1");

    exec_sql(&db, "BEGIN");
    let decision = from_js(engine_preflight(&db, "group-1", "writer-1", "1", "user-1").unwrap());
    assert_eq!(decision, json!({ "kind": "applied" }));
    exec_sql(
        &db,
        "INSERT INTO item (id, label) VALUES ('i1', 'from wasm')",
    );
    engine_finalize(&db, "group-1", "writer-1", "1").unwrap();
    exec_sql(&db, "COMMIT");

    let response = from_js(
        engine_assemble_push_response(to_js(&json!([{
            "clientID": "writer-1",
            "id": "1",
            "result": {},
        }])))
        .unwrap(),
    );
    assert_eq!(response["pushResponse"]["mutations"][0]["id"]["id"], 1);

    exec_sql(&db, "BEGIN");
    let pull = engine_handle_pull(
        &db,
        to_js(&schema()),
        JsValue::NULL,
        to_js(&json!({ "maxChangeRows": 100, "maxChangeBytes": 65_536 })),
        "4096",
        to_js(&json!({
            "clientID": "reader-1",
            "clientGroupID": "group-1",
            "cookie": null,
        })),
        "user-1",
    )
    .unwrap();
    exec_sql(&db, "COMMIT");
    let pull = from_js(pull);
    assert_eq!(pull["lastMutationIDChanges"]["writer-1"], 1);
    assert!(
        pull["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| {
                operation["op"] == "put"
                    && operation["tableName"] == "item"
                    && operation["value"] == json!({ "id": "i1", "label": "from wasm" })
            })
    );
}

#[wasm_bindgen_test]
fn engine_errors_keep_400_and_403_statuses_without_panicking() {
    let db = db();
    initialize(&db);

    exec_sql(&db, "BEGIN");
    let out_of_order = engine_preflight(&db, "group-1", "writer-1", "2", "user-1")
        .expect_err("skipping mutation 1 must be a 400 error");
    exec_sql(&db, "ROLLBACK");
    assert_eq!(status(&out_of_order), 400);

    exec_sql(&db, "BEGIN");
    engine_preflight(&db, "group-1", "writer-1", "1", "user-1").unwrap();
    engine_finalize(&db, "group-1", "writer-1", "1").unwrap();
    exec_sql(&db, "COMMIT");

    exec_sql(&db, "BEGIN");
    let forbidden = engine_preflight(&db, "group-1", "writer-2", "1", "user-2")
        .expect_err("a different user must not claim an owned client group");
    exec_sql(&db, "ROLLBACK");
    assert_eq!(status(&forbidden), 403);
}

#[wasm_bindgen_test]
fn encrypted_visibility_is_rejected_while_projection_stays_opaque() {
    let db = db();
    let schema = json!({
        "tables": {
            "item": {
                "columns": {
                    "id": { "type": "string" },
                    "secret": { "type": "string", "encrypted": true },
                },
                "primaryKey": ["id"],
            },
        },
    });
    exec_sql(
        &db,
        "CREATE TABLE item (id TEXT PRIMARY KEY, secret TEXT NOT NULL)",
    );
    engine_init_schema(&db, to_js(&schema)).unwrap();
    exec_sql(
        &db,
        "INSERT INTO item VALUES ('i1', 'orez-e1.7.tag.ciphertext')",
    );

    exec_sql(&db, "BEGIN");
    let error = engine_handle_pull(
        &db,
        to_js(&schema),
        to_js(&json!({
            "rowLocal": false,
            "filters": [{
                "table": "item",
                "sql": "secret = ?",
                "params": ["orez-e1.7.tag.ciphertext"],
                "columns": [{ "table": "item", "column": "secret" }],
            }],
        })),
        to_js(&json!({ "maxChangeRows": 100, "maxChangeBytes": 65_536 })),
        "4096",
        to_js(&json!({
            "clientID": "reader-1",
            "clientGroupID": "group-1",
            "cookie": null,
        })),
        "user-1",
    )
    .expect_err("visibility must not inspect an encrypted column");
    exec_sql(&db, "ROLLBACK");
    assert_eq!(status(&error), 400);
    assert!(message(&error).contains("forbidden use 'visibility'"));

    exec_sql(&db, "BEGIN");
    let pull = from_js(
        engine_handle_pull(
            &db,
            to_js(&schema),
            JsValue::NULL,
            to_js(&json!({ "maxChangeRows": 100, "maxChangeBytes": 65_536 })),
            "4096",
            to_js(&json!({
                "clientID": "reader-1",
                "clientGroupID": "group-1",
                "cookie": null,
            })),
            "user-1",
        )
        .unwrap(),
    );
    exec_sql(&db, "COMMIT");
    assert_eq!(
        pull["rowsPatch"][1]["value"]["secret"],
        "orez-e1.7.tag.ciphertext"
    );
}

#[wasm_bindgen_test]
fn preflight_then_application_write_then_finalize_preserves_journal_order() {
    let db = db();
    initialize(&db);

    exec_sql(&db, "BEGIN");
    let decision = from_js(engine_preflight(&db, "group-1", "writer-1", "1", "user-1").unwrap());
    assert_eq!(decision, json!({ "kind": "applied" }));
    assert_eq!(
        from_js(query_sql(&db, "SELECT count(*) AS n FROM _zsync_changes"))[0]["values"][0]["value"],
        "0",
    );
    exec_sql(
        &db,
        "INSERT INTO item (id, label) VALUES ('ordered', 'effect first')",
    );
    engine_finalize(&db, "group-1", "writer-1", "1").unwrap();
    exec_sql(&db, "COMMIT");

    let changes = from_js(query_sql(
        &db,
        "SELECT op FROM _zsync_changes ORDER BY watermark",
    ));
    assert_eq!(changes[0]["values"][0]["value"], "row");
    assert_eq!(changes[1]["values"][0]["value"], "lmid");

    let replay = from_js(engine_preflight(&db, "group-1", "writer-1", "1", "user-1").unwrap());
    assert_eq!(replay, json!({ "kind": "replay", "expected": "2" }));
}
