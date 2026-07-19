mod common;

use serde_json::{Value, json};

use common::TestDb;
use sync_core::pull::Caps;
use sync_core::query::{init_query_schema, register_query};
use sync_core::schema::ColumnUse;
use sync_core::{Tables, Transactor, handle_pull, init_schema};

fn encrypted_schema() -> Value {
    json!({
        "tables": {
            "message": {
                "serverName": "messages",
                "columns": {
                    "id": { "type": "string", "serverName": "message_id" },
                    "route": { "type": "string", "serverName": "route_key" },
                    "secret": {
                        "type": "string",
                        "serverName": "secret_blob",
                        "encrypted": true
                    },
                    "details": {
                        "type": "json",
                        "serverName": "details_blob",
                        "encrypted": true
                    }
                },
                "primaryKey": ["id"]
            },
            "attachment": {
                "serverName": "attachments",
                "columns": {
                    "id": { "type": "string", "serverName": "attachment_id" },
                    "messageId": { "type": "string", "serverName": "message_id" },
                    "body": {
                        "type": "string",
                        "serverName": "body_blob",
                        "encrypted": true
                    }
                },
                "primaryKey": ["id"]
            }
        }
    })
}

fn tables() -> Tables {
    Tables::from_zero_schema(&encrypted_schema()).unwrap()
}

fn simple(left: Value, right: Value) -> Value {
    json!({
        "table": "message",
        "where": { "type": "simple", "op": "=", "left": left, "right": right }
    })
}

fn column(name: &str) -> Value {
    json!({ "type": "column", "name": name })
}

fn literal(value: impl Into<Value>) -> Value {
    json!({ "type": "literal", "value": value.into() })
}

fn registration_error(ast: Value) -> sync_core::EngineError {
    let mut db = TestDb::memory();
    register_query(&mut db, &tables(), "group", "query", &ast, 0).unwrap_err()
}

#[test]
fn schema_rejects_encrypted_primary_keys_and_unsupported_types() {
    let primary_key = json!({
        "tables": {
            "record": {
                "columns": { "secret": { "type": "string", "encrypted": true } },
                "primaryKey": ["secret"]
            }
        }
    });
    assert!(
        Tables::from_zero_schema(&primary_key)
            .unwrap_err()
            .contains("forbidden use 'primary-key'")
    );

    for column_type in ["number", "boolean", "null"] {
        let schema = json!({
            "tables": {
                "record": {
                    "columns": {
                        "id": { "type": "string" },
                        "secret": { "type": column_type, "encrypted": true }
                    },
                    "primaryKey": ["id"]
                }
            }
        });
        let error = Tables::from_zero_schema(&schema).unwrap_err();
        assert!(error.contains("unsupported logical type"), "{error}");
        assert!(error.contains(column_type), "{error}");
    }
}

#[test]
fn schema_rejects_ambiguous_logical_and_physical_column_names() {
    let schema = json!({
        "tables": {
            "record": {
                "columns": {
                    "id": { "type": "string" },
                    "clear": { "type": "string", "serverName": "secret" },
                    "secret": { "type": "string", "serverName": "ciphertext" }
                },
                "primaryKey": ["id"]
            }
        }
    });
    assert!(
        Tables::from_zero_schema(&schema)
            .unwrap_err()
            .contains("ambiguous logical/physical column mapping")
    );
}

#[test]
fn schema_resolver_accepts_logical_and_physical_names() {
    let tables = tables();
    let logical = tables.resolve_column("message", "secret").unwrap();
    let physical = tables.resolve_column("messages", "secret_blob").unwrap();
    assert_eq!(logical.logical_column, "secret");
    assert_eq!(physical.logical_column, "secret");
    assert!(logical.encrypted && physical.encrypted);
}

#[test]
fn initialization_rejects_every_sqlite_index_kind_on_encrypted_columns() {
    let cases = [
        (
            "ordinary",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX secret_ordinary ON messages(secret_blob)",
        ),
        (
            "partial",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX secret_partial ON messages(secret_blob) WHERE route_key IS NOT NULL",
        ),
        (
            "unique",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT UNIQUE, details_blob TEXT)",
        ),
        (
            "primary",
            "CREATE TABLE messages (message_id TEXT UNIQUE, route_key TEXT, secret_blob TEXT PRIMARY KEY, details_blob TEXT)",
        ),
        (
            "expression",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX secret_expression ON messages(lower(secret_blob))",
        ),
    ];

    for (kind, ddl) in cases {
        let mut db = TestDb::memory();
        db.conn.execute_batch(ddl).unwrap();
        db.conn
            .execute_batch(
                "CREATE TABLE attachments (attachment_id TEXT PRIMARY KEY, message_id TEXT, body_blob TEXT)",
            )
            .unwrap();
        let error = init_schema(&mut db, &tables()).unwrap_err().0;
        assert!(error.contains("forbidden use 'index'"), "{kind}: {error}");
        assert!(error.contains(kind), "{kind}: {error}");
    }
}

#[test]
fn clear_indexes_and_encrypted_projection_are_allowed() {
    let mut db = TestDb::memory();
    db.conn
        .execute_batch(
            "CREATE TABLE messages (
                message_id TEXT PRIMARY KEY,
                route_key TEXT NOT NULL,
                secret_blob TEXT NOT NULL,
                details_blob TEXT NOT NULL
             );
             CREATE INDEX route_index ON messages(route_key);
             CREATE TABLE attachments (
                attachment_id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                body_blob TEXT NOT NULL
             );
             INSERT INTO messages VALUES (
                'm1', 'r1', 'orez-e1.7.tag.stringcipher', 'orez-e1.7.tag.jsoncipher'
             )",
        )
        .unwrap();
    let tables = tables();
    init_schema(&mut db, &tables).unwrap();

    let response = db
        .transaction(|database| {
            handle_pull(
                database,
                &tables,
                4096,
                None,
                Caps::default(),
                &json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": null }),
                "u1",
            )
        })
        .unwrap();
    let put = &response["rowsPatch"].as_array().unwrap()[1];
    assert_eq!(put["tableName"], "messages");
    assert_eq!(put["value"]["secret_blob"], "orez-e1.7.tag.stringcipher");
    assert_eq!(put["value"]["details_blob"], "orez-e1.7.tag.jsoncipher");
}

#[test]
fn query_registration_rejects_encrypted_predicate_operands() {
    let left = registration_error(simple(column("secret_blob"), literal("x")));
    assert!(left.message.contains("message.secret"), "{}", left.message);
    assert!(left.message.contains("forbidden use 'predicate'"));

    let right = registration_error(simple(column("route"), column("secret")));
    assert!(
        right.message.contains("message.secret"),
        "{}",
        right.message
    );
    assert!(right.message.contains("forbidden use 'predicate'"));
}

#[test]
fn query_registration_rejects_encrypted_order_and_cursor_keys() {
    let order = registration_error(json!({
        "table": "message",
        "orderBy": [["secret", "asc"]]
    }));
    assert!(
        order.message.contains("forbidden use 'order'"),
        "{}",
        order.message
    );

    let cursor = registration_error(json!({
        "table": "message",
        "orderBy": [["secret", "asc"]],
        "start": {
            "row": { "secret": "orez-e1.7.tag.cipher", "id": "m1" },
            "exclusive": true
        }
    }));
    assert!(
        cursor.message.contains("forbidden use 'cursor'"),
        "{}",
        cursor.message
    );
}

#[test]
fn query_registration_rejects_both_relationship_correlation_sides() {
    let related = |parent: &str, child: &str| {
        json!({
            "table": "message",
            "related": [{
                "correlation": { "parentField": [parent], "childField": [child] },
                "subquery": { "table": "attachment" }
            }]
        })
    };
    for ast in [related("secret", "messageId"), related("id", "body")] {
        let error = registration_error(ast);
        assert!(
            error.message.contains("forbidden use 'correlation'"),
            "{}",
            error.message
        );
    }
}

#[test]
fn projection_and_clear_relationship_correlation_remain_allowed() {
    let mut db = TestDb::memory();
    init_query_schema(&mut db).unwrap();
    register_query(
        &mut db,
        &tables(),
        "group",
        "projection",
        &json!({
            "table": "message",
            "where": {
                "type": "simple",
                "op": "=",
                "left": { "type": "column", "name": "route" },
                "right": { "type": "literal", "value": "r1" }
            },
            "orderBy": [["route", "asc"]],
            "start": {
                "row": {
                    "route": "r0",
                    "id": "m0",
                    "secret": "orez-e1.7.tag.projected-cursor-value"
                },
                "exclusive": true
            },
            "related": [{
                "correlation": { "parentField": ["id"], "childField": ["messageId"] },
                "subquery": { "table": "attachment" }
            }]
        }),
        0,
    )
    .unwrap();
}

#[test]
fn visibility_references_fail_closed_through_the_shared_resolver() {
    let tables = tables();
    tables
        .validate_column_usage("messages", "route_key", ColumnUse::Visibility)
        .unwrap();
    let error = tables
        .validate_column_usage("messages", "secret_blob", ColumnUse::Visibility)
        .unwrap_err();
    assert!(
        error.message.contains("message.secret"),
        "{}",
        error.message
    );
    assert!(error.message.contains("forbidden use 'visibility'"));
}
