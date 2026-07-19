mod common;

use std::collections::BTreeSet;

use serde_json::{Value, json};

use common::TestDb;
use sync_core::pull::Caps;
use sync_core::query::compile::compile_predicate_probe;
use sync_core::query::{
    compile, compile_transaction_query, handle_query_pull, init_query_schema, parse_ast,
    parse_query_format, parse_query_schema, recompute_group, register_query, set_desire,
};
use sync_core::{
    Tables, Transactor, VisibilityExpression, compile_visibility_filter, handle_pull, init_schema,
};

fn encrypted_schema() -> Value {
    json!({
        "schemaID": "encryption-guards-v1",
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

fn clear_tables() -> Tables {
    let mut schema = encrypted_schema();
    for table in schema["tables"].as_object_mut().unwrap().values_mut() {
        for column in table["columns"].as_object_mut().unwrap().values_mut() {
            column.as_object_mut().unwrap().remove("encrypted");
        }
    }
    Tables::from_zero_schema(&schema).unwrap()
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
    let missing_schema_id = json!({
        "tables": {
            "record": {
                "columns": {
                    "id": { "type": "string" },
                    "secret": { "type": "string", "encrypted": true }
                },
                "primaryKey": ["id"]
            }
        }
    });
    assert_eq!(
        Tables::from_zero_schema(&missing_schema_id).unwrap_err(),
        "schema.schemaID is required when encrypted column metadata is present"
    );

    let primary_key = json!({
        "schemaID": "primary-key-v1",
        "tables": {
            "record": {
                "columns": { "secret": { "type": "string", "encrypted": true } },
                "primaryKey": ["secret"]
            }
        }
    });
    let error = Tables::from_zero_schema(&primary_key).unwrap_err();
    assert!(error.contains("schema 'primary-key-v1'"), "{error}");
    assert!(error.contains("forbidden use 'primary-key'"), "{error}");

    for column_type in ["number", "boolean", "null"] {
        let schema = json!({
            "schemaID": "unsupported-type-v1",
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
        assert!(error.contains("schema 'unsupported-type-v1'"), "{error}");
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
            "partial",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX clear_key_secret_predicate ON messages(route_key) WHERE secret_blob IS NOT NULL",
        ),
        (
            "partial",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX quoted_secret_predicate ON messages(route_key) WHERE \"secret_blob\" IS NOT NULL",
        ),
        (
            "partial",
            "CREATE TABLE messages (message_id TEXT PRIMARY KEY, route_key TEXT, secret_blob TEXT, details_blob TEXT);\
             CREATE INDEX expression_secret_predicate ON messages(route_key) WHERE length(secret_blob) > 0",
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
        assert!(
            error.contains("schema 'encryption-guards-v1'"),
            "{kind}: {error}"
        );
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
fn query_registration_rejects_encrypted_predicates() {
    let left = registration_error(simple(column("secret_blob"), literal("x")));
    assert!(left.message.contains("message.secret"), "{}", left.message);
    assert!(left.message.contains("forbidden use 'predicate'"));
    assert!(left.message.contains("schema 'encryption-guards-v1'"));
}

#[test]
fn every_public_compiler_rejects_encrypted_predicates() {
    let tables = tables();
    let ast_json = simple(column("secret"), literal("x"));
    let ast = parse_ast(&ast_json).unwrap();

    let errors = [
        compile(&ast, &tables).err().unwrap(),
        compile_predicate_probe(&ast, &tables).err().unwrap(),
    ];
    for error in errors {
        assert!(error.message.contains("schema 'encryption-guards-v1'"));
        assert!(error.message.contains("forbidden use 'predicate'"));
    }

    let query_schema = parse_query_schema(&encrypted_schema()).unwrap();
    let format = parse_query_format(&json!({
        "singular": false,
        "relationships": {}
    }))
    .unwrap();
    let error = compile_transaction_query(&query_schema, &tables, &ast, &format).unwrap_err();
    assert!(error.message.contains("schema 'encryption-guards-v1'"));
    assert!(error.message.contains("forbidden use 'predicate'"));
}

#[test]
fn persisted_queries_are_revalidated_when_encryption_metadata_changes() {
    let mut db = TestDb::memory();
    db.conn
        .execute_batch(
            "CREATE TABLE messages (
                message_id TEXT PRIMARY KEY,
                route_key TEXT,
                secret_blob TEXT,
                details_blob TEXT
            )",
        )
        .unwrap();
    init_query_schema(&mut db).unwrap();
    let ast = simple(column("secret"), literal("clear-before-schema-change"));
    register_query(&mut db, &clear_tables(), "group", "same-version", &ast, 7).unwrap();
    set_desire(&mut db, "group", "client", "same-version", 7).unwrap();
    recompute_group(&mut db, &clear_tables(), "group", &BTreeSet::new()).unwrap();

    let error = recompute_group(&mut db, &tables(), "group", &BTreeSet::new()).unwrap_err();
    assert!(error.message.contains("schema 'encryption-guards-v1'"));
    assert!(error.message.contains("message.secret"));
    assert!(error.message.contains("forbidden use 'predicate'"));
}

#[test]
fn caught_up_query_pulls_revalidate_persisted_queries_before_unchanged() {
    let mut db = TestDb::memory();
    db.conn
        .execute_batch(
            "CREATE TABLE messages (
                message_id TEXT PRIMARY KEY,
                route_key TEXT,
                secret_blob TEXT,
                details_blob TEXT
            );
            CREATE TABLE attachments (
                attachment_id TEXT PRIMARY KEY,
                message_id TEXT,
                body_blob TEXT
            )",
        )
        .unwrap();
    let clear_tables = clear_tables();
    init_schema(&mut db, &clear_tables).unwrap();
    init_query_schema(&mut db).unwrap();
    let ast = simple(column("secret"), literal("clear-before-schema-change"));
    let first = handle_query_pull(
        &mut db,
        &clear_tables,
        4096,
        &json!({
            "clientID": "client",
            "clientGroupID": "group",
            "cookie": null,
            "queries": {
                "version": 7,
                "patch": [{
                    "op": "put",
                    "hash": "same-version",
                    "ast": ast,
                    "transformVersion": 7
                }]
            }
        }),
        "user",
    )
    .unwrap();

    let error = handle_query_pull(
        &mut db,
        &tables(),
        4096,
        &json!({
            "clientID": "client",
            "clientGroupID": "group",
            "cookie": first["cookie"].clone()
        }),
        "user",
    )
    .unwrap_err();
    assert!(error.message.contains("schema 'encryption-guards-v1'"));
    assert!(error.message.contains("message.secret"));
    assert!(error.message.contains("forbidden use 'predicate'"));
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
fn structured_visibility_resolves_physical_names_and_rejects_encrypted_columns() {
    let tables = tables();
    let clear: VisibilityExpression = serde_json::from_value(json!({
        "type": "comparison",
        "operator": "=",
        "left": { "type": "column", "table": "messages", "column": "route_key" },
        "right": { "type": "value", "value": "r1" }
    }))
    .unwrap();
    let compiled = compile_visibility_filter(&tables, "messages", &clear).unwrap();
    assert_eq!(compiled.sql, "\"message\".\"route\" = ?");

    let encrypted: VisibilityExpression = serde_json::from_value(json!({
        "type": "comparison",
        "operator": "=",
        "left": { "type": "column", "table": "messages", "column": "secret_blob" },
        "right": { "type": "value", "value": "ciphertext" }
    }))
    .unwrap();
    let error = compile_visibility_filter(&tables, "messages", &encrypted)
        .err()
        .unwrap();
    assert!(
        error.message.contains("message.secret"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains("schema 'encryption-guards-v1'"),
        "{}",
        error.message
    );
    assert!(error.message.contains("forbidden use 'visibility'"));
}
