mod common;

use common::TestDb;
use serde_json::json;

use sync_core::query::{
    CompiledQueryNode, QueryBinding, compile_transaction_query, parse_ast, parse_query_format,
    parse_query_schema,
};
use sync_core::{Row, SqlValue, SyncDb, Tables};

fn schema() -> serde_json::Value {
    json!({
        "tables": {
            "user": {
                "name": "user",
                "serverName": "user_records",
                "columns": {
                    "id": { "type": "string" },
                    "displayName": { "type": "string", "serverName": "display_name" },
                    "active": { "type": "boolean" },
                    "metadata": { "type": "json" }
                },
                "primaryKey": ["id"]
            },
            "post": {
                "name": "post",
                "serverName": "post_records",
                "columns": {
                    "id": { "type": "string" },
                    "authorId": { "type": "string", "serverName": "author_id" },
                    "title": { "type": "string" },
                    "rank": { "type": "number" }
                },
                "primaryKey": ["id"]
            },
            "group": {
                "name": "group",
                "serverName": "group_records",
                "columns": {
                    "id": { "type": "string" },
                    "label": { "type": "string" }
                },
                "primaryKey": ["id"]
            },
            "membership": {
                "name": "membership",
                "serverName": "membership_records",
                "columns": {
                    "userId": { "type": "string", "serverName": "user_id" },
                    "groupId": { "type": "string", "serverName": "group_id" }
                },
                "primaryKey": ["userId", "groupId"]
            },
            "comment": {
                "name": "comment",
                "serverName": "comment_records",
                "columns": {
                    "id": { "type": "string" },
                    "postId": { "type": "string", "serverName": "post_id" },
                    "body": { "type": "string" }
                },
                "primaryKey": ["id"]
            }
        }
    })
}

fn format(singular: bool) -> serde_json::Value {
    json!({ "singular": singular, "relationships": {} })
}

fn tables() -> Tables {
    Tables::from_zero_schema(&schema()).unwrap()
}

fn params(node: &CompiledQueryNode, parent: Option<&Row>) -> Vec<SqlValue> {
    node.bindings
        .iter()
        .map(|binding| match binding {
            QueryBinding::Literal(value) => value.clone(),
            QueryBinding::ParentField(field) => parent
                .and_then(|row| row.get(field))
                .unwrap_or_else(|| panic!("missing parent field {field}"))
                .clone(),
        })
        .collect()
}

fn execute(db: &mut TestDb, node: &CompiledQueryNode, parent: Option<&Row>) -> Vec<Row> {
    db.query(&node.sql, &params(node, parent)).unwrap()
}

fn text(row: &Row, column: &str) -> String {
    match row.get(column) {
        Some(SqlValue::Text(value)) => value.clone(),
        value => panic!("expected text {column}, got {value:?}"),
    }
}

#[test]
fn compiles_physical_names_and_a_singular_related_plan() {
    let schema = parse_query_schema(&schema()).unwrap();
    let ast = parse_ast(&json!({
        "table": "user",
        "alias": "user",
        "where": {
            "type": "simple",
            "op": "=",
            "left": { "type": "column", "name": "id" },
            "right": { "type": "literal", "value": "u1" }
        },
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["authorId"] },
            "subquery": {
                "table": "post",
                "alias": "latestPost",
                "orderBy": [["rank", "desc"]]
            }
        }]
    }))
    .unwrap();
    let format = parse_query_format(&json!({
        "singular": true,
        "relationships": {
            "latestPost": { "singular": true, "relationships": {} }
        }
    }))
    .unwrap();

    let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();

    assert_eq!(plan.root_table, "user");
    assert_eq!(plan.plan_hash.len(), 16);
    assert!(plan.root.sql.contains("FROM \"user_records\""));
    assert!(
        plan.root
            .sql
            .contains("\"display_name\" AS \"displayName\"")
    );
    assert!(plan.root.sql.ends_with("LIMIT ?"));
    assert_eq!(plan.root.relationships.len(), 1);

    let relationship = &plan.root.relationships[0];
    assert_eq!(relationship.name, "latestPost");
    assert!(relationship.node.singular);
    assert!(relationship.node.sql.contains("FROM \"post_records\""));
    assert!(relationship.node.sql.contains("\"author_id\" = ?"));
    assert!(matches!(
        relationship.node.bindings.first(),
        Some(QueryBinding::ParentField(field)) if field == "id"
    ));
}

#[test]
fn rejects_a_format_tree_that_omits_related_output() {
    let schema = parse_query_schema(&schema()).unwrap();
    let ast = parse_ast(&json!({
        "table": "user",
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["authorId"] },
            "subquery": { "table": "post", "alias": "posts" }
        }]
    }))
    .unwrap();
    let format = parse_query_format(&json!({ "singular": false, "relationships": {} })).unwrap();

    let error = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap_err();
    assert!(error.to_string().contains("format relationships"));
}

#[test]
fn rejects_scalar_subqueries_and_malformed_planning_hints() {
    let scalar = json!({
        "table": "user",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "scalar": true,
            "related": {
                "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                "subquery": { "table": "post" }
            }
        }
    });
    assert!(
        parse_ast(&scalar)
            .unwrap_err()
            .to_string()
            .contains("scalar")
    );

    let malformed_flip = json!({
        "table": "user",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "flip": "yes",
            "related": {
                "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                "subquery": { "table": "post" }
            }
        }
    });
    assert!(
        parse_ast(&malformed_flip)
            .unwrap_err()
            .to_string()
            .contains("flip")
    );
}

#[test]
fn executes_case_sensitive_like_ascii_ilike_and_null_comparison_families() {
    let schema = parse_query_schema(&schema()).unwrap();
    let format = parse_query_format(&format(false)).unwrap();
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE user_records (id TEXT PRIMARY KEY, display_name TEXT, active INTEGER, metadata TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO user_records VALUES ('u1','Alice',1,NULL), ('u2','alice',0,'{}'), ('u3',NULL,1,NULL)",
        &[],
    )
    .unwrap();

    let query = |operator: &str, value: serde_json::Value| {
        parse_ast(&json!({
            "table": "user",
            "where": {
                "type": "simple",
                "op": operator,
                "left": { "type": "column", "name": "displayName" },
                "right": { "type": "literal", "value": value }
            }
        }))
        .unwrap()
    };
    let ids = |db: &mut TestDb, operator: &str, value: serde_json::Value| {
        let ast = query(operator, value);
        let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();
        execute(db, &plan.root, None)
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>()
    };

    assert_eq!(ids(&mut db, "LIKE", json!("alice")), vec!["u2"]);
    assert_eq!(ids(&mut db, "ILIKE", json!("alice")), vec!["u1", "u2"]);
    assert!(ids(&mut db, "=", json!(null)).is_empty());
    assert!(ids(&mut db, "!=", json!(null)).is_empty());
    assert_eq!(ids(&mut db, "IS", json!(null)), vec!["u3"]);
    assert_eq!(ids(&mut db, "IS NOT", json!(null)), vec!["u1", "u2"]);

    db.exec("INSERT INTO user_records VALUES ('u4','Émile',1,NULL)", &[])
        .unwrap();
    assert!(ids(&mut db, "ILIKE", json!("é%")).is_empty());
    assert_eq!(ids(&mut db, "ILIKE", json!("É%")), vec!["u4"]);
}

#[test]
fn executes_related_order_and_limit_independently_per_parent() {
    let schema = parse_query_schema(&schema()).unwrap();
    let ast = parse_ast(&json!({
        "table": "user",
        "orderBy": [["id", "asc"]],
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["authorId"] },
            "subquery": {
                "table": "post",
                "alias": "topPost",
                "orderBy": [["rank", "desc"]],
                "start": { "row": { "rank": 6, "id": "cursor" }, "exclusive": true },
                "limit": 1,
                "related": [{
                    "correlation": { "parentField": ["id"], "childField": ["postId"] },
                    "subquery": { "table": "comment", "alias": "comments" }
                }]
            }
        }]
    }))
    .unwrap();
    let format = parse_query_format(&json!({
        "singular": false,
        "relationships": {
            "topPost": {
                "singular": false,
                "relationships": {
                    "comments": { "singular": false, "relationships": {} }
                }
            }
        }
    }))
    .unwrap();
    let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();
    let child = &plan.root.relationships[0].node;
    let grandchild = &child.relationships[0].node;

    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE user_records (id TEXT PRIMARY KEY, display_name TEXT, active INTEGER, metadata TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE post_records (id TEXT PRIMARY KEY, author_id TEXT, title TEXT, rank REAL)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE comment_records (id TEXT PRIMARY KEY, post_id TEXT, body TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO user_records VALUES ('u1','one',1,NULL), ('u2','two',1,NULL)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO post_records VALUES ('p1','u1','low',1), ('p2','u1','high',5), ('p3','u2','only',3)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO comment_records VALUES ('c1','p2','first'), ('c2','p1','other')",
        &[],
    )
    .unwrap();

    let users = execute(&mut db, &plan.root, None);
    assert_eq!(users.len(), 2);
    let first_posts = execute(&mut db, child, Some(&users[0]));
    assert_eq!(
        first_posts
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>(),
        vec!["p2"]
    );
    assert_eq!(
        execute(&mut db, grandchild, Some(&first_posts[0]))
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>(),
        vec!["c1"]
    );
    assert_eq!(
        execute(&mut db, child, Some(&users[1]))
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>(),
        vec!["p3"]
    );
}

#[test]
fn executes_the_standard_hidden_two_hop_junction_shape() {
    let schema = parse_query_schema(&schema()).unwrap();
    let ast = parse_ast(&json!({
        "table": "user",
        "where": {
            "type": "simple",
            "op": "=",
            "left": { "type": "column", "name": "id" },
            "right": { "type": "literal", "value": "u1" }
        },
        "related": [{
            "hidden": true,
            "correlation": { "parentField": ["id"], "childField": ["userId"] },
            "subquery": {
                "table": "membership",
                "alias": "groups",
                "related": [{
                    "correlation": { "parentField": ["groupId"], "childField": ["id"] },
                    "subquery": { "table": "group", "alias": "destination" }
                }]
            }
        }]
    }))
    .unwrap();
    let format = parse_query_format(&json!({
        "singular": true,
        "relationships": {
            "groups": { "singular": false, "relationships": {} }
        }
    }))
    .unwrap();
    let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();
    let groups = &plan.root.relationships[0].node;
    assert_eq!(groups.table, "group");
    assert!(groups.sql.contains("JOIN \"group_records\""));

    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE user_records (id TEXT PRIMARY KEY, display_name TEXT, active INTEGER, metadata TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE group_records (id TEXT PRIMARY KEY, label TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE membership_records (user_id TEXT, group_id TEXT, PRIMARY KEY (user_id, group_id))",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO user_records VALUES ('u1','one',1,NULL)", &[])
        .unwrap();
    db.exec(
        "INSERT INTO group_records VALUES ('g1','one'), ('g2','two')",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO membership_records VALUES ('u1','g2'), ('u1','g1')",
        &[],
    )
    .unwrap();

    let users = execute(&mut db, &plan.root, None);
    assert_eq!(users.len(), 1);
    assert_eq!(
        execute(&mut db, groups, Some(&users[0]))
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>(),
        vec!["g1", "g2"]
    );
}

#[test]
fn preserves_compound_correlation_binding_order() {
    let schema = parse_query_schema(&schema()).unwrap();
    let ast = parse_ast(&json!({
        "table": "membership",
        "related": [{
            "correlation": {
                "parentField": ["userId", "groupId"],
                "childField": ["userId", "groupId"]
            },
            "subquery": { "table": "membership", "alias": "sameMembership" }
        }]
    }))
    .unwrap();
    let format = parse_query_format(&json!({
        "singular": false,
        "relationships": {
            "sameMembership": { "singular": true, "relationships": {} }
        }
    }))
    .unwrap();

    let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();
    assert_eq!(
        plan.root.relationships[0].node.bindings,
        vec![
            QueryBinding::ParentField("userId".into()),
            QueryBinding::ParentField("groupId".into()),
            QueryBinding::Literal(SqlValue::Integer(1)),
        ]
    );
}

#[test]
fn rejects_invalid_schema_mappings_aliases_and_hidden_shapes() {
    let invalid_type = json!({
        "tables": {
            "item": {
                "columns": { "id": { "type": "date" } },
                "primaryKey": ["id"]
            }
        }
    });
    assert!(
        parse_query_schema(&invalid_type)
            .unwrap_err()
            .to_string()
            .contains("unsupported schema type")
    );

    let invalid_physical_name = json!({
        "tables": {
            "item": {
                "serverName": "item records",
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"]
            }
        }
    });
    assert!(
        parse_query_schema(&invalid_physical_name)
            .unwrap_err()
            .to_string()
            .contains("valid identifier")
    );

    let schema = parse_query_schema(&schema()).unwrap();
    let conflicting_alias = parse_ast(&json!({
        "table": "user",
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["authorId"] },
            "subquery": { "table": "post", "alias": "active" }
        }]
    }))
    .unwrap();
    let conflicting_format = parse_query_format(&json!({
        "singular": false,
        "relationships": { "active": { "singular": false, "relationships": {} } }
    }))
    .unwrap();
    assert!(
        compile_transaction_query(&schema, &tables(), &conflicting_alias, &conflicting_format,)
            .unwrap_err()
            .to_string()
            .contains("conflicts with column")
    );

    let invalid_hidden = parse_ast(&json!({
        "table": "user",
        "related": [{
            "hidden": true,
            "correlation": { "parentField": ["id"], "childField": ["userId"] },
            "subquery": { "table": "membership", "alias": "groups" }
        }]
    }))
    .unwrap();
    let hidden_format = parse_query_format(&json!({
        "singular": false,
        "relationships": { "groups": { "singular": false, "relationships": {} } }
    }))
    .unwrap();
    assert!(
        compile_transaction_query(&schema, &tables(), &invalid_hidden, &hidden_format)
            .unwrap_err()
            .to_string()
            .contains("two-hop junction")
    );
}

#[test]
fn rejects_every_unsupported_contract_shape() {
    let schema = parse_query_schema(&schema()).unwrap();
    let plural = parse_query_format(&format(false)).unwrap();
    let reject = |value: serde_json::Value, format: &sync_core::query::QueryFormat| {
        parse_ast(&value)
            .and_then(|ast| compile_transaction_query(&schema, &tables(), &ast, format))
            .unwrap_err()
            .to_string()
    };
    let simple = |operator: &str, column: &str, value: serde_json::Value| {
        json!({
            "type": "simple",
            "op": operator,
            "left": { "type": "column", "name": column },
            "right": { "type": "literal", "value": value }
        })
    };

    let cases = [
        (json!({ "table": "ghost" }), "unknown table"),
        (
            json!({ "table": "user", "where": simple("=", "missing", json!(1)) }),
            "unknown column",
        ),
        (
            json!({ "table": "user", "where": simple("BETWEEN", "id", json!(1)) }),
            "unsupported operator",
        ),
        (
            json!({ "table": "user", "unsupported": true }),
            "unsupported query field",
        ),
        (
            json!({
                "table": "user",
                "related": [{
                    "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                    "subquery": { "table": "post" }
                }]
            }),
            "non-empty alias",
        ),
        (
            json!({
                "table": "user",
                "related": [
                    {
                        "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                        "subquery": { "table": "post", "alias": "posts" }
                    },
                    {
                        "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                        "subquery": { "table": "post", "alias": "posts" }
                    }
                ]
            }),
            "duplicate related alias",
        ),
        (
            json!({
                "table": "user",
                "where": {
                    "type": "simple",
                    "op": "=",
                    "left": { "type": "column", "name": "id" },
                    "right": { "type": "static", "anchor": "authData", "field": "sub" }
                }
            }),
            "static parameters",
        ),
        (
            json!({ "table": "user", "where": simple("=", "post.id", json!(1)) }),
            "unknown column",
        ),
        (
            json!({ "table": "user", "where": simple("=", "id", json!({ "id": 1 })) }),
            "object literals",
        ),
        (
            json!({
                "table": "user",
                "related": [{
                    "correlation": { "parentField": ["id"], "childField": ["authorId", "id"] },
                    "subquery": { "table": "post", "alias": "posts" }
                }]
            }),
            "length mismatch",
        ),
        (
            json!({ "table": "user", "orderBy": [["id", "sideways"]] }),
            "orderBy direction",
        ),
        (
            json!({ "table": "user", "orderBy": [["missing", "asc"]] }),
            "unknown column",
        ),
        (
            json!({ "table": "user", "start": { "row": { "id": "u1" }, "exclusive": true } }),
            "requires an orderBy",
        ),
        (
            json!({
                "table": "post",
                "orderBy": [["rank", "asc"]],
                "start": { "row": { "rank": 1 }, "exclusive": true }
            }),
            "missing ordering key 'id'",
        ),
        (
            json!({ "table": "user", "limit": -1 }),
            "non-negative integer",
        ),
    ];

    let related_format = parse_query_format(&json!({
        "singular": false,
        "relationships": { "posts": { "singular": false, "relationships": {} } }
    }))
    .unwrap();
    for (index, (value, expected)) in cases.into_iter().enumerate() {
        let format = if matches!(index, 5 | 9) {
            &related_format
        } else {
            &plural
        };
        assert!(
            reject(value, format).contains(expected),
            "rejection {index} did not contain '{expected}'"
        );
    }

    let extra_format = parse_query_format(&json!({
        "singular": false,
        "relationships": { "ghost": { "singular": false, "relationships": {} } }
    }))
    .unwrap();
    assert!(reject(json!({ "table": "user" }), &extra_format).contains("format relationships"));
    assert!(
        parse_query_format(&json!({
            "singular": false,
            "relationships": {},
            "unknown": true
        }))
        .unwrap_err()
        .to_string()
        .contains("unsupported format field")
    );

    let incomplete_related = parse_ast(&json!({
        "table": "user",
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["authorId"] },
            "subquery": {
                "table": "post",
                "alias": "posts",
                "related": [{
                    "correlation": { "parentField": ["id"], "childField": ["postId"] },
                    "subquery": { "table": "ghost", "alias": "ghosts" }
                }]
            }
        }]
    }))
    .unwrap();
    let nested_format = parse_query_format(&json!({
        "singular": false,
        "relationships": {
            "posts": {
                "singular": false,
                "relationships": {
                    "ghosts": { "singular": false, "relationships": {} }
                }
            }
        }
    }))
    .unwrap();
    assert!(
        compile_transaction_query(&schema, &tables(), &incomplete_related, &nested_format,)
            .unwrap_err()
            .to_string()
            .contains("unknown table 'ghost'")
    );

    let duplicate_schema = json!({
        "tables": {
            "one": {
                "serverName": "records",
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"]
            },
            "two": {
                "serverName": "RECORDS",
                "columns": { "id": { "type": "string" } },
                "primaryKey": ["id"]
            }
        }
    });
    assert!(
        parse_query_schema(&duplicate_schema)
            .unwrap_err()
            .to_string()
            .contains("duplicate physical table mapping")
    );
}

#[test]
fn executes_every_remaining_simple_operator_and_boolean_junction() {
    let schema = parse_query_schema(&schema()).unwrap();
    let format = parse_query_format(&format(false)).unwrap();
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE post_records (id TEXT PRIMARY KEY, author_id TEXT, title TEXT, rank REAL)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO post_records VALUES ('p1','u1','Alpha',1), ('p2','u1','beta',2), ('p3','u2','gamma',3)",
        &[],
    )
    .unwrap();

    let ids = |db: &mut TestDb, where_: serde_json::Value| {
        let ast = parse_ast(&json!({ "table": "post", "where": where_ })).unwrap();
        let plan = compile_transaction_query(&schema, &tables(), &ast, &format).unwrap();
        execute(db, &plan.root, None)
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>()
    };
    let simple = |operator: &str, column: &str, value: serde_json::Value| {
        json!({
            "type": "simple",
            "op": operator,
            "left": { "type": "column", "name": column },
            "right": { "type": "literal", "value": value }
        })
    };

    assert_eq!(ids(&mut db, simple("<", "rank", json!(2))), vec!["p1"]);
    assert_eq!(ids(&mut db, simple(">", "rank", json!(2))), vec!["p3"]);
    assert_eq!(
        ids(&mut db, simple("<=", "rank", json!(2))),
        vec!["p1", "p2"]
    );
    assert_eq!(
        ids(&mut db, simple(">=", "rank", json!(2))),
        vec!["p2", "p3"]
    );
    assert_eq!(
        ids(&mut db, simple("NOT LIKE", "title", json!("A%"))),
        vec!["p2", "p3"]
    );
    assert_eq!(
        ids(&mut db, simple("NOT ILIKE", "title", json!("A%"))),
        vec!["p2", "p3"]
    );
    assert_eq!(
        ids(&mut db, simple("IN", "id", json!(["p1", "p3"]))),
        vec!["p1", "p3"]
    );
    assert_eq!(
        ids(&mut db, simple("NOT IN", "id", json!(["p1", "p3"]))),
        vec!["p2"]
    );
    assert!(ids(&mut db, simple("IN", "id", json!([]))).is_empty());
    assert_eq!(
        ids(&mut db, simple("NOT IN", "id", json!([]))),
        vec!["p1", "p2", "p3"]
    );

    let rank_two = simple("=", "rank", json!(2));
    let title_beta = simple("=", "title", json!("beta"));
    assert_eq!(
        ids(
            &mut db,
            json!({ "type": "and", "conditions": [rank_two.clone(), title_beta.clone()] })
        ),
        vec!["p2"]
    );
    assert_eq!(
        ids(
            &mut db,
            json!({ "type": "or", "conditions": [rank_two, simple("=", "id", json!("p3"))] })
        ),
        vec!["p2", "p3"]
    );
}

#[test]
fn executes_correlated_exists_and_not_exists() {
    let schema = parse_query_schema(&schema()).unwrap();
    let format = parse_query_format(&format(false)).unwrap();
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE user_records (id TEXT PRIMARY KEY, display_name TEXT, active INTEGER, metadata TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE post_records (id TEXT PRIMARY KEY, author_id TEXT, title TEXT, rank REAL)",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO user_records VALUES ('u1','one',1,NULL), ('u2','two',1,NULL)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO post_records VALUES ('p1','u1','one',1)", &[])
        .unwrap();

    let ast = |operator: &str| {
        parse_ast(&json!({
            "table": "user",
            "where": {
                "type": "correlatedSubquery",
                "op": operator,
                "related": {
                    "correlation": { "parentField": ["id"], "childField": ["authorId"] },
                    "subquery": { "table": "post" }
                }
            }
        }))
        .unwrap()
    };
    let ids = |db: &mut TestDb, operator: &str| {
        let plan = compile_transaction_query(&schema, &tables(), &ast(operator), &format).unwrap();
        execute(db, &plan.root, None)
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Vec<_>>()
    };

    assert_eq!(ids(&mut db, "EXISTS"), vec!["u1"]);
    assert_eq!(ids(&mut db, "NOT EXISTS"), vec!["u2"]);
}

#[test]
fn compiles_the_harvested_chat_query_corpus() {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../harness/corpus/chat-transaction-query-v1.json"
    ))
    .unwrap();
    assert_eq!(
        corpus["source"]["commit"],
        json!("cc2d26fa24a88161231f3337c0e0cae9d43ae2d1")
    );
    assert_eq!(corpus["counts"]["cases"], json!(252));
    assert_eq!(corpus["counts"]["queries"], json!(123));
    let schema = parse_query_schema(&corpus["schema"]).unwrap();
    let tables = Tables::from_zero_schema(&corpus["schema"]).unwrap();
    let cases = corpus["cases"].as_array().unwrap();
    for test_case in cases {
        let name = test_case["name"].as_str().unwrap();
        let user = test_case["user"].as_str().unwrap();
        let ast = parse_ast(&test_case["ast"])
            .unwrap_or_else(|error| panic!("{name}/{user} AST failed: {error}"));
        let format = parse_query_format(&test_case["format"])
            .unwrap_or_else(|error| panic!("{name}/{user} format failed: {error}"));
        compile_transaction_query(&schema, &tables, &ast, &format)
            .unwrap_or_else(|error| panic!("{name}/{user} compile failed: {error}"));
    }
}
