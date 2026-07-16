// M4c dataset measurement: the cost of a FULL AUTHORIZED SNAPSHOT of a Chat
// server namespace, run through the real engine (compiled permission query +
// zero_row serialization), across scales including message-heavy namespaces.
// this is the measurement that decides M4b narrowing: it shows whether shipping
// (and re-shipping, on every permission change) a whole authorized snapshot is
// viable for message-heavy servers.
//
// the small scale runs as a normal test (a correctness + shape sanity check).
// the large, message-heavy scales are #[ignore] and run explicitly to gather
// the numbers transcribed into plans/chat-m4c-dataset-report.md:
//   cargo test -p sync-core --test chat_snapshot_bench -- --ignored --nocapture
mod common;

use std::time::Instant;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{compile, parse_ast};
use sync_core::schema::TableSpec;
use sync_core::value::{ZeroColumnType, zero_row};
use sync_core::{SqlValue, SyncDb, Tables};

// the message-bearing subset of Chat's schema that dominates a namespace's size
fn chat_tables() -> Tables {
    use ZeroColumnType::*;
    let s = |n: &str| (n.to_string(), String);
    let b = |n: &str| (n.to_string(), Boolean);
    Tables::new()
        .with(
            "server",
            TableSpec {
                columns: vec![s("id"), b("private"), s("creatorId")],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "serverMember",
            TableSpec {
                columns: vec![s("id"), s("serverId"), s("userId")],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "userRole",
            TableSpec {
                columns: vec![s("id"), s("serverId"), s("userId"), b("canAdmin")],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "channelUserRole",
            TableSpec {
                columns: vec![s("id"), s("channelId"), s("userId")],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "channel",
            TableSpec {
                columns: vec![
                    s("id"),
                    s("serverId"),
                    b("private"),
                    b("deleted"),
                    b("solo"),
                ],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "message",
            TableSpec {
                columns: vec![
                    s("id"),
                    s("channelId"),
                    s("creatorId"),
                    s("type"),
                    b("deleted"),
                    s("content"),
                ],
                primary_key: vec!["id".into()],
            },
        )
}

// Chat's message-read permission, transcribed to a transformed AST (auth id
// already resolved to a literal): not deleted, channel readable (server
// readable + channel not-private / role / admin), and the solo-channel check.
fn message_read_ast(auth: &str) -> Value {
    let eq = |col: &str, v: Value| json!({ "type": "simple", "op": "=", "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } });
    let ne = |col: &str, v: Value| json!({ "type": "simple", "op": "!=", "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } });
    let exists = |pf: &str, cf: &str, table: &str, where_: Value| {
        json!({ "type": "correlatedSubquery", "op": "EXISTS", "related": {
            "correlation": { "parentField": [pf], "childField": [cf] },
            "subquery": { "table": table, "where": where_ }
        } })
    };
    let not_exists = |pf: &str, cf: &str, table: &str, where_: Value| {
        json!({ "type": "correlatedSubquery", "op": "NOT EXISTS", "related": {
            "correlation": { "parentField": [pf], "childField": [cf] },
            "subquery": { "table": table, "where": where_ }
        } })
    };

    // server readable: public OR the user is a member
    let server_readable = json!({ "type": "or", "conditions": [
        eq("private", json!(false)),
        exists("id", "serverId", "serverMember", eq("userId", json!(auth))),
    ] });
    // channel access: not private OR user has a channel role OR server admin
    let channel_access = json!({ "type": "or", "conditions": [
        eq("private", json!(false)),
        exists("id", "channelId", "channelUserRole", eq("userId", json!(auth))),
        exists("serverId", "serverId", "userRole", json!({ "type": "and", "conditions": [
            eq("userId", json!(auth)), eq("canAdmin", json!(true))
        ] })),
    ] });
    // channel readable: not deleted AND server readable (nested EXISTS on server) AND channel access
    let channel_where = json!({ "type": "and", "conditions": [
        ne("deleted", json!(true)),
        exists("serverId", "id", "server", server_readable),
        channel_access,
    ] });
    // solo-channel restriction: not a solo channel, or the user's own message, or a bot message
    let solo_check = json!({ "type": "or", "conditions": [
        not_exists("channelId", "id", "channel", eq("solo", json!(true))),
        eq("creatorId", json!(auth)),
        eq("type", json!("bot")),
    ] });

    json!({ "table": "message", "where": { "type": "and", "conditions": [
        ne("deleted", json!(true)),
        exists("channelId", "id", "channel", channel_where),
        solo_check,
    ] } })
}

struct Scale {
    channels: usize,
    members: usize,
    messages_per_channel: usize,
    private_channels: usize, // of `channels`, how many are private (need a role)
}

struct Measured {
    authorized_rows: usize,
    snapshot_bytes: usize,
    total_messages: usize,
    query_ms: f64,
    serialize_ms: f64,
    // the query-aware alternative: one open channel's most-recent window
    window_rows: usize,
    window_bytes: usize,
    window_ms: f64,
}

fn seed_and_measure(scale: &Scale, auth: &str) -> Measured {
    let mut db = TestDb::memory();
    // schema
    db.exec(
        "CREATE TABLE server (id TEXT PRIMARY KEY, private INTEGER, creatorId TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE serverMember (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE userRole (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT, canAdmin INTEGER)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE channelUserRole (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec("CREATE TABLE channel (id TEXT PRIMARY KEY, serverId TEXT, private INTEGER, deleted INTEGER, solo INTEGER)", &[]).unwrap();
    db.exec("CREATE TABLE message (id TEXT PRIMARY KEY, channelId TEXT, creatorId TEXT, type TEXT, deleted INTEGER, content TEXT)", &[]).unwrap();
    db.exec("CREATE INDEX ix_msg_channel ON message(channelId)", &[])
        .unwrap();
    db.exec(
        "CREATE INDEX ix_member ON serverMember(serverId, userId)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE INDEX ix_chanrole ON channelUserRole(channelId, userId)",
        &[],
    )
    .unwrap();

    // one server, the auth user is a member
    db.exec("INSERT INTO server VALUES ('s1', 0, 'owner')", &[])
        .unwrap();
    db.exec(
        "INSERT INTO serverMember VALUES ('sm-auth', 's1', ?)",
        &[SqlValue::Text(auth.into())],
    )
    .unwrap();
    for m in 0..scale.members {
        db.exec(
            "INSERT INTO serverMember VALUES (?, 's1', ?)",
            &[
                SqlValue::Text(format!("sm{m}")),
                SqlValue::Text(format!("u{m}")),
            ],
        )
        .unwrap();
    }
    // a realistic message body (~280 chars, like an average chat message)
    let content = "x".repeat(280);
    for ch in 0..scale.channels {
        let private = ch < scale.private_channels;
        db.exec(
            "INSERT INTO channel VALUES (?, 's1', ?, 0, 0)",
            &[
                SqlValue::Text(format!("c{ch}")),
                SqlValue::Integer(private as i64),
            ],
        )
        .unwrap();
        if private {
            // the auth user has a role on the private channel (so it is authorized)
            db.exec(
                "INSERT INTO channelUserRole VALUES (?, ?, ?)",
                &[
                    SqlValue::Text(format!("cur{ch}")),
                    SqlValue::Text(format!("c{ch}")),
                    SqlValue::Text(auth.into()),
                ],
            )
            .unwrap();
        }
        for msg in 0..scale.messages_per_channel {
            db.exec(
                "INSERT INTO message VALUES (?, ?, ?, 'text', 0, ?)",
                &[
                    SqlValue::Text(format!("c{ch}m{msg}")),
                    SqlValue::Text(format!("c{ch}")),
                    SqlValue::Text(format!("u{}", msg % scale.members.max(1))),
                    SqlValue::Text(content.clone()),
                ],
            )
            .unwrap();
        }
    }
    let total_messages = scale.channels * scale.messages_per_channel;

    // compile + run the authorized message-read snapshot
    let tables = chat_tables();
    let spec = tables.get("message").unwrap().clone();
    let ast = parse_ast(&message_read_ast(auth)).unwrap();
    let compiled = compile(&ast, &tables).unwrap();

    let t0 = Instant::now();
    let rows = db.query(&compiled.sql, &compiled.params).unwrap();
    let query_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t1 = Instant::now();
    let mut bytes = 0usize;
    for row in &rows {
        let v = zero_row(&tables, "message", &spec, row).unwrap();
        bytes += serde_json::to_string(&v).unwrap().len();
    }
    let serialize_ms = t1.elapsed().as_secs_f64() * 1000.0;

    // the query-aware alternative M4b would ship instead of the whole namespace:
    // one open channel's most-recent message window (the client's list limit).
    let window_ast = parse_ast(&json!({
        "table": "message",
        "where": { "type": "simple", "op": "=", "left": { "type": "column", "name": "channelId" },
                   "right": { "type": "literal", "value": "c0" } },
        "orderBy": [["id", "desc"]],
        "limit": 100
    }))
    .unwrap();
    let window_compiled = compile(&window_ast, &tables).unwrap();
    let tw = Instant::now();
    let window_result = db
        .query(&window_compiled.sql, &window_compiled.params)
        .unwrap();
    let mut window_bytes = 0usize;
    for row in &window_result {
        window_bytes += serde_json::to_string(&zero_row(&tables, "message", &spec, row).unwrap())
            .unwrap()
            .len();
    }
    let window_ms = tw.elapsed().as_secs_f64() * 1000.0;

    Measured {
        authorized_rows: rows.len(),
        snapshot_bytes: bytes,
        total_messages,
        query_ms,
        serialize_ms,
        window_rows: window_result.len(),
        window_bytes,
        window_ms,
    }
}

fn report_line(name: &str, scale: &Scale) {
    let m = seed_and_measure(scale, "auth-user");
    let ratio = m.snapshot_bytes as f64 / (m.window_bytes.max(1) as f64);
    println!(
        "{name}: total_msgs={} | FULL SNAPSHOT rows={} bytes={} ({:.1} MB) engine={:.1}ms || WINDOW(1 channel, 100 msgs) rows={} bytes={} ({:.1} KB) engine={:.2}ms || snapshot/window bytes ratio {:.0}x",
        m.total_messages,
        m.authorized_rows,
        m.snapshot_bytes,
        m.snapshot_bytes as f64 / 1_048_576.0,
        m.query_ms + m.serialize_ms,
        m.window_rows,
        m.window_bytes,
        m.window_bytes as f64 / 1024.0,
        m.window_ms,
        ratio,
    );
}

// fast sanity check that runs in CI: the permission query authorizes exactly the
// readable messages (public channels + the private channel the user has a role
// on), excluding a private channel the user cannot see.
#[test]
fn authorized_snapshot_respects_permissions() {
    let scale = Scale {
        channels: 3,
        members: 5,
        messages_per_channel: 4,
        private_channels: 1,
    };
    // add an EXTRA private channel with messages the auth user must NOT see
    let mut db = TestDb::memory();
    let _ = &scale;
    // reuse the seed+measure path for the authorized set, then separately assert
    // an unauthorized private channel's messages are excluded
    db.exec(
        "CREATE TABLE server (id TEXT PRIMARY KEY, private INTEGER, creatorId TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE serverMember (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE userRole (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT, canAdmin INTEGER)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE channelUserRole (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec("CREATE TABLE channel (id TEXT PRIMARY KEY, serverId TEXT, private INTEGER, deleted INTEGER, solo INTEGER)", &[]).unwrap();
    db.exec("CREATE TABLE message (id TEXT PRIMARY KEY, channelId TEXT, creatorId TEXT, type TEXT, deleted INTEGER, content TEXT)", &[]).unwrap();
    db.exec("INSERT INTO server VALUES ('s1', 0, 'owner')", &[])
        .unwrap();
    db.exec("INSERT INTO serverMember VALUES ('sm', 's1', 'auth')", &[])
        .unwrap();
    // public channel c-pub (visible), private c-mine (has role -> visible), private c-secret (no role -> hidden)
    db.exec("INSERT INTO channel VALUES ('c-pub','s1',0,0,0), ('c-mine','s1',1,0,0), ('c-secret','s1',1,0,0)", &[]).unwrap();
    db.exec(
        "INSERT INTO channelUserRole VALUES ('cur','c-mine','auth')",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO message VALUES ('m-pub','c-pub','u','text',0,'a'), ('m-mine','c-mine','u','text',0,'b'), ('m-secret','c-secret','u','text',0,'c')", &[]).unwrap();

    let tables = chat_tables();
    let ast = parse_ast(&message_read_ast("auth")).unwrap();
    let compiled = compile(&ast, &tables).unwrap();
    let mut ids: Vec<String> = db
        .query(&compiled.sql, &compiled.params)
        .unwrap()
        .iter()
        .map(|r| match r.get("id") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => panic!(),
        })
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["m-mine", "m-pub"],
        "secret-channel message must be excluded"
    );
}

#[test]
#[ignore = "measurement — run explicitly with --ignored --nocapture to gather M4c dataset numbers"]
fn dataset_report_measurements() {
    println!(
        "\n=== M4c full-authorized-snapshot cost (message-read permission, one server namespace) ==="
    );
    report_line(
        "small        ",
        &Scale {
            channels: 10,
            members: 20,
            messages_per_channel: 100,
            private_channels: 3,
        },
    );
    report_line(
        "medium       ",
        &Scale {
            channels: 30,
            members: 100,
            messages_per_channel: 500,
            private_channels: 8,
        },
    );
    report_line(
        "large        ",
        &Scale {
            channels: 50,
            members: 300,
            messages_per_channel: 2_000,
            private_channels: 15,
        },
    );
    report_line(
        "message-heavy",
        &Scale {
            channels: 80,
            members: 500,
            messages_per_channel: 6_250,
            private_channels: 25,
        },
    );
}
