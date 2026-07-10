// M5 transform authoring: Chat's `where/` permission predicates expressed as
// server-side TRANSFORMED Zero v51 ASTs and validated against a Chat-shaped
// fixture. these are the permission families a consumer host AND-s into a
// client query; here they are pure sync-core AST fragments with allow AND deny
// cases per family (the consumer wiring is M4c/M5 host work). transcribed from
// ~/chat/src/data/where/{server,channel,message}.ts.
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{compile, parse_ast};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables};

fn tables() -> Tables {
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
                columns: vec![
                    s("id"),
                    s("serverId"),
                    s("userId"),
                    s("roleId"),
                    b("canAdmin"),
                ],
                primary_key: vec!["id".into()],
            },
        )
        // the channelUserRoles junction is a real two-hop: channel ->
        // channelPermission (by channelId) -> userRole (by roleId)
        .with(
            "channelPermission",
            TableSpec {
                columns: vec![s("id"), s("channelId"), s("roleId")],
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
                ],
                primary_key: vec!["id".into()],
            },
        )
}

// ---- AST fragment builders (mirroring the on-zero `_` DSL) -----------------

fn eq(col: &str, v: Value) -> Value {
    json!({ "type": "simple", "op": "=", "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } })
}
fn ne(col: &str, v: Value) -> Value {
    json!({ "type": "simple", "op": "!=", "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } })
}
fn and(conds: Vec<Value>) -> Value {
    json!({ "type": "and", "conditions": conds })
}
fn or(conds: Vec<Value>) -> Value {
    json!({ "type": "or", "conditions": conds })
}
// EXISTS(child where <cond>) correlated parent.pf = child.cf
fn exists(pf: &str, cf: &str, table: &str, cond: Value) -> Value {
    json!({ "type": "correlatedSubquery", "op": "EXISTS", "related": {
        "correlation": { "parentField": [pf], "childField": [cf] },
        "subquery": { "table": table, "where": cond } } })
}
fn not_exists(pf: &str, cf: &str, table: &str, cond: Value) -> Value {
    json!({ "type": "correlatedSubquery", "op": "NOT EXISTS", "related": {
        "correlation": { "parentField": [pf], "childField": [cf] },
        "subquery": { "table": table, "where": cond } } })
}

// ---- Chat permission transforms -------------------------------------------
// server.ts hasServerReadPermission (from a table related to server, here the
// server row itself): the server is public, or the user is a member.
fn server_readable(auth: &str) -> Value {
    or(vec![
        eq("private", json!(false)),
        exists("id", "serverId", "serverMember", eq("userId", json!(auth))),
    ])
}

// server.ts hasServerAdminPermission: a userRole with canAdmin for the user
fn server_admin_for(auth: &str) -> Value {
    exists(
        "id",
        "serverId",
        "userRole",
        and(vec![eq("userId", json!(auth)), eq("canAdmin", json!(true))]),
    )
}

// channel.ts channelUserRoles junction (two-hop): channel -> channelPermission
// (by channelId) -> userRole (by roleId) matching the user.
fn user_has_channel_role(auth: &str) -> Value {
    exists(
        "id",
        "channelId",
        "channelPermission",
        exists("roleId", "roleId", "userRole", eq("userId", json!(auth))),
    )
}

// channel.ts hasChannelReadPermission: not deleted AND the server is readable
// AND (channel not private OR the user has a channel role OR is a server admin).
fn channel_readable(auth: &str) -> Value {
    and(vec![
        ne("deleted", json!(true)),
        exists("serverId", "id", "server", server_readable(auth)),
        or(vec![
            eq("private", json!(false)),
            user_has_channel_role(auth),
            server_admin_for(auth),
        ]),
    ])
}

// message.ts hasMessageReadPermission: not deleted AND channel readable AND the
// solo-channel restriction (not a solo channel, or own message, or a bot).
fn message_readable(auth: &str) -> Value {
    and(vec![
        ne("deleted", json!(true)),
        exists("channelId", "id", "channel", channel_readable(auth)),
        or(vec![
            not_exists("channelId", "id", "channel", eq("solo", json!(true))),
            eq("creatorId", json!(auth)),
            eq("type", json!("bot")),
        ]),
    ])
}

// ---- fixture --------------------------------------------------------------

fn fixture() -> TestDb {
    let mut db = TestDb::memory();
    for ddl in [
        "CREATE TABLE server (id TEXT PRIMARY KEY, private INTEGER, creatorId TEXT)",
        "CREATE TABLE serverMember (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT)",
        "CREATE TABLE userRole (id TEXT PRIMARY KEY, serverId TEXT, userId TEXT, roleId TEXT, canAdmin INTEGER)",
        "CREATE TABLE channelPermission (id TEXT PRIMARY KEY, channelId TEXT, roleId TEXT)",
        "CREATE TABLE channel (id TEXT PRIMARY KEY, serverId TEXT, private INTEGER, deleted INTEGER, solo INTEGER)",
        "CREATE TABLE message (id TEXT PRIMARY KEY, channelId TEXT, creatorId TEXT, type TEXT, deleted INTEGER)",
    ] {
        db.exec(ddl, &[]).unwrap();
    }
    // s1 public, s2 private. alice is a member of both; bob is a member of s1 only.
    db.exec(
        "INSERT INTO server VALUES ('s1',0,'owner'), ('s2',1,'owner')",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO serverMember VALUES ('m1','s1','alice'), ('m2','s2','alice'), ('m3','s1','bob')", &[]).unwrap();
    // alice is admin of s1 (role adminRole), and holds channelRole (a non-admin role)
    db.exec("INSERT INTO userRole VALUES ('ur','s1','alice','adminRole',1), ('ur2','s1','alice','channelRole',0)", &[]).unwrap();
    // channels: c1 public in s1; c2 private in s1 (alice has a role via channelPermission); c3 private in s2 (nobody's role); c4 solo in s1
    db.exec("INSERT INTO channel VALUES ('c1','s1',0,0,0), ('c2','s1',1,0,0), ('c3','s2',1,0,0), ('c4','s1',0,0,1)", &[]).unwrap();
    db.exec(
        // c2 grants channelRole; alice holds channelRole -> she can read c2
        "INSERT INTO channelPermission VALUES ('cp','c2','channelRole')",
        &[],
    )
    .unwrap();
    // messages
    db.exec(
        "INSERT INTO message VALUES
         ('msg1','c1','bob','text',0),
         ('msg2','c2','alice','text',0),
         ('msg3','c3','x','text',0),
         ('msg4','c4','alice','text',0),
         ('msg5','c4','bob','text',0),
         ('del1','c1','bob','text',1)",
        &[],
    )
    .unwrap();
    db
}

fn allowed(db: &mut TestDb, table: &str, perm: Value) -> Vec<String> {
    let ast = parse_ast(&json!({ "table": table, "where": perm })).unwrap();
    let compiled = compile(&ast, &tables()).unwrap();
    let mut ids: Vec<String> = db
        .query(&compiled.sql, &compiled.params)
        .unwrap()
        .iter()
        .map(|r| match r.get("id") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    ids.sort();
    ids
}

// ---- allow / deny per family ----------------------------------------------

#[test]
fn server_read_allows_members_and_public_denies_outsiders() {
    let mut db = fixture();
    // alice: member of both -> s1, s2
    assert_eq!(
        allowed(&mut db, "server", server_readable("alice")),
        vec!["s1", "s2"]
    );
    // bob: member of s1 only; s2 is private and he is not a member -> s1
    assert_eq!(
        allowed(&mut db, "server", server_readable("bob")),
        vec!["s1"]
    );
    // carol: no memberships -> only the public server s1
    assert_eq!(
        allowed(&mut db, "server", server_readable("carol")),
        vec!["s1"]
    );
}

#[test]
fn server_admin_allows_only_the_admin() {
    let mut db = fixture();
    assert_eq!(
        allowed(&mut db, "server", server_admin_for("alice")),
        vec!["s1"]
    );
    assert!(allowed(&mut db, "server", server_admin_for("bob")).is_empty());
}

#[test]
fn channel_read_honors_privacy_role_and_admin() {
    let mut db = fixture();
    // alice: c1 (public), c2 (has role / is s1 admin), c4 (public solo). NOT c3
    // (private in s2, no role, not admin of s2).
    assert_eq!(
        allowed(&mut db, "channel", channel_readable("alice")),
        vec!["c1", "c2", "c4"]
    );
    // bob: member of s1 -> c1 (public), c4 (public). NOT c2 (private, no role, not
    // admin) and NOT c3 (s2, not a member).
    assert_eq!(
        allowed(&mut db, "channel", channel_readable("bob")),
        vec!["c1", "c4"]
    );
    // carol: not a member of s1 (private channels need membership via server-read);
    // c1/c4 are public channels in a PUBLIC server -> readable.
    assert_eq!(
        allowed(&mut db, "channel", channel_readable("carol")),
        vec!["c1", "c4"]
    );
}

#[test]
fn message_read_honors_channel_access_solo_and_deleted() {
    let mut db = fixture();
    // alice: msg1 (c1 readable), msg2 (c2 readable), msg4 (own message in solo c4).
    // NOT msg3 (c3 hidden), NOT msg5 (solo c4, not her message, not a bot), NOT
    // del1 (deleted).
    assert_eq!(
        allowed(&mut db, "message", message_readable("alice")),
        vec!["msg1", "msg2", "msg4"]
    );
    // bob: msg1 (c1 readable). NOT msg2 (c2 private, no role), NOT msg3 (c3), NOT
    // msg4 (solo, not his), msg5 IS his own solo message -> visible.
    assert_eq!(
        allowed(&mut db, "message", message_readable("bob")),
        vec!["msg1", "msg5"]
    );
}

#[test]
fn deny_case_forbidden_rows_never_leak() {
    // the security property: a non-member of the private server s2 can never see
    // its private channel c3 or its messages, through any of the transforms.
    let mut db = fixture();
    assert!(!allowed(&mut db, "channel", channel_readable("bob")).contains(&"c3".to_string()));
    assert!(!allowed(&mut db, "message", message_readable("bob")).contains(&"msg3".to_string()));
    assert!(!allowed(&mut db, "channel", channel_readable("carol")).contains(&"c3".to_string()));
    assert!(!allowed(&mut db, "message", message_readable("carol")).contains(&"msg3".to_string()));
}
