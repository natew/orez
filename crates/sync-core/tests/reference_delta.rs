// the reference core's delta correctness suite (src/sync-server/sync-server.test.ts)
// ported verbatim: every named test and every table-driven case. drives the
// engine through the synchronous test host (rusqlite + the push-step driver).
mod common;

use common::{Host, item_tables};
use serde_json::{Value, json};

use sync_core::pull::{Caps, Visibility, VisibleFilter};
use sync_core::{EngineError, SqlValue, Transactor, handle_pull, invalidate, push_validate};

// ---- helpers mirroring the TS suite's push()/pull()/patchOf() -------------

fn setup() -> Host {
    let mut h = Host::new(true);
    // seed before init_schema: the seed row stays out of the change log
    h.exec(
        "INSERT INTO item (id, label, rank, done, meta)
         VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')",
    );
    h.init();
    h
}

fn cookie_of(resp: &Value) -> i64 {
    resp["cookie"].as_i64().unwrap()
}

fn patch_of(resp: &Value) -> &Vec<Value> {
    resp["rowsPatch"].as_array().expect("rowsPatch present")
}

fn puts(patch: &[Value]) -> Vec<&Value> {
    patch.iter().filter(|op| op["op"] == "put").collect()
}

// ---- request validation ---------------------------------------------------

#[test]
fn rejects_invalid_pull_cookies() {
    // the reference's table was [undefined, "0", -1, 1.5, NaN, +Infinity].
    // under the pinned wire decision a CANONICAL decimal string ("0", "5") is a
    // valid cookie, so the reference's string-"0" rejection is replaced by
    // non-canonical string rejection; NaN/Infinity can't exist in parsed JSON.
    let cases = vec![
        json!(-1),
        json!(1.5),
        json!({}),
        json!([]),
        json!(true),
        json!("abc"), // non-numeric string
        json!("01"),  // non-canonical (leading zero)
        json!("-5"),  // signed
        json!("1.5"), // fractional string
    ];
    for cookie in cases {
        let mut h = setup();
        let tables = item_tables();
        let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie });
        let err = h
            .db
            .transaction(|db| handle_pull(db, &tables, 4096, None, Caps::default(), &body, "u1"))
            .unwrap_err();
        assert_eq!(err.status, 400, "cookie {cookie} should 400");
    }

    // undefined (missing cookie field) is malformed, like the reference
    let mut h = setup();
    let tables = item_tables();
    let body = json!({ "clientID": "c1", "clientGroupID": "g1" });
    let err =
        h.db.transaction(|db| handle_pull(db, &tables, 4096, None, Caps::default(), &body, "u1"))
            .unwrap_err();
    assert_eq!(err.status, 400, "missing cookie should 400");
}

#[test]
fn canonical_string_cookie_is_accepted() {
    // the pinned wire decision: a canonical unsigned base-10 string is a valid
    // cookie (sol-m0's precision-safe boundary format). "0" == watermark 0.
    let mut h = setup();
    let tables = item_tables();
    let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": "0" });
    let resp =
        h.db.transaction(|db| handle_pull(db, &tables, 4096, None, Caps::default(), &body, "u1"))
            .unwrap();
    assert_eq!(resp, json!({ "cookie": 0, "unchanged": true }));
}

#[test]
fn rejects_malformed_pull_body() {
    let cases = vec![
        json!({ "clientID": 1, "clientGroupID": "g1", "cookie": null }),
        json!({ "clientID": "c1", "clientGroupID": 1, "cookie": null }),
        json!(null),
    ];
    for body in cases {
        let mut h = setup();
        let tables = item_tables();
        let err = h
            .db
            .transaction(|db| handle_pull(db, &tables, 4096, None, Caps::default(), &body, "u1"))
            .unwrap_err();
        assert_eq!(err.status, 400);
    }
}

fn valid_push_body(over: Value) -> Value {
    let mut mutation = json!({
        "type": "custom", "id": 1, "clientID": "c1", "name": "item.put",
        "args": [{ "id": "validated", "label": "ok", "rank": 1, "done": false, "meta": null }],
        "timestamp": 0,
    });
    if let Value::Object(o) = over {
        for (k, v) in o {
            mutation[k] = v;
        }
    }
    json!({ "clientGroupID": "g1", "mutations": [mutation], "pushVersion": 1, "requestID": "validated-request" })
}

#[test]
fn rejects_malformed_mutations() {
    let cases = vec![
        json!({ "id": 1.5 }),
        json!({ "id": 0 }),
        json!({ "clientID": 7 }),
        json!({ "name": 7 }),
        json!({ "args": { "id": "x" } }),
        json!({ "type": "crud" }),
    ];
    for over in cases {
        let body = valid_push_body(over.clone());
        let err = push_validate(&body).err().expect("should 400");
        assert_eq!(err.status, 400, "mutation override {over} should 400");
    }
}

#[test]
fn rejects_malformed_push_body() {
    let cases = vec![
        json!(null),
        json!({ "clientGroupID": 1, "mutations": [], "pushVersion": 1 }),
        json!({ "clientGroupID": "g1", "mutations": {}, "pushVersion": 1 }),
        json!({ "clientGroupID": "g1", "mutations": [], "pushVersion": "1" }),
    ];
    for body in cases {
        assert_eq!(push_validate(&body).err().map(|e| e.status), Some(400));
    }
}

#[test]
fn validates_entire_push_before_processing_first_mutation() {
    let mut h = setup();
    let mut body = valid_push_body(json!({}));
    let extra = json!({ "type": "custom", "id": 2, "clientID": "c1", "name": 42, "args": [{}], "timestamp": 0 });
    body["mutations"].as_array_mut().unwrap().push(extra);

    let err = h.push_from_body(&body, "u1").unwrap_err();
    assert_eq!(err.status, 400);
    assert!(h.query_item("validated").is_none());
    assert_eq!(h.watermark(), 0);
}

#[test]
fn returns_unsupported_push_version_without_processing() {
    let mut h = setup();
    let mut body = valid_push_body(json!({}));
    body["pushVersion"] = json!(2);
    let resp = h.push_from_body(&body, "u1").unwrap();
    assert_eq!(
        resp,
        json!({ "pushResponse": { "error": "unsupportedPushVersion", "mutationIDs": [{ "clientID": "c1", "id": 1 }] } })
    );
    assert!(h.query_item("validated").is_none());
    assert_eq!(h.watermark(), 0);
}

// ---- snapshot and unchanged -----------------------------------------------

#[test]
fn fresh_pull_is_clear_puts_snapshot_with_typed_values() {
    let mut h = setup();
    let resp = h.pull(json!(null), "u1").unwrap();
    let patch = patch_of(&resp);
    assert_eq!(patch[0], json!({ "op": "clear" }));
    assert_eq!(
        patch[1],
        json!({ "op": "put", "tableName": "item",
                "value": { "id": "seed1", "label": "first", "rank": 1.5, "done": false, "meta": { "tag": "a" } } })
    );
    assert_eq!(cookie_of(&resp), 0);
}

#[test]
fn same_cookie_pull_is_unchanged() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    assert_eq!(
        h.pull(json!(cookie), "u1").unwrap(),
        json!({ "cookie": cookie, "unchanged": true })
    );
}

#[test]
fn future_cookie_is_409() {
    let mut h = setup();
    let err = h.pull(json!(99), "u1").unwrap_err();
    assert_eq!(err.status, 409);
}

// ---- cursor diffs ---------------------------------------------------------

#[test]
fn insert_arrives_as_put_diff_without_clear_floats_exact() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    let rank = 0.1 + 0.2; // 0.30000000000000004 — 17 significant digits
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": rank, "done": true, "meta": [1, "x"] }),
        1,
    );
    let resp = h.pull(json!(cookie), "u1").unwrap();
    let patch = patch_of(&resp);
    assert!(!patch.iter().any(|op| op["op"] == "clear"));
    let put = patch.iter().find(|op| op["op"] == "put").unwrap();
    assert_eq!(
        put["value"],
        json!({ "id": "i2", "label": "two", "rank": rank, "done": true, "meta": [1, "x"] })
    );
    // exact, not sqlite json's 15-digit form
    assert_eq!(put["value"]["rank"].as_f64().unwrap(), rank);
}

#[test]
fn update_arrives_as_put_of_only_the_touched_row() {
    let mut h = setup();
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": 2, "done": false, "meta": null }),
        1,
    );
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "i2",
        json!({ "id": "i2", "label": "renamed", "rank": 2, "done": false, "meta": null }),
        2,
    );
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    let ps = puts(&patch);
    assert_eq!(ps.len(), 1);
    assert_eq!(ps[0]["value"]["id"], json!("i2"));
    assert_eq!(ps[0]["value"]["label"], json!("renamed"));
}

#[test]
fn delete_arrives_as_del_with_primary_key() {
    let mut h = setup();
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": 2, "done": false, "meta": null }),
        1,
    );
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.del("i2", 2);
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(
        patch,
        vec![json!({ "op": "del", "tableName": "item", "id": { "id": "i2" } })]
    );
}

#[test]
fn delete_then_recreate_collapses_to_put() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.del("seed1", 1);
    h.put(
        "seed1",
        json!({ "id": "seed1", "label": "reborn", "rank": 9, "done": false, "meta": null }),
        2,
    );
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(
        patch,
        vec![json!({ "op": "put", "tableName": "item",
                     "value": { "id": "seed1", "label": "reborn", "rank": 9, "done": false, "meta": null } })]
    );
}

#[test]
fn insert_then_delete_collapses_to_del() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "ephemeral",
        json!({ "id": "ephemeral", "label": "x", "rank": 0, "done": false, "meta": null }),
        1,
    );
    h.del("ephemeral", 2);
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(
        patch,
        vec![json!({ "op": "del", "tableName": "item", "id": { "id": "ephemeral" } })]
    );
}

#[test]
fn upstream_sql_outside_push_advances_watermark() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.exec("UPDATE item SET label = 'edited behind zero' WHERE id = 'seed1'");
    let resp = h.pull(json!(cookie), "u1").unwrap();
    assert!(cookie_of(&resp) > cookie);
    assert_eq!(
        patch_of(&resp).clone(),
        vec![json!({ "op": "put", "tableName": "item",
                     "value": { "id": "seed1", "label": "edited behind zero", "rank": 1.5, "done": false, "meta": { "tag": "a" } } })]
    );
}

#[test]
fn pk_changing_update_dels_old_and_puts_new() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.exec("UPDATE item SET id = 'seed1-renamed' WHERE id = 'seed1'");
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert!(patch.contains(&json!({ "op": "del", "tableName": "item", "id": { "id": "seed1" } })));
    assert!(patch.contains(&json!({ "op": "put", "tableName": "item",
        "value": { "id": "seed1-renamed", "label": "first", "rank": 1.5, "done": false, "meta": { "tag": "a" } } })));
}

// ---- push semantics -------------------------------------------------------

#[test]
fn app_error_advances_lmid_and_watermark_but_no_rows() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    let resp = h
        .push_one("item.reject", json!({}), "c1", "g1", 1, "u1")
        .unwrap();
    assert_eq!(
        resp["pushResponse"]["mutations"][0]["result"],
        json!({ "error": "app", "message": "nope", "details": "nope" })
    );
    assert!(h.query_item("rejected").is_none());
    let next = h.pull(json!(cookie), "u1").unwrap();
    assert!(cookie_of(&next) > cookie);
    assert_eq!(next["lastMutationIDChanges"]["c1"], json!(1));
    assert_eq!(patch_of(&next).clone(), Vec::<Value>::new());
}

#[test]
fn replayed_mutation_acks_idempotently() {
    let mut h = setup();
    h.push_one(
        "item.put",
        json!({ "id": "i2", "label": "once", "rank": 1, "done": false, "meta": null }),
        "c1",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    let replay = h
        .push_one(
            "item.put",
            json!({ "id": "i2", "label": "twice?", "rank": 1, "done": false, "meta": null }),
            "c1",
            "g1",
            1,
            "u1",
        )
        .unwrap();
    assert_eq!(
        replay["pushResponse"]["mutations"][0]["result"],
        json!({ "error": "alreadyProcessed",
                "details": "Ignoring mutation from c1 with ID 1 as it was already processed. Expected: 2" })
    );
    assert_eq!(
        h.query_item("i2").unwrap()["label"].as_str().unwrap(),
        "once"
    );
}

#[test]
fn out_of_order_mutation_id_is_400() {
    let mut h = setup();
    let err = h
        .push_one("item.put", json!({ "id": "x" }), "c1", "g1", 5, "u1")
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("skips lmid"));
}

#[test]
fn two_tabs_settle_through_last_mutation_id_changes() {
    let mut h = setup();
    h.push_one(
        "item.put",
        json!({ "id": "a", "label": "a", "rank": 0, "done": false, "meta": null }),
        "tab1",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    h.push_one(
        "item.put",
        json!({ "id": "b", "label": "b", "rank": 0, "done": false, "meta": null }),
        "tab2",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    let resp = h.pull_as("tab1", "g1", json!(null), None, "u1").unwrap();
    assert_eq!(
        resp["lastMutationIDChanges"],
        json!({ "tab1": 1, "tab2": 1 })
    );
}

#[test]
fn client_group_claimed_by_one_user_rejects_another() {
    let mut h = setup();
    h.pull(json!(null), "u1").unwrap();
    let err = h.pull(json!(null), "intruder").unwrap_err();
    assert_eq!(err.status, 403);
    assert!(err.message.contains("different user"));
}

// ---- retention floor ------------------------------------------------------

#[test]
fn pull_prunes_upstream_churn_before_unchanged() {
    let mut h = Host::new(true);
    h.retain = 2;
    h.exec("INSERT INTO item (id, label, rank, done, meta) VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')");
    h.init();
    let ancient = cookie_of(&h.pull(json!(null), "u1").unwrap());
    for i in 0..6 {
        h.exec(&format!(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('upstream-{i}', 'upstream {i}', {i}, 0, NULL)"
        ));
    }
    let current = h.watermark();
    assert_eq!(h.change_count(), 6);
    assert_eq!(
        h.pull(json!(current), "u1").unwrap(),
        json!({ "cookie": current, "unchanged": true })
    );
    assert_eq!(h.change_count(), 2);
    assert_eq!(h.floor(), current - 2);
    let stale = h.pull(json!(ancient), "u1").unwrap();
    assert_eq!(patch_of(&stale)[0], json!({ "op": "clear" }));
}

#[test]
fn cookie_below_floor_snapshots_recent_cookies_still_diff() {
    let mut h = Host::new(true);
    h.retain = 2;
    h.exec("INSERT INTO item (id, label, rank, done, meta) VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')");
    h.init();
    let ancient = cookie_of(&h.pull(json!(null), "u1").unwrap());
    for i in 0..6 {
        h.push_one("item.put", json!({ "id": format!("i{i}"), "label": format!("l{i}"), "rank": i, "done": false, "meta": null }), "c1", "g1", i + 1, "u1").unwrap();
    }
    let recent = cookie_of(&h.pull_as("c2", "g1", json!(null), None, "u1").unwrap());
    h.push_one(
        "item.put",
        json!({ "id": "last", "label": "last", "rank": 99, "done": false, "meta": null }),
        "c1",
        "g1",
        7,
        "u1",
    )
    .unwrap();

    let stale = h.pull(json!(ancient), "u1").unwrap();
    let stale_patch = patch_of(&stale).clone();
    assert_eq!(stale_patch[0], json!({ "op": "clear" })); // snapshot fallback
    assert!(puts(&stale_patch).len() >= 8);

    let fresh = h.pull_as("c2", "g1", json!(recent), None, "u1").unwrap();
    let fresh_patch = patch_of(&fresh).clone();
    assert!(!fresh_patch.iter().any(|op| op["op"] == "clear")); // still a diff
    assert_eq!(
        fresh_patch,
        vec![json!({ "op": "put", "tableName": "item",
                     "value": { "id": "last", "label": "last", "rank": 99, "done": false, "meta": null } })]
    );
}

// ---- epoch invalidation ---------------------------------------------------

#[test]
fn invalidate_forces_one_snapshot_then_diffs_resume() {
    let mut h = setup();
    let c1 = cookie_of(&h.pull(json!(null), "u1").unwrap());
    // a client that is fully caught up would otherwise answer `unchanged`
    let tables = item_tables();
    h.db.transaction(|db| invalidate(db)).unwrap();
    let after = h.pull(json!(c1), "u1").unwrap();
    assert!(cookie_of(&after) > c1);
    assert_eq!(patch_of(&after)[0], json!({ "op": "clear" })); // full snapshot
    let _ = tables;
    // after re-snapshotting, incremental diffs resume
    h.put(
        "post",
        json!({ "id": "post", "label": "post", "rank": 1, "done": false, "meta": null }),
        1,
    );
    let diff = h.pull(json!(cookie_of(&after)), "u1").unwrap();
    let diff_patch = patch_of(&diff).clone();
    assert!(!diff_patch.iter().any(|op| op["op"] == "clear"));
    assert_eq!(
        diff_patch,
        vec![json!({ "op": "put", "tableName": "item",
                     "value": { "id": "post", "label": "post", "rank": 1, "done": false, "meta": null } })]
    );
}

// ---- per-user visibility --------------------------------------------------

#[test]
fn visible_configs_always_snapshot_filtered_per_user() {
    let mut h = setup();
    // non-row-local visibility (done can flip without touching a row) forces
    // snapshot, exactly like the reference core's `visible`.
    let vis = Visibility {
        row_local: false,
        filter: Box::new(|_table: &str, _user: &str| {
            Some(VisibleFilter {
                sql: "done = 0".into(),
                params: Vec::<SqlValue>::new(),
            })
        }),
    };
    h.push_one(
        "item.put",
        json!({ "id": "hidden", "label": "done item", "rank": 0, "done": true, "meta": null }),
        "c1",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    let cookie = cookie_of(&h.pull_vis(json!(null), Some(&vis), "u1").unwrap());
    h.push_one(
        "item.put",
        json!({ "id": "shown", "label": "open item", "rank": 0, "done": false, "meta": null }),
        "c1",
        "g1",
        2,
        "u1",
    )
    .unwrap();
    let resp = h.pull_vis(json!(cookie), Some(&vis), "u1").unwrap();
    let patch = patch_of(&resp).clone();
    assert_eq!(patch[0], json!({ "op": "clear" })); // never a diff with visibility filtering
    let ids: Vec<Value> = puts(&patch)
        .iter()
        .map(|op| op["value"]["id"].clone())
        .collect();
    assert!(ids.iter().any(|id| id == &json!("shown")));
    assert!(!ids.iter().any(|id| id == &json!("hidden")));
}

// ---- interleaved churn converges ------------------------------------------

#[test]
fn interleaved_pushes_and_upstream_converge() {
    let mut h = setup();
    use std::collections::HashMap;
    let mut stores: HashMap<&str, HashMap<String, Value>> = HashMap::new();
    let mut cookies: HashMap<&str, Value> = HashMap::new();
    cookies.insert("c1", json!(null));
    cookies.insert("c2", json!(null));

    fn apply_pull(
        h: &mut Host,
        client: &'static str,
        stores: &mut std::collections::HashMap<&str, std::collections::HashMap<String, Value>>,
        cookies: &mut std::collections::HashMap<&str, Value>,
    ) {
        let resp = h
            .pull_as(client, "g1", cookies[client].clone(), None, "u1")
            .unwrap();
        cookies.insert(client, resp["cookie"].clone());
        if resp.get("unchanged") == Some(&json!(true)) {
            return;
        }
        let store = stores.entry(client).or_default();
        for op in resp["rowsPatch"].as_array().unwrap() {
            match op["op"].as_str() {
                Some("clear") => store.clear(),
                Some("put") => {
                    store.insert(
                        op["value"]["id"].as_str().unwrap().to_string(),
                        op["value"].clone(),
                    );
                }
                Some("del") => {
                    store.remove(op["id"]["id"].as_str().unwrap());
                }
                _ => {}
            }
        }
    }

    apply_pull(&mut h, "c1", &mut stores, &mut cookies);
    let mut ids: HashMap<&str, i64> = HashMap::new();
    ids.insert("c1", 0);
    for round in 0..20 {
        let id = {
            let e = ids.entry("c1").or_insert(0);
            *e += 1;
            *e
        };
        h.push_one(
            "item.put",
            json!({ "id": format!("r{}", round % 7), "label": format!("round {round}"),
                    "rank": round as f64 + 0.1, "done": round % 2 == 1,
                    "meta": if round % 3 == 0 { json!({ "round": round }) } else { json!(null) } }),
            "c1",
            "g1",
            id,
            "u1",
        )
        .unwrap();
        if round % 4 == 0 {
            h.exec(&format!(
                "DELETE FROM item WHERE id = 'r{}'",
                (round + 3) % 7
            ));
        }
        if round % 5 == 2 {
            apply_pull(&mut h, "c1", &mut stores, &mut cookies);
        }
        if round % 3 == 1 {
            apply_pull(&mut h, "c2", &mut stores, &mut cookies);
        }
    }
    apply_pull(&mut h, "c1", &mut stores, &mut cookies);
    apply_pull(&mut h, "c2", &mut stores, &mut cookies);

    // oracle: a fresh client's full snapshot
    let oracle_resp = h.pull_as("c3", "g1", json!(null), None, "u1").unwrap();
    let mut oracle: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for op in oracle_resp["rowsPatch"].as_array().unwrap() {
        if op["op"] == "put" {
            oracle.insert(
                op["value"]["id"].as_str().unwrap().to_string(),
                op["value"].clone(),
            );
        }
    }
    for client in ["c1", "c2"] {
        assert_eq!(
            &stores[client], &oracle,
            "client {client} diverged from oracle"
        );
    }
}

#[test]
fn row_cap_zero_still_makes_progress() {
    // MEDIUM-8: a maxChangeRows cap of 0 must not stall the diff forever. the
    // engine admits at least one change row per diff, so repeated pulls drain the
    // log instead of echoing the same cookie with an empty patch.
    let mut h = Host::new(true);
    h.init();
    let c0 = cookie_of(&h.pull(json!(null), "u1").unwrap());
    // triggers are installed by init(), so direct inserts append change rows
    for i in 0..3 {
        h.exec(&format!(
            "INSERT INTO item VALUES ('i{i}', 'l', 1.0, 0, NULL)"
        ));
    }
    let target = h.watermark();
    assert!(target > c0);

    h.caps = Caps {
        max_change_rows: 0,
        max_change_bytes: 2_000_000,
    };
    let mut cookie = c0;
    let mut steps = 0;
    loop {
        let resp = h.pull(json!(cookie), "u1").unwrap();
        let next = cookie_of(&resp);
        if resp.get("unchanged") == Some(&json!(true)) || next == target {
            cookie = next;
            break;
        }
        assert!(next > cookie, "cap-0 diff stalled at cookie {cookie}");
        cookie = next;
        steps += 1;
        assert!(steps < 100, "cap-0 diff did not drain the log");
    }
    assert_eq!(cookie, target, "cap-0 diffs drained the whole change log");
}

// exercised indirectly above but keep the type imports honest
#[allow(dead_code)]
fn _uses(_: EngineError) {}
