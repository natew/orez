// randomized model tests: a deterministic hand-rolled PRNG drives interleaved
// pushes, upstream writes, epoch invalidations, and pulls (with capping) across
// several clients, run over many seeds, asserting the load-bearing invariants
// continuously and convergence at the end.
//
// the exit-gate invariant "randomized traces never produce an ack or cookie
// ahead of effects" is checked precisely on the server output: when a push
// advances client C to lmid L, we record the watermark it produced (the lmid
// row's watermark, which sits ABOVE that mutation's row effects). any later pull
// that acks C->L must return a cookie >= that watermark, so the ack (and the
// cookie) can never precede the effects.
mod common;

use std::collections::HashMap;

use common::Host;
use serde_json::{Value, json};

use sync_core::Transactor;
use sync_core::pull::Caps;

// xorshift64* — deterministic, seedable, no external rng dependency
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn boolean(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

const CLIENTS: [&str; 3] = ["c1", "c2", "c3"];
const GROUP: &str = "g1";
const USER: &str = "u1";
const POOL: u64 = 8; // item id pool, so updates / deletes / re-creates happen

struct Model {
    next_id: HashMap<&'static str, i64>,
    stores: HashMap<&'static str, HashMap<String, Value>>,
    cookies: HashMap<&'static str, Value>,
    // watermark produced when client C advanced to lmid L (>= its row effects)
    effect_wm: HashMap<(&'static str, i64), i64>,
}

impl Model {
    fn new() -> Model {
        let mut next_id = HashMap::new();
        let mut stores = HashMap::new();
        let mut cookies = HashMap::new();
        for c in CLIENTS {
            next_id.insert(c, 0);
            stores.insert(c, HashMap::new());
            cookies.insert(c, json!(null));
        }
        Model {
            next_id,
            stores,
            cookies,
            effect_wm: HashMap::new(),
        }
    }
}

fn apply_patch(store: &mut HashMap<String, Value>, patch: &[Value]) {
    for op in patch {
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

fn client_key(s: &str) -> &'static str {
    CLIENTS.iter().copied().find(|c| *c == s).unwrap_or("?")
}

// one pull for `client`, applying the patch and checking every invariant
fn pull_and_check(h: &mut Host, m: &mut Model, client: &'static str, seed: u64) {
    let prev = m.cookies[client].clone();
    let resp = match h.pull_as(client, GROUP, prev.clone(), None, USER) {
        Ok(r) => r,
        Err(e) if e.status == 409 => {
            m.cookies.insert(client, json!(null));
            m.stores.get_mut(client).unwrap().clear();
            return;
        }
        Err(e) => panic!(
            "seed {seed}: unexpected pull error {}: {}",
            e.status, e.message
        ),
    };
    let wm_after = h.watermark();
    let c = resp["cookie"].as_i64().unwrap();

    assert!(
        c <= wm_after,
        "seed {seed}: cookie {c} ahead of watermark {wm_after}"
    );
    if let Some(pc) = prev.as_i64() {
        assert!(
            c >= pc,
            "seed {seed}: cookie regressed {pc} -> {c} for {client}"
        );
    }

    if resp.get("unchanged") == Some(&json!(true)) {
        assert_eq!(
            prev.as_i64(),
            Some(c),
            "seed {seed}: unchanged with a moving cookie"
        );
    } else {
        let patch = resp["rowsPatch"].as_array().unwrap();
        apply_patch(m.stores.get_mut(client).unwrap(), patch);
        for (target, lmid) in resp["lastMutationIDChanges"].as_object().unwrap() {
            let l = lmid.as_i64().unwrap();
            if let Some(w) = m.effect_wm.get(&(client_key(target), l)) {
                assert!(
                    c >= *w,
                    "seed {seed}: ack {target}->{l} (effects @ {w}) ahead of cookie {c}"
                );
            }
        }
    }
    m.cookies.insert(client, resp["cookie"].clone());
}

fn run_trace(seed: u64, steps: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1));
    let mut h = Host::new(true);
    h.init();
    let mut m = Model::new();
    let mut seq = 0u64;

    for step in 0..steps {
        // occasionally shrink caps so diffs truncate and must be drained
        h.caps = match rng.below(4) {
            0 => Caps {
                max_change_rows: 1,
                max_change_bytes: 120,
            },
            1 => Caps {
                max_change_rows: 3,
                max_change_bytes: 300,
            },
            _ => Caps::default(),
        };

        match rng.below(6) {
            0 | 1 => {
                let client = CLIENTS[rng.below(3) as usize];
                let id = {
                    let e = m.next_id.get_mut(client).unwrap();
                    *e += 1;
                    *e
                };
                seq += 1;
                let item = format!("k{}", rng.below(POOL));
                let rank = (rng.below(1000) as f64) / 7.0; // fractional
                let done = rng.boolean();
                let meta = if rng.boolean() {
                    json!({ "s": seq })
                } else {
                    json!(null)
                };
                let res = h
                    .push_one(
                        "item.put",
                        json!({ "id": item, "label": format!("l{seq}"), "rank": rank, "done": done, "meta": meta }),
                        client,
                        GROUP,
                        id,
                        USER,
                    )
                    .unwrap();
                if res["pushResponse"]["mutations"][0]["result"]["error"].is_null() {
                    m.effect_wm.insert((client, id), h.watermark());
                }
            }
            2 => {
                let client = CLIENTS[rng.below(3) as usize];
                let id = {
                    let e = m.next_id.get_mut(client).unwrap();
                    *e += 1;
                    *e
                };
                let item = format!("k{}", rng.below(POOL));
                h.push_one("item.del", json!({ "id": item }), client, GROUP, id, USER)
                    .unwrap();
                m.effect_wm.insert((client, id), h.watermark());
            }
            3 => {
                // app-error push: rolls back its row effect, still advances lmid
                let client = CLIENTS[rng.below(3) as usize];
                let id = {
                    let e = m.next_id.get_mut(client).unwrap();
                    *e += 1;
                    *e
                };
                h.push_one("item.reject", json!({}), client, GROUP, id, USER)
                    .unwrap();
                m.effect_wm.insert((client, id), h.watermark());
            }
            4 => {
                // upstream write straight to the table (feeds the log via triggers)
                let item = format!("k{}", rng.below(POOL));
                if rng.boolean() {
                    seq += 1;
                    h.exec(&format!(
                        "INSERT INTO item (id, label, rank, done, meta) VALUES ('{item}', 'u{seq}', {}, 0, NULL)
                         ON CONFLICT (id) DO UPDATE SET label = excluded.label",
                        (rng.below(50) as f64) / 3.0
                    ));
                } else {
                    h.exec(&format!("DELETE FROM item WHERE id = '{item}'"));
                }
            }
            _ => {
                let client = CLIENTS[rng.below(3) as usize];
                pull_and_check(&mut h, &mut m, client, seed);
            }
        }

        if step % 97 == 96 {
            h.db.transaction(|db| sync_core::invalidate(db)).unwrap();
        }
    }

    // drain every client to the current watermark, then converge to the oracle
    h.caps = Caps::default();
    for client in CLIENTS {
        for _ in 0..128 {
            let before = m.cookies[client].clone();
            pull_and_check(&mut h, &mut m, client, seed);
            if m.cookies[client] == before {
                break;
            }
        }
    }
    let oracle_resp = h
        .pull_as("c1", "oracle-group", json!(null), None, USER)
        .unwrap();
    let mut oracle: HashMap<String, Value> = HashMap::new();
    for op in oracle_resp["rowsPatch"].as_array().unwrap() {
        if op["op"] == "put" {
            oracle.insert(
                op["value"]["id"].as_str().unwrap().to_string(),
                op["value"].clone(),
            );
        }
    }
    for client in CLIENTS {
        assert_eq!(
            &m.stores[client], &oracle,
            "seed {seed}: client {client} did not converge"
        );
    }
}

#[test]
fn randomized_traces_hold_invariants_and_converge() {
    for seed in 0..24u64 {
        run_trace(seed, 500);
    }
}
