// generated operation-trace differential against the TypeScript reference core.
// a deterministic PRNG generates a trace of high-level ops; the Rust engine and
// the TS core (src/sync-server/sync-server.ts, run by ts-oracle/run-oracle.ts
// under bun) each execute the SAME trace with identical per-client id/cookie
// bookkeeping, and their pull responses are compared.
//
// comparison is SEMANTIC where the two cores legitimately differ:
// - cookie: exact (both are the change-log watermark)
// - unchanged flag: exact
// - rowsPatch: order-independent (a clear must lead; the rest is a set) — both
//   resolve the same touched pks against the same live rows
// - lastMutationIDChanges: the Rust core derives diff acks from the INCLUDED log
//   prefix (soot semantics), the reference core from a full clients read, so the
//   Rust map is a subset of the reference map with identical values — every
//   (client,lmid) the Rust core reports must equal the reference's. this is the
//   representational difference (a client already knows its unchanged peers'
//   lmids); it can never ack ahead of effects.
//
// REQUIRES bun on PATH (documented in the crate README / ts-oracle). run with a
// normal `cargo test -p sync-core`.
mod common;

use std::collections::HashMap;
use std::io::Write;
use std::process::Command;

use common::Host;
use serde_json::{Value, json};

use sync_core::Transactor;
use sync_core::pull::Caps;

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
const POOL: u64 = 6;

// generate a trace of high-level ops (symbolic: ids/cookies assigned by the
// runner, identically in Rust and TS)
fn make_trace(seed: u64, steps: u64) -> Vec<Value> {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0xABC));
    let mut ops = Vec::new();
    let mut seq = 0u64;
    for step in 0..steps {
        match rng.below(7) {
            0 | 1 => {
                seq += 1;
                let rank = (rng.below(1000) as f64) / 7.0;
                ops.push(json!({
                    "op": "put",
                    "client": CLIENTS[rng.below(3) as usize],
                    "item": format!("k{}", rng.below(POOL)),
                    "label": format!("l{seq}"),
                    "rank": rank,
                    "done": rng.boolean(),
                    "meta": if rng.boolean() { json!({ "s": seq }) } else { json!(null) },
                }));
            }
            2 => ops.push(json!({
                "op": "del",
                "client": CLIENTS[rng.below(3) as usize],
                "item": format!("k{}", rng.below(POOL)),
            })),
            3 => ops.push(json!({ "op": "reject", "client": CLIENTS[rng.below(3) as usize] })),
            4 => {
                let item = format!("k{}", rng.below(POOL));
                if rng.boolean() {
                    seq += 1;
                    ops.push(json!({ "op": "upstream", "sql":
                        format!("INSERT INTO item (id,label,rank,done,meta) VALUES ('{item}','u{seq}',{},0,NULL) ON CONFLICT (id) DO UPDATE SET label=excluded.label", (rng.below(50) as f64)/3.0) }));
                } else {
                    ops.push(json!({ "op": "upstream", "sql": format!("DELETE FROM item WHERE id='{item}'") }));
                }
            }
            5 => ops.push(json!({ "op": "pull", "client": CLIENTS[rng.below(3) as usize] })),
            _ => {
                if step % 40 == 39 {
                    ops.push(json!({ "op": "invalidate" }));
                } else {
                    ops.push(json!({ "op": "pull", "client": CLIENTS[rng.below(3) as usize] }));
                }
            }
        }
    }
    // drain: pull every client at the end so late state is observed
    for c in CLIENTS {
        ops.push(json!({ "op": "pull", "client": c }));
        ops.push(json!({ "op": "pull", "client": c }));
    }
    ops
}

// run the trace through the Rust engine, returning the pull responses in order
fn run_rust(trace: &[Value]) -> Vec<Value> {
    let mut h = Host::new(true);
    h.init();
    // uncapped, to mirror the reference core (which has no caps)
    h.caps = Caps {
        max_change_rows: 1_000_000,
        max_change_bytes: usize::MAX,
    };
    let mut next_id: HashMap<String, i64> = HashMap::new();
    let mut cookies: HashMap<String, Value> = HashMap::new();
    let mut pulls = Vec::new();

    for op in trace {
        match op["op"].as_str().unwrap() {
            kind @ ("put" | "del" | "reject") => {
                let client = op["client"].as_str().unwrap().to_string();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                let (name, args) = match kind {
                    "put" => (
                        "item.put",
                        json!({ "id": op["item"], "label": op["label"], "rank": op["rank"], "done": op["done"], "meta": op["meta"] }),
                    ),
                    "del" => ("item.del", json!({ "id": op["item"] })),
                    _ => ("item.reject", json!({})),
                };
                h.push_one(name, args, &client, "g1", id, "u1").unwrap();
            }
            "upstream" => h.exec(op["sql"].as_str().unwrap()),
            "invalidate" => {
                h.db.transaction(|db| sync_core::invalidate(db)).unwrap();
            }
            "pull" => {
                let client = op["client"].as_str().unwrap().to_string();
                let cookie = cookies.get(&client).cloned().unwrap_or(json!(null));
                let resp = h.pull_as(&client, "g1", cookie, None, "u1").unwrap();
                cookies.insert(client, resp["cookie"].clone());
                pulls.push(resp);
            }
            other => panic!("unknown op {other}"),
        }
    }
    pulls
}

fn run_ts(trace: &[Value], seed: u64) -> Vec<Value> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let oracle = format!("{manifest}/ts-oracle/run-oracle.ts");
    let trace_path = std::env::temp_dir().join(format!("sync-core-diff-{seed}.json"));
    let mut f = std::fs::File::create(&trace_path).expect("write trace file");
    f.write_all(serde_json::to_string(trace).unwrap().as_bytes())
        .unwrap();
    drop(f);

    let output = Command::new("bun")
        .arg(&oracle)
        .arg(&trace_path)
        .output()
        .expect("run bun oracle — is bun on PATH? the differential test requires it");
    if !output.status.success() {
        panic!(
            "oracle failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).expect("parse oracle json")
}

// (has_clear, sorted non-clear ops) — rowsPatch order is not semantic
fn normalize_patch(resp: &Value) -> (bool, Vec<String>) {
    let patch = resp["rowsPatch"].as_array().cloned().unwrap_or_default();
    let has_clear = patch.first() == Some(&json!({ "op": "clear" }));
    let mut ops: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] != "clear")
        .map(|op| serde_json::to_string(op).unwrap())
        .collect();
    ops.sort();
    (has_clear, ops)
}

fn compare(trace: &[Value], rust: &[Value], ts: &[Value], seed: u64) {
    assert_eq!(rust.len(), ts.len(), "seed {seed}: pull count differs");
    for (i, (r, t)) in rust.iter().zip(ts.iter()).enumerate() {
        assert_eq!(
            r["cookie"], t["cookie"],
            "seed {seed} pull {i}: cookie differs\nrust={r}\nts={t}"
        );
        assert_eq!(
            r.get("unchanged").is_some(),
            t.get("unchanged").is_some(),
            "seed {seed} pull {i}: unchanged flag differs\nrust={r}\nts={t}"
        );
        if r.get("unchanged").is_some() {
            continue;
        }
        assert_eq!(
            normalize_patch(r),
            normalize_patch(t),
            "seed {seed} pull {i}: rowsPatch differs\nrust={r}\nts={t}"
        );
        // rust lmids must be a subset of ts lmids with identical values
        let rl = r["lastMutationIDChanges"].as_object().unwrap();
        let tl = t["lastMutationIDChanges"].as_object().unwrap();
        for (client, lmid) in rl {
            assert_eq!(
                tl.get(client),
                Some(lmid),
                "seed {seed} pull {i}: rust acks {client}->{lmid} not matched by ts {t}"
            );
        }
    }
    let _ = trace;
}

#[test]
fn rust_matches_the_ts_reference_core_on_generated_traces() {
    for seed in 0..8u64 {
        let trace = make_trace(seed, 200);
        let rust = run_rust(&trace);
        let ts = run_ts(&trace, seed);
        compare(&trace, &rust, &ts, seed);
    }
}
