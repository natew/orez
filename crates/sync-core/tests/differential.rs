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
use std::sync::atomic::{AtomicU64, Ordering};

use common::Host;
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use sync_core::Transactor;
use sync_core::pull::Caps;

const CLIENTS: [&str; 3] = ["c1", "c2", "c3"];
const POOL: u8 = 6;
static TRACE_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

// Commands are symbolic: both runners assign mutation ids and cookies from
// their current state. This makes every generated command valid while still
// exercising state-dependent duplicate deletes, rejected transactions, stale
// pulls, invalidation, and writes outside the mutation path.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Op {
    Put {
        client: String,
        item: String,
        label: String,
        rank: f64,
        done: bool,
        meta: Value,
    },
    Del {
        client: String,
        item: String,
    },
    Reject {
        client: String,
    },
    Upstream {
        sql: String,
    },
    Pull {
        client: String,
    },
    Invalidate,
}

fn client() -> impl Strategy<Value = String> {
    prop::sample::select(&CLIENTS).prop_map(str::to_owned)
}

fn item() -> impl Strategy<Value = String> {
    (0..POOL).prop_map(|id| format!("k{id}"))
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (client(), item(), 0u16..1000, any::<bool>(), any::<bool>()).prop_map(
            |(client, item, n, done, with_meta)| Op::Put {
                client,
                item,
                label: format!("l{n}"),
                rank: f64::from(n) / 7.0,
                done,
                meta: if with_meta { json!({ "s": n }) } else { Value::Null },
            },
        ),
        2 => (client(), item()).prop_map(|(client, item)| Op::Del { client, item }),
        1 => client().prop_map(|client| Op::Reject { client }),
        2 => (item(), 0u8..50, any::<bool>()).prop_map(|(item, n, put)| {
            let sql = if put {
                format!(
                    "INSERT INTO item (id,label,rank,done,meta) VALUES ('{item}','u{n}',{},0,NULL) ON CONFLICT (id) DO UPDATE SET label=excluded.label",
                    f64::from(n) / 3.0
                )
            } else {
                format!("DELETE FROM item WHERE id='{item}'")
            };
            Op::Upstream { sql }
        }),
        3 => client().prop_map(|client| Op::Pull { client }),
        1 => Just(Op::Invalidate),
    ]
}

fn trace() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op(), 1..=64).prop_map(|mut ops| {
        // Observe late state and verify the unchanged response after convergence.
        for client in CLIENTS {
            ops.push(Op::Pull {
                client: client.into(),
            });
            ops.push(Op::Pull {
                client: client.into(),
            });
        }
        ops
    })
}

fn fixed_client(rng: &mut Rng) -> String {
    CLIENTS[rng.below(3) as usize].to_owned()
}

fn fixed_item(rng: &mut Rng) -> String {
    format!("k{}", rng.below(u64::from(POOL)))
}

// Preserve the original stable corpus alongside proptest. These long traces
// provide coverage-neutral migration while the property lane adds shrinking.
fn fixed_trace(seed: u64, steps: u64) -> Vec<Op> {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0xABC));
    let mut ops = Vec::new();
    let mut seq = 0u64;
    for step in 0..steps {
        match rng.below(7) {
            0 | 1 => {
                seq += 1;
                // Keep PRNG consumption byte-for-byte compatible with the
                // pre-proptest fixed corpus.
                let rank = rng.below(1000) as f64 / 7.0;
                let client = fixed_client(&mut rng);
                let item = fixed_item(&mut rng);
                let done = rng.boolean();
                let meta = if rng.boolean() {
                    json!({ "s": seq })
                } else {
                    Value::Null
                };
                ops.push(Op::Put {
                    client,
                    item,
                    label: format!("l{seq}"),
                    rank,
                    done,
                    meta,
                });
            }
            2 => ops.push(Op::Del {
                client: fixed_client(&mut rng),
                item: fixed_item(&mut rng),
            }),
            3 => ops.push(Op::Reject {
                client: fixed_client(&mut rng),
            }),
            4 => {
                let item = fixed_item(&mut rng);
                let sql = if rng.boolean() {
                    seq += 1;
                    format!(
                        "INSERT INTO item (id,label,rank,done,meta) VALUES ('{item}','u{seq}',{},0,NULL) ON CONFLICT (id) DO UPDATE SET label=excluded.label",
                        rng.below(50) as f64 / 3.0
                    )
                } else {
                    format!("DELETE FROM item WHERE id='{item}'")
                };
                ops.push(Op::Upstream { sql });
            }
            5 => ops.push(Op::Pull {
                client: fixed_client(&mut rng),
            }),
            _ if step % 40 == 39 => ops.push(Op::Invalidate),
            _ => ops.push(Op::Pull {
                client: fixed_client(&mut rng),
            }),
        }
    }
    for client in CLIENTS {
        ops.push(Op::Pull {
            client: client.into(),
        });
        ops.push(Op::Pull {
            client: client.into(),
        });
    }
    ops
}

// run the trace through the Rust engine, returning the pull responses in order
fn run_rust(trace: &[Op]) -> Vec<Value> {
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
        match op {
            Op::Put {
                client,
                item,
                label,
                rank,
                done,
                meta,
            } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one(
                    "item.put",
                    json!({ "id": item, "label": label, "rank": rank, "done": done, "meta": meta }),
                    &client,
                    "g1",
                    id,
                    "u1",
                )
                .unwrap();
            }
            Op::Del { client, item } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one("item.del", json!({ "id": item }), &client, "g1", id, "u1")
                    .unwrap();
            }
            Op::Reject { client } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one("item.reject", json!({}), &client, "g1", id, "u1")
                    .unwrap();
            }
            Op::Upstream { sql } => h.exec(sql),
            Op::Invalidate => {
                h.db.transaction(|db| sync_core::invalidate(db)).unwrap();
            }
            Op::Pull { client } => {
                let client = client.clone();
                let cookie = cookies.get(&client).cloned().unwrap_or(json!(null));
                let resp = h.pull_as(&client, "g1", cookie, None, "u1").unwrap();
                cookies.insert(client, resp["cookie"].clone());
                pulls.push(resp);
            }
        }
    }
    pulls
}

fn run_ts(trace: &[Op]) -> Vec<Value> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let oracle = format!("{manifest}/ts-oracle/run-oracle.ts");
    let sequence = TRACE_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let trace_path = std::env::temp_dir().join(format!(
        "sync-core-diff-{}-{sequence}.json",
        std::process::id(),
    ));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&trace_path)
        .expect("create unique trace file");
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
            "oracle failed; trace preserved at {}: {}\n{}",
            trace_path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let parsed = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse oracle json; trace preserved at {}: {error}",
            trace_path.display()
        )
    });
    std::fs::remove_file(&trace_path).expect("remove successful oracle trace");
    parsed
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

fn compare(rust: &[Value], ts: &[Value]) -> Result<(), String> {
    if rust.len() != ts.len() {
        return Err(format!(
            "pull count differs: rust={} ts={}",
            rust.len(),
            ts.len()
        ));
    }
    for (i, (r, t)) in rust.iter().zip(ts.iter()).enumerate() {
        if r["cookie"] != t["cookie"] {
            return Err(format!("pull {i}: cookie differs\nrust={r}\nts={t}"));
        }
        if r.get("unchanged").is_some() != t.get("unchanged").is_some() {
            return Err(format!(
                "pull {i}: unchanged flag differs\nrust={r}\nts={t}"
            ));
        }
        if r.get("unchanged").is_some() {
            continue;
        }
        if normalize_patch(r) != normalize_patch(t) {
            return Err(format!("pull {i}: rowsPatch differs\nrust={r}\nts={t}"));
        }
        // rust lmids must be a subset of ts lmids with identical values
        let rl = r["lastMutationIDChanges"].as_object().unwrap();
        let tl = t["lastMutationIDChanges"].as_object().unwrap();
        for (client, lmid) in rl {
            if tl.get(client) != Some(lmid) {
                return Err(format!(
                    "pull {i}: rust acks {client}->{lmid} not matched by ts {t}"
                ));
            }
        }
    }
    Ok(())
}

fn property_config() -> Config {
    let default = Config::default();
    // Each case launches the black-box Bun oracle. Keep pull requests bounded;
    // nightly can raise this without changing code, e.g. PROPTEST_CASES=256.
    let cases = if std::env::var_os("PROPTEST_CASES").is_none() {
        12
    } else {
        default.cases
    };
    Config {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/differential.txt"
        )))),
        ..default
    }
}

fn failure_envelope(reason: String, ops: &[Op], process_id: u32) -> Value {
    let cases = std::env::var("PROPTEST_CASES").unwrap_or_else(|_| "12".into());
    let seed = std::env::var("PROPTEST_RNG_SEED").ok();
    // The nightly collector gives this envelope a stable basename. Discovering
    // it beneath cwd keeps the command valid after `gh run download`, whether
    // upload-artifact retained the results wrapper or extracted its contents.
    let replay_path = format!(
        "$(find . -type f -path '*/{process_id}/sync-core-differential-minimized.json' -exec realpath {{}} \\; -quit)"
    );
    json!({
        "schemaVersion": 1,
        "kind": "sync-core-differential",
        "seed": {
            "value": seed.as_deref().unwrap_or("persisted by proptest after failure"),
            "source": if seed.is_some() { "PROPTEST_RNG_SEED" } else { "proptest runner" },
        },
        "generator": {
            "name": "sync-core-operation-state-machine",
            "version": 1,
            "cases": cases,
        },
        "replay": {
            "command": format!(
                "SYNC_CORE_REPLAY=\"{}\" cargo test -p sync-core --test differential replay_saved_differential_trace -- --ignored --exact --nocapture",
                replay_path
            ),
            "env": {},
        },
        "input": { "trace": ops },
        "failure": { "message": reason },
    })
}

fn persist_failure_envelope(reason: String, ops: &[Op]) -> (std::path::PathBuf, Value) {
    // Proptest invokes the body repeatedly while shrinking. Replacing this
    // process-scoped file means the final write is the latest (minimized)
    // failing input instead of leaving one artifact per shrink attempt.
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("sync-core is nested under workspace/crates");
    let process_id = std::process::id();
    let path = workspace
        .join("target/sync-core-differential")
        .join(process_id.to_string())
        .join("sync-core-differential-minimized.json");
    std::fs::create_dir_all(path.parent().unwrap()).expect("create differential results dir");
    let staging = path.with_extension("json.writing");
    let envelope = failure_envelope(reason, ops, process_id);
    std::fs::write(&staging, serde_json::to_vec_pretty(&envelope).unwrap())
        .expect("write differential failure artifact");
    std::fs::rename(&staging, &path).expect("publish differential failure artifact");
    (path, envelope)
}

#[test]
#[ignore = "set SYNC_CORE_REPLAY to a saved differential envelope"]
fn replay_saved_differential_trace() {
    let path = std::env::var_os("SYNC_CORE_REPLAY").expect("SYNC_CORE_REPLAY artifact path");
    let bytes = std::fs::read(&path).expect("read SYNC_CORE_REPLAY artifact");
    let envelope: Value = serde_json::from_slice(&bytes).expect("parse replay envelope");
    assert_eq!(envelope["schemaVersion"], 1, "unsupported replay schema");
    assert_eq!(
        envelope["kind"], "sync-core-differential",
        "wrong replay artifact kind"
    );
    let ops: Vec<Op> = serde_json::from_value(envelope["input"]["trace"].clone())
        .expect("deserialize replay input.trace");
    let rust = run_rust(&ops);
    let ts = run_ts(&ops);
    if let Err(reason) = compare(&rust, &ts) {
        panic!(
            "replayed differential failure from {}: {reason}",
            std::path::Path::new(&path).display()
        );
    }
}

#[test]
fn rust_matches_the_ts_reference_core_on_fixed_traces() {
    for seed in 0..8 {
        let ops = fixed_trace(seed, 200);
        let rust = run_rust(&ops);
        let ts = run_ts(&ops);
        if let Err(reason) = compare(&rust, &ts) {
            panic!(
                "fixed seed {seed}: {reason}\ntrace={}",
                serde_json::to_string_pretty(&ops).unwrap()
            );
        }
    }
}

proptest! {
    #![proptest_config(property_config())]

    #[test]
    fn rust_matches_the_ts_reference_core_on_generated_traces(ops in trace()) {
        let rust = run_rust(&ops);
        let ts = run_ts(&ops);
        if let Err(reason) = compare(&rust, &ts) {
            let (artifact_path, envelope) = persist_failure_envelope(reason, &ops);
            let artifact = serde_json::to_string_pretty(&envelope).unwrap();
            prop_assert!(false,
                "minimized failure artifact saved at {}:\n{artifact}\n\nReplay with replay.command in the artifact. The minimized seed is also saved in crates/sync-core/proptest-regressions/differential.txt; to replay the original RNG stream, use the seed printed by proptest as PROPTEST_RNG_SEED with PROPTEST_CASES=1.",
                artifact_path.display()
            );
        }
    }
}
