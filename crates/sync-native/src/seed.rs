// deterministic fixture seed, a faithful port of harness/src/fixture-data.ts
// `generateSeed(1)`. every target (stock-zero postgres, orez-local sqlite,
// this native host) must produce byte-identical rows or the differential
// lanes fail, so the RNG draw order and float math mirror the TS source
// exactly (see the golden test at the bottom, checked against the TS output).

use sync_core::SqlValue;

// mulberry32: a single shared 32-bit PRNG stream. all arithmetic is 32-bit
// (JS `| 0` / `>>>` / Math.imul), so we track state as i32 and shift through
// u32 to match JS unsigned-right-shift semantics.
struct Rng {
    a: i32,
}

impl Rng {
    fn new(seed: i32) -> Self {
        Self { a: seed }
    }

    fn next(&mut self) -> f64 {
        // a = (a + 0x6d2b79f5) | 0
        self.a = self.a.wrapping_add(0x6d2b79f5u32 as i32);
        let a = self.a;
        // t = imul(a ^ (a >>> 15), 1 | a)
        let mut t = (a ^ (((a as u32) >> 15) as i32)).wrapping_mul(1 | a);
        // t = (t + imul(t ^ (t >>> 7), 61 | t)) ^ t
        t = t
            .wrapping_add((t ^ (((t as u32) >> 7) as i32)).wrapping_mul(61 | t))
            ^ t;
        // ((t ^ (t >>> 14)) >>> 0) / 4294967296
        let bits = (t ^ (((t as u32) >> 14) as i32)) as u32;
        (bits as f64) / 4294967296.0
    }

    // Math.floor(next() * len)
    fn index(&mut self, len: usize) -> usize {
        (self.next() * len as f64).floor() as usize
    }
}

pub struct TaskRow {
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub rank: f64,
    pub done: bool,
    pub meta: Option<String>,
    pub due_at: Option<i64>,
}

pub struct SeedData {
    pub users: Vec<(String, String)>,
    pub projects: Vec<(String, String, String)>,
    pub members: Vec<(String, String, String)>,
    pub tasks: Vec<TaskRow>,
}

const USER_NAMES: &[&str] = &[
    "ann", "bob 🌵", "çelik", "dee", "evan fix", "frida", "gus", "hana",
];
const PROJECT_NAMES: &[&str] = &["alpha", "fixup", "Zenith", "delta x", "ütopia", "omega"];
const TASK_TITLES: &[&str] = &[
    "fix login",
    "polish ux",
    "refactor sync",
    "fix flaky test",
    "ship it 🚀",
    "triage",
];

// JSON.stringify output of each meta value (insertion order preserved). null
// stays a SQL NULL; every other value is stored as its json text.
const METAS: &[Option<&str>] = &[
    None,
    Some(r#"{"tags":["a","b"],"depth":{"n":1}}"#),
    Some(r#"{"emoji":"✅","list":[1,2.5,-3]}"#),
    Some(r#"{"s":"plain"}"#),
    Some(r#"[1,"two",null]"#),
    Some(r#""scalar string""#),
    Some("42.5"),
    Some("true"),
];

pub fn generate_seed() -> SeedData {
    let mut rng = Rng::new(1);

    let users: Vec<(String, String)> = (0..8)
        .map(|i| {
            let name = format!("{} {}", USER_NAMES[rng.index(USER_NAMES.len())], i);
            (format!("u{i}"), name)
        })
        .collect();

    let projects: Vec<(String, String, String)> = (0..12)
        .map(|i| {
            let name = format!("{} {}", PROJECT_NAMES[rng.index(PROJECT_NAMES.len())], i);
            (format!("p{i}"), format!("u{}", i % users.len()), name)
        })
        .collect();

    let mut members: Vec<(String, String, String)> = Vec::new();
    let mut m = 0usize;
    for p in &projects {
        let count = 1 + rng.index(3);
        for _ in 0..count {
            let user = format!("u{}", rng.index(users.len()));
            members.push((format!("m{m}"), p.0.clone(), user));
            m += 1;
        }
    }

    let tasks: Vec<TaskRow> = (0..48)
        .map(|i| {
            let project_id = format!("p{}", rng.index(10));
            let title = format!("{} {}", TASK_TITLES[rng.index(TASK_TITLES.len())], i);
            // Math.round((next()*20-4)*100)/100, JS Math.round = floor(x+0.5)
            let rank = ((rng.next() * 20.0 - 4.0) * 100.0 + 0.5).floor() / 100.0;
            let done = rng.next() > 0.6;
            let meta = METAS[rng.index(METAS.len())].map(|s| s.to_string());
            // dueAt: the condition draws once; the value draws again only when true
            let due_at = if rng.next() > 0.3 {
                Some(1_750_000_000_000i64 + (rng.next() * 10_000_000_000.0).floor() as i64)
            } else {
                None
            };
            TaskRow {
                id: format!("t{i}"),
                project_id,
                title,
                rank,
                done,
                meta,
                due_at,
            }
        })
        .collect();

    SeedData {
        users,
        projects,
        members,
        tasks,
    }
}

// insert-column layout per table, matching fixture-data.ts seedSqlite: json
// columns store json text, booleans store 0/1, everything else raw.
pub fn seed_rows() -> Vec<(&'static str, &'static [&'static str], Vec<Vec<SqlValue>>)> {
    let data = generate_seed();
    vec![
        (
            "user",
            &["id", "name"][..],
            data.users
                .into_iter()
                .map(|(id, name)| vec![SqlValue::Text(id), SqlValue::Text(name)])
                .collect(),
        ),
        (
            "project",
            &["id", "ownerId", "name"][..],
            data.projects
                .into_iter()
                .map(|(id, owner, name)| {
                    vec![
                        SqlValue::Text(id),
                        SqlValue::Text(owner),
                        SqlValue::Text(name),
                    ]
                })
                .collect(),
        ),
        (
            "member",
            &["id", "projectId", "userId"][..],
            data.members
                .into_iter()
                .map(|(id, project, user)| {
                    vec![
                        SqlValue::Text(id),
                        SqlValue::Text(project),
                        SqlValue::Text(user),
                    ]
                })
                .collect(),
        ),
        (
            "task",
            &["id", "projectId", "title", "rank", "done", "meta", "dueAt"][..],
            data.tasks
                .into_iter()
                .map(|t| {
                    vec![
                        SqlValue::Text(t.id),
                        SqlValue::Text(t.project_id),
                        SqlValue::Text(t.title),
                        SqlValue::Real(t.rank),
                        SqlValue::Integer(if t.done { 1 } else { 0 }),
                        t.meta.map(SqlValue::Text).unwrap_or(SqlValue::Null),
                        t.due_at.map(SqlValue::Integer).unwrap_or(SqlValue::Null),
                    ]
                })
                .collect(),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    // golden captured from the TS harness:
    //   bun -e 'import {SEED} from "./src/fixture-data.ts";
    //           process.stdout.write(JSON.stringify(SEED))'
    // a mismatch here means the differential lanes would diverge on seed data,
    // so this compares the FULL structure (ids, names, float ranks, json meta,
    // i64 dueAt) against the TS output byte-for-byte after json normalization.
    #[test]
    fn seed_matches_typescript_golden() {
        let golden: Value =
            serde_json::from_str(include_str!("../tests/seed-golden.json")).unwrap();
        let d = generate_seed();

        let users: Vec<Value> = d
            .users
            .iter()
            .map(|(id, name)| json!({ "id": id, "name": name }))
            .collect();
        let projects: Vec<Value> = d
            .projects
            .iter()
            .map(|(id, owner, name)| json!({ "id": id, "ownerId": owner, "name": name }))
            .collect();
        let members: Vec<Value> = d
            .members
            .iter()
            .map(|(id, project, user)| {
                json!({ "id": id, "projectId": project, "userId": user })
            })
            .collect();
        let tasks: Vec<Value> = d
            .tasks
            .iter()
            .map(|t| {
                let meta: Value = match &t.meta {
                    Some(text) => serde_json::from_str(text).unwrap(),
                    None => Value::Null,
                };
                let due_at: Value = match t.due_at {
                    Some(v) => json!(v),
                    None => Value::Null,
                };
                json!({
                    "id": t.id,
                    "projectId": t.project_id,
                    "title": t.title,
                    "rank": t.rank,
                    "done": t.done,
                    "meta": meta,
                    "dueAt": due_at,
                })
            })
            .collect();

        let got = json!({
            "user": users,
            "project": projects,
            "member": members,
            "task": tasks,
        });
        assert_eq!(got, golden, "generated seed diverged from the TS golden");
    }
}
