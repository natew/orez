# Query oracle red proof

Recorded 2026-07-16 for `crates/sync-core/tests/differential.rs`. Both mutants
were applied one at a time to the Rust query compiler, exercised with the
hermetic Bun oracle, allowed to shrink, and then reverted. The command was:

```sh
PROPTEST_CASES=1 PROPTEST_MAX_SHRINK_ITERS=1000 \
  cargo test -p sync-core --test differential \
  rust_matches_the_ts_reference_core_on_generated_traces -- --exact
```

## Mutant 1: ignore all but the first AND branch

Temporary diff, reverted after the red run:

```diff
diff --git a/crates/sync-core/src/query/compile.rs b/crates/sync-core/src/query/compile.rs
@@ -304,7 +304,9 @@ impl<'a> Compiler<'a> {
-            Condition::And(conds) => self.compile_junction(conds, "AND", "1", table, alias),
+            Condition::And(conds) => {
+                self.compile_junction(&conds[..conds.len().min(1)], "AND", "1", table, alias)
+            }
```

The generated differential failed on pull 6. Rust leaked `project:p3`
(`ownerId=u0`, `name=outside`) after dropping the second branch. The TypeScript
oracle returned only `project:p0` and `project:p2`.

Proptest persistence hash: `f0ab1d78ddc8703e0db015779442738d7510e6d7fa82142ebf68ebd4cb397e1d`.

## Mutant 2: reverse explicit orderBy directions

Temporary diff, reverted after the red run:

```diff
diff --git a/crates/sync-core/src/query/compile.rs b/crates/sync-core/src/query/compile.rs
@@ -183,7 +183,7 @@ impl<'a> Compiler<'a> {
-                if *desc { "DESC" } else { "ASC" }
+                if *desc { "ASC" } else { "DESC" }
```

The generated differential failed on pull 7. For `rank DESC LIMIT 2`, Rust
returned `task:t0,t1`; the TypeScript oracle returned `task:t2,t3`.

Proptest persistence hash: `10521d8f79f5c75f133f890b9da1c27cb74d41319eff1da75532b80c360de7f3`.

## Shrunk failing trace

Both mutants shrank to the same 11-operation trace. The first seven operations
are the existing baseline lane's mandatory convergence suffix. The two named
query puts and two query pulls are the query lane's minimal retained proof.

```json
[
  {
    "op": "put",
    "client": "c1",
    "item": "k0",
    "label": "l0",
    "rank": 0.0,
    "done": false,
    "meta": null
  },
  { "op": "pull", "client": "c1" },
  { "op": "pull", "client": "c1" },
  { "op": "pull", "client": "c2" },
  { "op": "pull", "client": "c2" },
  { "op": "pull", "client": "c3" },
  { "op": "pull", "client": "c3" },
  {
    "op": "queryput",
    "hash": "and_or",
    "transform_version": 0,
    "ast": {
      "table": "project",
      "where": {
        "type": "and",
        "conditions": [
          {
            "type": "simple",
            "op": "=",
            "left": { "type": "column", "name": "ownerId" },
            "right": { "type": "literal", "value": "u0" }
          },
          {
            "type": "or",
            "conditions": [
              {
                "type": "simple",
                "op": "=",
                "left": { "type": "column", "name": "name" },
                "right": { "type": "literal", "value": "A" }
              },
              {
                "type": "simple",
                "op": "=",
                "left": { "type": "column", "name": "name" },
                "right": { "type": "literal", "value": "C" }
              }
            ]
          }
        ]
      },
      "orderBy": [["name", "asc"]]
    }
  },
  {
    "op": "queryput",
    "hash": "top_tasks",
    "transform_version": 0,
    "ast": {
      "table": "task",
      "orderBy": [["rank", "desc"]],
      "limit": 2
    }
  },
  { "op": "querypull" },
  { "op": "querypull" }
]
```

The failure seeds were created by deliberate mutants, so they are not committed
under `crates/sync-core/proptest-regressions/`. Replaying them against the
correct compiler is green. No genuine product failure was found while building
the oracle.

Both saved minimized envelopes passed through
`replay_saved_differential_trace` after the compiler diffs were reverted, and
the full differential test returned 2 passed, 1 ignored.
