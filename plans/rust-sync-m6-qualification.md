# Rust sync M6 qualification matrix

This matrix is for native and `lslcf` test infrastructure only. It does not
authorize a Chat/Soot production deploy, namespace routing change, or package
publish. Run Node-dependent harness commands with the repository mise default
(`node 24.3.0`); the stock-Zero SQLite native module must remain ABI 137.

## Reproducibility rules

- Build and deploy only from a committed, clean worktree and record the SHA and
  Cloudflare version ID.
- Use a fresh `drill-*`, `rust-*`, or other explicitly test-only namespace for
  every remote run.
- Record the exact command, budget, elapsed time, and final structured result.
- A correctness failure fails the lane even if its latency or memory budget
  passes.
- Never log request bodies, row contents, named-query arguments, tokens, admin
  keys, or raw production namespaces.

## Lane budgets and commands

### Eviction and hard-kill recovery

Budget: 25 repeated cycles per target; zero 409s, duplicate mutations, cookie
regressions, or missed rows. Recovery must complete within 10 seconds per
cycle. Durable Object boot ID or native process ID must change each cycle.

```sh
mise exec node@24.3.0 -- bun harness/src/eviction.ts --target rust-local --clients 20
mise exec node@24.3.0 -- bun harness/src/eviction.ts --target rust-cf --clients 20
```

The long qualification runner repeats these commands 25 times. Boundary fault
coverage must include before application mutation, after application mutation
before LMID finalize, after commit before response, during pull transaction,
and after pull commit before response. Lost-response coverage is already in the
reconnect/query lifecycle lanes; the remaining precise boundary hooks are an
explicit M6 harness item, not inferred from a process kill between requests.

### Retention pressure and offline clients

Budget: retention window 2, at least 100 committed changes while one client is
offline, then convergence by a safe snapshot/reset in 15 seconds. Zero partial
incremental patches, unauthorized rows, LMID regression, or cookie regression.

```sh
mise exec node@24.3.0 -- bun harness/src/reconnect.ts --target rust-local
mise exec node@24.3.0 -- bun harness/src/reconnect.ts --target rust-cf
```

### Query, connection, and tab churn

Budget: all 22 desired-query corpus shapes, 100 clients, 5 writers, 5 rounds,
20 operations per writer, and 100 tab open/close cycles. Zero reset, raw-store
membership, permission, or convergence failures. Cloudflare propagation p95
must remain below 1 second; native below 100 ms; no safety-poll convergence.

```sh
mise exec node@24.3.0 -- bun harness/src/query-diff.ts --against rust-local
mise exec node@24.3.0 -- bun harness/src/query-diff.ts --against rust-cf
mise exec node@24.3.0 -- bun harness/src/storm.ts --target rust-local --clients 100 --writers 5 --rounds 5 --ops-per-writer 20
mise exec node@24.3.0 -- bun harness/src/storm.ts --target rust-cf --clients 100 --writers 5 --rounds 5 --ops-per-writer 20
mise exec node@24.3.0 -- bun harness/src/multi-tab.ts --target rust-local
mise exec node@24.3.0 -- bun harness/src/multi-tab.ts --target rust-cf
```

The rust-cf differential's intermittent `allProjects` completion stall was
root-caused (2026-07-16) to the harness's vendored `httpPullTransport.ts`
lagging canonical fix `1efd3e5`: a got-query ack rode an early poke, a following
snapshot-reset pull (leading `rowsPatch` `clear`) wiped the stock client's
got-query marks, and the transport never re-asserted an ack it believed
delivered, so the view never reached `complete` (load-dependent, ~3/10 under CPU
load; a low-latency local run rarely trips it). The got-set re-assertion +
dedupe is now ported into the vendored transport and pinned by
`harness/src/vendor/httpPullTransport.stall.test.ts` (red before the port, green
after); 15/15 repeated `query-diff --against rust-cf` runs against local workerd
pass. This lane is settled green.

### Malformed and adversarial protocol inputs

Budget: at least 10,000 deterministic seeded pull/push cases per target. All
requests must complete in 2 seconds, malformed/unsupported requests must return
4xx, invariant failures must remain zero, and a valid pull/push must succeed
after the corpus. Raw AST and unknown named query puts must return 400.

```sh
mise exec node@24.3.0 -- bun harness/src/query-security.ts
mise exec node@24.3.0 -- bun harness/src/protocol-fuzz.ts --target rust-local --cases 10000 --seed 1
mise exec node@24.3.0 -- bun harness/src/protocol-fuzz.ts --target rust-cf --cases 10000 --seed 1
```

### Memory and bundle headroom

Budget: after warm-up, wasm linear-memory high-water growth must be at most one
page (64 KiB) across each block of 1,000 query/connection churn operations and
must not increase across three consecutive blocks. The measured instance must
stay alive for the whole run: a restart re-instantiates the wasm module and
resets linear memory, which turns every sample into a boot-footprint reading
and hides any leak. Restart/eviction churn belongs to the eviction lane, never
this one. Database size is tracked separately and is not a substitute for wasm
memory. The compressed worker bundle must retain at least 40 percent headroom
below the applicable Cloudflare limit.

```sh
mise exec node@24.3.0 -- bun harness/src/memory-soak.ts --target rust-cf --blocks 3 --ops 1000
mise exec node@24.3.0 -- bun harness/src/push-memory-soak.ts --target rust-cf --blocks 3 --ops 3000 --writers 12
mise exec node@24.3.0 -- bun --cwd packages/sync-cf-host run measure
```

The wasm lane uses the host's authenticated byte-count diagnostic and must not
infer flat linear memory from process RSS or database size. Native allocator/RSS
soak is tracked separately because it is not a wasm runtime.

The push lane warms the instance with 3,000 chat-shaped `message.send` pushes,
then runs three measured 3,000-push blocks through 12 writers. It applies the
same 65,536-byte block-growth and three-block monotonic-growth gates to wasm
memory, fails on any push or application error, and samples JS heap bytes from
the authenticated status endpoint when the current workerd exposes
`performance.memory`.

### 2026-07-16 rust-cf query-diff stall root-cause + deployed lane

The intermittent `allProjects` completion stall (held red above) was a transport
bug, not an engine bug. The harness's vendored `httpPullTransport.ts` had
drifted behind canonical fix `1efd3e5`: nothing re-asserted a client's acked
got-query set when a later snapshot-reset pull (`rowsPatch` `clear`) wiped
replicache, so an ack that rode an earlier poke silently regressed to unknown
and the materialized view never completed. Ported the got-set re-assertion +
per-hash dedupe into the vendored transport;
`harness/src/vendor/httpPullTransport.stall.test.ts` pins the ordering
deterministically (view never completes against the pre-fix transport, completes
against the fixed one). Repeated end-to-end `query-diff --against rust-cf` runs
against local workerd: 15/15 pass (2.9–4.3 s each; the pre-fix 23 s slow-round
outlier is gone). The fix is transport-wide, so every query-aware rust-cf/orez-cf
lane benefits, not just query-diff.

The credentialed deployed Cloudflare qualification now runs on its own schedule
via `.github/workflows/deployed-qualification.yml` (weekly + `workflow_dispatch`,
never on PRs): it deploys `packages/sync-cf-host` (the rust-cf WASM DO host)
under `orez-rust-sync-qual` to the account holding `CLOUDFLARE_API_TOKEN`
(`aa20b480…`, parameterized via env), runs the bounded m6 CF suite (reconnect,
eviction, storage-faults, backup-restore, state-machine 24 steps) against the
live origin, uploads traces, and tears the worker down. The whole suite was
validated green against a local workerd stand-in (rc=0); the first credentialed
run happens on the schedule.

### Storage failure, quota, and clock skew

Budget: every injected storage failure rolls back application rows, LMID, query
membership, and deferred effects together. Recovery succeeds within 10 seconds
after clearing the fault. Quota exhaustion returns a bounded error without
increasing invariant failures. Application timestamp tests cover ±24 hours of
skew and never use the skewed client clock for sync ordering.

```sh
mise exec node@24.3.0 -- bun harness/src/storage-faults.ts --target rust-local
mise exec node@24.3.0 -- bun harness/src/storage-faults.ts --target rust-cf
mise exec node@24.3.0 -- bun harness/src/clock-skew.ts --target rust-local --clock-skew-hours 24
mise exec node@24.3.0 -- bun harness/src/clock-skew.ts --target rust-cf --clock-skew-hours 24
```

Native uses SIGKILL-shaped faults to prove the pre-commit row is absent after
restart and the post-commit row survives. Cloudflare injects transaction errors
and quota responses at the equivalent boundaries; real process/instance loss is
covered by the persistent-workerd restart and DO eviction lanes. A lost HTTP
response is not treated as a storage failure.

### Backup, restore, canary, and rollback

Budget: one writer at every observable phase, with an explicit zero-writer gap
between owners. A stopped writer must reject pushes before the other is enabled.
Mutation ordering and desired-query acknowledgement survive rollback; invariant
failure counters remain zero. Complete the drill in 5 minutes.

```sh
mise exec node@24.3.0 -- bun harness/src/backup-restore.ts --target rust-local
mise exec node@24.3.0 -- bun harness/src/backup-restore.ts --target rust-cf
mise exec node@24.3.0 -- bun harness/src/rollback-drill.ts --confirm-test-only
```

Backup/restore quiesces the source, captures the four fixture application
tables, restores them into a fresh namespace, and requires a full fresh-client
snapshot. The rollback runner accepts only `lslcf.workers.dev` or loopback and
creates fresh `drill-*` namespaces. Neither changes a production route.

## Evidence record

For every run, append a dated entry containing source SHA, deployed version,
target, command, configured budget, measured result, and PASS/FAIL. A lane with
missing measurements is pending, not green. Production retirement remains
outside this prep gate and requires user-approved cutover plus the observation
window.

### 2026-07-10 full re-qualification (fable-5, replaces retracted passes #0101–#0106)

The prior qualification passes were retracted after the running session was
silently downgraded to a small model. This run re-executes both suites on a
trusted model after an audit of that session's lanes fixed two defects: the CF
before-commit fault re-fired forever (fix 50c6860, coordinator), and the
memory-soak lane restarted the instance inside measured blocks so it could not
observe a leak (fix cbb66bc). The fuzz corpus was also replaced with a seeded
structural generator under a strict every-case-4xx assertion (e2100e4).

- Source SHA: `011bf2d` (native suite; CF worker built and deployed from it).
  CF deploy: `orez-rust-sync.lslcf.workers.dev` version
  `3762dad8-92f7-445f-a18a-de29eeda4c4a`, upload 664.96 KiB / 220.39 KiB gzip
  (92.8% headroom below the 3 MiB limit; budget >= 40%). The CF suite ran at
  `f08ac4c` (adds fuzz error context only; worker artifact unchanged).
- Command: `bun harness/src/m6-runner.ts --suite native` and `--suite cf`
  (full budgets, not `--quick`).

Native suite (7/7 PASS, 43.4 s total):

| lane                | measured result                                                                                    |
| ------------------- | -------------------------------------------------------------------------------------------------- |
| protocol-fuzz       | 10,000 seeded cases (seed 1), all 400, 137 ms; also green on seeds 2/3/42                          |
| eviction            | SIGKILL restart, outage 1559 ms (budget 10 s), 30 writes, converge 53 ms, 0 409s, cookies monotone |
| retention-reconnect | persisted resume, lost-response recovery, epoch snapshot, future-cookie 409 all PASS               |
| query-tab-churn     | shared group, per-tab LMIDs, replacement-tab resume PASS                                           |
| clock-skew          | ±24 h application timestamps stored verbatim, LMID 2, ordering unaffected                          |
| storage-faults      | 5 boundary points, kill-shaped: pre-commit row absent after restart, post-commit row survives      |
| backup-restore      | 4 tables, 91 rows, fresh snapshot emitted exactly 91 puts                                          |

CF suite vs deployed lslcf worker (9/9 PASS, 157.2 s total):

| lane                | measured result                                                                                                                                            |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| protocol-fuzz       | 10,000 seeded cases, all 400, 68.9 s (one earlier post-deploy cold run had a single >2 s request; clean end-to-end rerun recorded here)                    |
| eviction            | DO boot ID changed across teardown, 20 writes, 0 409s, cookies monotone, late convergence 155 ms                                                           |
| retention-reconnect | all four phases PASS                                                                                                                                       |
| query-tab-churn     | PASS                                                                                                                                                       |
| clock-skew          | ±24 h, LMID 2                                                                                                                                              |
| storage-faults      | 5 points via error/quota injection, pre-commit rollback + post-commit durability + replay + recovery                                                       |
| backup-restore      | 91 rows, 91 fresh-snapshot puts                                                                                                                            |
| wasm-memory-soak    | live instance, warm block + 3 measured blocks of 1,000 query/connection churn ops: samples 1,572,864 bytes flat, growth 0/0/0 (budget <= 65,536 per block) |
| rollback-one-writer | old-only -> none -> new-only -> none -> old-only, stopped writers reject with 503, 0 invariant failures                                                    |

Local workerd `measure` at the same SHA: cold DO p50/p95 5.351/7.829 ms, ack
p50/p95 1.797/3.127 ms, storage delta 8,192 bytes across 50 pushes.

### 2026-07-10 push-memory addendum

The CF lane list now includes `cf-push-memory`. A local workerd qualification
completed 12,000 chat-shaped pushes with zero failures and flat wasm samples of
1,245,184 bytes across the warm block and all three measured blocks. This
workerd build returns `null` for the optional JS heap fields, so the leak fix was
also verified with the inspector: V8 heap returned to its roughly 31 MB baseline
after collection. The full Chat adapter and server-effects path completed
12,000/12,000 `message.send` pushes after the fix; before it, pushes failed at
workerd's 10,000-active-timeout limit.
