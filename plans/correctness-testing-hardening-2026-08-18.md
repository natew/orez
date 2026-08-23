# Correctness testing hardening, 2026-08-18

Triggered by an outside suggestion to run Antithesis against orez-lite after a
read anomaly was reported in a different system. Two questions: does orez-lite
have that class of anomaly, and is Antithesis the right next investment.

## What orez-lite's read path actually guarantees

Traced end to end on 2026-08-18. Labels follow the repo's evidence convention.

**READ.** Zero clients never read the authority. Application data lives in
`ZeroSqlDO` (`packages/orez-lite/src/cf-do/worker.ts`, `ZeroDO`). The sync
engine is a separate durable object that ingests from `/changes`, which serves
`_zero_changes` rows only (`worker.ts` `readChangesSince`). Uncommitted work
sits in `_zero_pending_changes` and is promoted into `_zero_changes` with its
watermarks allocated at commit, so a client cannot observe a transaction that
has not committed, and cannot observe a watermark hole.

**READ.** A truncated change page cannot lose a change. `/changes` returns the
global head as its `watermark` while capping rows at `limit`
(`worker.ts` `handleChanges`), which reads like the exact mistake
`cursor-pull.ts` warns about, but the consumer never uses that field as a
cursor. `apply_upstream` advances the cursor per applied change and sets
`caught_up: cursor >= batch.watermark`
(`crates/sync-core/src/upstream.rs`), so a truncated page reports not caught up
and the loop refetches from the last applied row.

**READ.** Application writes are exclusive. A write session excludes every
other application-SQL session for its whole life, and a writer is admitted only
when the reader set is empty (`worker.ts` `canAdmitApplicationSqlSession`), so
the eager-write plus before-image undo journal in `tx-journal.ts` cannot be
read half-applied through that lane.

So the reported anomaly class does not reproduce on the client read path. What
clients get is a stale but committed view, which is the asynchronous cache
contract `plans/consistency-validation-architecture.md` already states.

The question that maps onto orez from the original discussion is the durability
one, and it is worth stating plainly rather than treating as novel: a system
that acks a write after a local commit plus a replication catch-up still lets a
reader hitting the primary directly observe a commit that has not replicated.
Postgres behaves the same way under `synchronous_commit`, because the standby
ack gates the ack and not local visibility. Eliminating it means gating
visibility on replication, which costs read latency. orez-lite does not have
this shape at all: clients read a replica, not the authority.

## The defect this investigation did find

**Namespace backups were not consistent snapshots.**
`namespace-backup.ts` `exportNamespace` read one page per statement, and each
statement was its own application-SQL session
(`application-sql.ts`: "One statement is already atomic, so it needs no session
round trips of its own"). Because a writer is admitted the moment the reader
set drains, an ordinary commit landed between two pages, and the dump held a
parent row read before the write next to child rows read after it. Restoring
that dump produces a database that never existed.

Red proof, recorded before the fix: a two-table account/ledger namespace whose
every transaction keeps `sum(ledger.amount) == account.balance` exported with a
deposit committing between the two tables produced `balance 0` against a ledger
summing to `100`.

Fixed by giving the whole scan one read session
(`options.readSession`, wired to `ApplicationSqlClient.readTransaction` in
`lite-data-worker.ts`). Pinned by
`namespace backup export consistency > dumps a state that some transaction
actually produced`, which models the admission lane so it goes red again if the
scan returns to per-statement reads.

Two residuals, stated in `docs/sync/testing.md` item 9 rather than closed:
the durable object's own maintenance writes run outside the admission queue, so
a read session is writer exclusion and not snapshot isolation; and the scan now
blocks application writes for its duration, which a queued writer surfaces as a
30-second admission timeout rather than as a backup failure.

**Superseded 2026-08-23.** The second residual was the whole problem. Writers
were later allowed to preempt the background reader instead of waiting for it
(`c7f7753e`), which moved the cost from the writer to the export: on the
production control plane every attempt for six hours lost its session and no
dump was written. The scan no longer owns one session. It runs in short
sessions and fences them with the `write_seq` marker, which proves the same
thing directly (equal markers on a monotonic counter means no transaction
committed in between) without holding the database across R2. The consistency
test below now models preemption as well as admission.

The other option was to give the export `priority: 'normal'` so it outranks
writers instead of yielding to them. That exists, implemented and working, as
`de253b91` on `fix/backup-export-bounded-progress-m8645`. It is unmerged
**because the owner considered it and chose the restructure**, not because
nobody looked at it: it makes the export hold the busiest object in the system
across its R2 uploads and delays request writes behind the network, which is
the cost background priority was introduced to avoid. Do not merge it and do
not rebase the chunked scan onto it. Full incident writeup lives in soot at
`plans/contrast/incidents/incident-2026-08-23-control-plane-backup-preemption.md`.

## Why no lane caught it

This is the part worth acting on. Three structural reasons, each a gap that
generalizes past this one bug.

1. **The mutation matrix only mutated the Rust engine.** At the time of the
   finding, all nine runnable mutants in `harness/mutants/` were engine mutants.
   The defect was in the TypeScript
   host, which is where most of the invented mechanism lives (the undo journal,
   the admission lane, the change feed, the backup). "Every mutant is caught by
   at least one suite" is therefore a claim about the engine, not the system.
2. **The one backup lane stops the writer first.**
   `harness/src/backup-restore.ts:52` halts writes before backing up, so it
   cannot observe a write racing an export by construction. It also covers the
   sync host's backup, not the R2 namespace dump.
3. **A backup was never treated as an observation.** The consistency checkers
   in `harness/src/consistency/` check client observations and authority reads.
   A dump, a restored namespace, and a snapshot page set are equally
   observations of the database and equally able to hold a state no transaction
   produced, and nothing checked them.

## Antithesis: not yet

Public evidence gathered 2026-08-18, sources in the research notes.

- It requires x86-64 Linux containers under Docker Compose or Kubernetes. The
  SDK is optional for basic runs; custom properties and thread-pausing faults
  need integration.
- No free or open-source tier. The AWS Marketplace listing's public example is
  a 12-month reserved-core contract at $7,000, with real pricing by private
  offer.
- No public precedent for workerd or for direct WASM execution under it. Not
  evidence it fails, but it means we would be first and would pay to find out.
- Turso is the closest precedent and it runs Antithesis **in addition to** its
  own in-repo deterministic simulator, for the OS and I/O interactions the
  simulator cannot see.

orez already has the layer Turso built first: seeded differential tests, a
generated state machine with a composed fault nemesis, Elle on a recorded
history, and a mutation matrix. The layers orez does not have are the ones
Antithesis sells, and also the ones that are cheap to approximate in repo.
Revisit Antithesis when the work below is done and still leaves whole-system
timing and OS faults uncovered.

## Work, highest signal first

1. **Host mutants, first tranche complete 2026-08-19.** The matrix now targets
   both the Rust engine and TypeScript host. Its pull-request host lane applies
   three patches: drop pending-change promotion at commit, admit a writer while
   readers are open, and run a namespace backup outside its read session. All
   three are caught. The backup patch first survived the host suite, which
   proved the production worker wiring had no test; the added test closed that
   gap before the catch was ratcheted into CI. Add further host mutants only at
   a concrete contract boundary with a compatible behavioral lane.
2. **Observation invariants.** Give the harness a declared cross-table
   invariant per fixture (the account/ledger shape above generalizes) and check
   it against every observation the system can produce: a client view, a
   restored backup, a replayed change feed, a completed snapshot generation.
   This is the check that would have caught the backup defect without anyone
   suspecting backups.
3. **Backup and restore under load.** Run the namespace export against a live
   write workload, restore into a fresh namespace, and assert the invariant
   plus convergence. Today's lane stops the writer, so this is a new lane, not
   a parameter.
4. **Network fault injection**, still the named hole in `docs/sync/testing.md`
   item 4. Partition, latency, and drop between client and worker, and between
   the sync object and the data worker. `turmoil` covers the Rust side; the
   workerd side needs a proxy the harness can degrade.
