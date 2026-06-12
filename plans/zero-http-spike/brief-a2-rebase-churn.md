# brief A2 — rebase/rollback + churn integration tests (segment 2)

you are agent A continuing the zero-http spike. segment 1 is reviewed and
merged. integration is proven: `src/zero-http/integration.test.ts` runs a
stock zero client end-to-end against the real fixture server through
`src/zero-http/test-harness.ts` (read both first — use the harness, do not
re-roll setup).

## file ownership (segment 2)

yours: `src/zero-http/rebase.test.ts`, `src/zero-http/churn.test.ts`,
`src/zero-http/transport.ts` (your file), and you MAY extend
`src/zero-http/test-harness.ts` additively (new optional fields only — do
not change existing signatures; agent B consumes it in parallel).
do NOT touch: `fixture-schema.ts`, `server.ts` (agent B is editing those),
`relations.test.ts`, `auth.test.ts`, `server.test.ts`,
`integration.test.ts`, `transport.test.ts` (frozen unless a real bug needs
a regression test).

## tasks

1. **cleanup (your own file).** `transport.ts` `answerMutationRecoveryPull`
   has a dead ternary — both branches of `response.unchanged ? … : …` are
   identical. collapse it.

2. **interleave control.** to choreograph ack-then-pull vs pull-then-ack
   you need to gate HTTP responses. add an optional `interceptFetch` (or
   similar) option to `startZeroHttpHarness` that wraps the fetch handed to
   `installZeroHttpTransport` — additive, default unchanged.

3. **`rebase.test.ts` — plan obligation 2, THE correctness question.**
   - **ack-then-pull**: mutation pushed, pushResponse lands, then a pull.
     attach a view listener BEFORE mutating and record every emission: the
     optimistic row must never disappear or flicker through any
     intermediate emission; final state equals server state.
   - **pull-then-ack**: hold the /push response open (gate) while a pull
     (triggered via `harness.transport.pull()`) completes with a snapshot
     that does NOT yet contain the mutation; the optimistic row must
     survive the rebase (it is replayed on top of the new snapshot); then
     release the push, pull again, converge.
   - **rollback**: `project|rename` against an id that does not exist on
     the server (but create it client-side optimistically first? NO — the
     clean case: rename a project that exists in the client snapshot but
     was made un-renameable server-side, e.g. owned by another user via
     seed, or simply rename a nonexistent id where the client mutator's
     optimistic update is a no-op…). pick the variant where the OPTIMISTIC
     state visibly diverges: seed project p1 owned by u2 but visible to u1
     (member), have u1 `project|rename` it — the client mutator applies the
     rename optimistically, the server answers `forbidden` app error, LMID
     advances, and after the next pull the name reverts. assert: optimistic
     name visible pre-push, error surfaced on `mutation.server`, name
     reverted post-pull, server rows unchanged.

4. **`churn.test.ts` — plan obligation 4.**
   - 15 sequential-burst mutations (queue them without awaiting
     individually) while pulls fire on a short timer; assert: all 15 rows
     present client + server, final lmid for the client is 15 (read it from
     a pull response or `server`-side bookkeeping via a raw fetch), views
     converge, no transport error.
   - stretch (attempt, report if fiddly, do not sink >30min): two zero
     instances in the SAME client group (same `storageKey` + userID) each
     getting its own fake socket — both converge, no cookie fights.

5. **latency investigation.** the e2e smoke takes ~3s for one mutation
   round-trip. find what waits (instrument locally, do not commit debug
   logs). if it is transport-induced (e.g. a missed immediate pull trigger
   after pushResponse, or a poke emitted before `connected`), fix it in
   transport.ts with a regression test; if it is zero-client-internal
   pacing, just report the mechanism precisely.

## acceptance

- `npx vitest run src/zero-http/` fully green (including the other agent's
  files once they land — rerun at the end), `npx tsc --noEmit` clean.
- new tests demonstrably fail without the machinery they test (state how
  you checked — e.g. rollback test against a server that doesn't advance
  LMID on error).
- conventional commits, explicit pathspec, your files only. never push.
- final report: per-obligation verdict (pass/fail/surprise), the latency
  mechanism, and anything segment 3 must know.
