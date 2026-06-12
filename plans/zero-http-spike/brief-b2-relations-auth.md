# brief B2 — relations + auth-parity e2e tests (segment 2)

you are agent B continuing the zero-http spike. segment 1 is reviewed and
merged. integration is proven: `src/zero-http/integration.test.ts` runs a
stock zero client end-to-end against your fixture server through
`src/zero-http/test-harness.ts` (read both first — use the harness, do not
re-roll setup). INTERFACE.md gained a `member|remove` mutator pinned for
you — re-read the mutators section.

## file ownership (segment 2)

yours: `src/zero-http/relations.test.ts`, `src/zero-http/auth.test.ts`,
`src/zero-http/server.ts` + `src/zero-http/server.test.ts` (your files),
and `src/zero-http/fixture-schema.ts` (transferred to you for this segment
— add the `member|remove` client mutator, change nothing else in it).
do NOT touch: `transport.ts`, `test-harness.ts`, `transport.test.ts`,
`rebase.test.ts`, `churn.test.ts`, `integration.test.ts` (agent A is
working in parallel; harness extensions may land mid-flight — `git pull` is
not needed, you share the worktree; just don't edit those files).

## tasks

1. **`member|remove`** per INTERFACE.md: server-side handler in `server.ts`
   (+ a server.test.ts case for not-found/forbidden/success), client-side
   mutator in `fixture-schema.ts` (`tx.mutate.member.delete({ id })`).

2. **`relations.test.ts` — plan obligation 3.** all through real zero
   clients on the harness:
   - **cross-user appearance**: u1's `.related('members')` view is
     complete; u2 creates a project and `member|add`s u1; after u1 pulls
     (`harness.transport.pull()` pulls for all live sockets), u1's view
     shows the new project WITH its member rows.
   - **row replacement**: u2 renames that project; u1 pulls; u1's view
     updates the name in place (same row identity, members intact).
   - **visibility revocation (the soot access-denied analog)**: u2
     `member|remove`s u1; u1 pulls; the project and its member rows vanish
     from u1's view entirely. assert the view emission after the pull no
     longer contains them and u1's next pull is `unchanged` (no thrash).

3. **`auth.test.ts` — plan obligation 5 e2e.**
   - two harness clients u1/u2 with seeded disjoint + shared rows: each
     client's completed snapshot contains exactly its pinned visibility
     set; assert NO row of the other user's private data ever appears in
     any view emission (listener capture, not just final state).
   - unknown token: create a zero client for a userID with no `user` row —
     the pull 401s; assert the client never receives a poke, surfaces no
     data, and the transport does not crash the suite (the fetch error is
     contained). if the current transport behavior on 401 is an unhandled
     rejection that kills vitest, report it as a finding for agent A —
     do not patch transport.ts yourself; assert current containable
     behavior or skip-with-comment ONLY for that one assertion and flag it
     loudly in your report.

## acceptance

- `npx vitest run src/zero-http/` fully green at the end (rerun once agent
  A's files land too), `npx tsc --noEmit` clean.
- new tests demonstrably fail without the machinery (e.g. revocation test
  red before `member|remove` lands — state how you checked).
- conventional commits, explicit pathspec, your files only. never push.
- final report: per-obligation verdict, any interface friction, anything
  segment 3 must know.
