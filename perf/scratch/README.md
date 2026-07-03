# perf/scratch — 2026-07-03 mutex-contention investigation

Rough probes behind the findings recorded in agentbus
`plans/orez-fundamentals.md` ("Perf investigation findings"). Not wired into
`perf:*` scripts; run directly with `bun perf/scratch/bench-<name>.ts`.

- `bench-trigger.ts` — CDC trigger overhead, direct PGlite (result: noise,
  <0.04ms on a ~0.2ms base at 1KB bodies)
- `bench-replay.ts` — `getChangesSince` scaling vs backlog size (result:
  index-only scan, ~12-15ms/1000 rows, flat 1k→10k; cost is JSONB
  materialization)
- `bench-concurrency.ts` — full proxy under 3 writers + reader + streamer
  (result: 0.76ms solo write → 18ms mean under mix; ~1300/s → 165/s)
- `bench-decomp.ts` — same, adding one contender at a time (result: no single
  contender dominates; any concurrency costs ~1-3.5ms mean, queueing is the
  story)
- `bench-core.ts`, `probe0.ts` — earlier combined probe / import smoke check
