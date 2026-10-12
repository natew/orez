# Lite engine production performance pass

Owner direction, 2026-10-12 (Nate, voice):
- "send a whole agent into OREZ let's have them do a big huge optimization
  pass. They can just spend as long as they need just optimizing. They should
  just land things. Piece by piece as they validate them and just keep going."
- "Specifically focused on performance in production and only the rust side"
- "And by rust, I mean just that the light engine, so any piece of that it
  doesn't have to be only rust code, but any piece of the light engine
  performance"

## Scope

- The lite engine, any language: the Rust crates (`crates/sync-core`,
  `crates/sync-wasm`, `crates/sync-native`) and the packages that make up
  orez-lite (`packages/orez-lite`, `sync-cf-host`, `sync-browser-host`,
  `sync-executor`, `orez-sync-native`). on-zero, the CLI, starters and app
  helpers are out of scope.
- Production cost and latency: Durable Object SQL rows read and written per
  pull and push, CPU and wall time per request, wasm memory, and allocation in
  hot paths.

## How work lands

- One measured improvement per commit: name the workload, the before and
  after numbers, and the check that would fail if the change were wrong.
- Land each validated piece on main and keep going; a push publishes a canary.
- Stable releases need Nate.

## Known lead

- Team Machine's Agentbus DO billed 2.4-3.9B rows read a day at about 5.8k
  rows per request (Oct 2026). One cause was a downstream index that made a
  live-sessions query walk all history (team-machine 220c414f4). How the
  engine's pull path turns query evaluation into SQL reads is a lead for this
  pass.

## Log

(the pass appends one line per landed change: sha, workload, before -> after)
