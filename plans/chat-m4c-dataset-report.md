# M4c dataset report: full-authorized-snapshot cost for Chat namespaces

Status: measurements landed; feeds the M4b narrowing decision

Date: 2026-07-09

Owner: opus-m1 (sync-core). Companion to
[rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md) sections
"M4c" and appendix "Chat compatibility branch scale ceiling".

## What this measures and why

M4c's branch serves Chat over the baseline http-pull surface using a FULL
AUTHORIZED SNAPSHOT per namespace: the whole set of rows a user is allowed to
see in a server, filtered server-side by Chat's permission predicates, with any
cross-row permission dependency change forcing an epoch invalidation and a fresh
authorized re-snapshot. That design is correct (no forbidden row ever reaches a
client, no heuristic windows) but potentially expensive, and this report
measures the expense so the M4b narrowing work is decided by numbers rather than
by guess.

The measurement runs through the real engine: Chat's message-read permission is
transcribed to a transformed Zero v51 AST, compiled to SQLite by `sync-core`,
executed against synthesized server data, and each authorized row is serialized
with the same `zero_row` conversion the pull path uses. So the byte and latency
numbers are the engine's actual output, not an estimate. The harness is
`crates/sync-core/tests/chat_snapshot_bench.rs`; regenerate with:

```
cargo test --release -p sync-core --test chat_snapshot_bench -- --ignored --nocapture
```

## Schema and query-shape reconciliation

Chat's schema (`~/chat/src/data/generated/schema.ts`) has 51 tables. The rows
that dominate a namespace's size are messages; everything else (channels,
members, roles, reactions, threads) is bounded by hundreds to low thousands per
server, while messages run into the hundreds of thousands.

The permission predicates (`~/chat/src/data/where/{server,channel,message}.ts`)
and the client query relations (`~/chat/src/features/message/queryMessageItemRelations.ts`)
were reconciled against the engine's supported AST subset:

- Read permissions are cross-table correlated `EXISTS` / `NOT EXISTS`, nested
  several levels deep (message to channel to server to `serverMember`, plus
  `channelUserRole` and `userRole` junctions). The engine compiles these to
  SQL `EXISTS` subqueries with positional binds. Supported.
- `_.and` / `_.or` / `_.cmp` with `=`, `!=`, and the solo-channel `NOT EXISTS`
  branch. Supported.
- `queryMessageItemRelations` uses related-of-related several levels deep
  (message to creator/thread/reactionStats to app/reaction). This was the one
  engine gap; nested related-of-related is now implemented (recursive child
  correlation, commit 75cb376) and tested, so the client query surface is
  fully covered.

No unsupported shape was found in Chat's read path, so no further engine
validation work is required for the Chat surface.

## Measurements

One server namespace, one authorized member, message-read permission applied.
Message bodies are 280 characters (a realistic average), so a serialized
message row is ~376 bytes. `query` is the compiled permission query's execution
time; `serialize` is the `zero_row` JSON conversion of every authorized row.
Release build, in-memory SQLite.

Full authorized snapshot per namespace:

| namespace | channels | members | msgs/channel | total messages | authorized rows | snapshot bytes | engine time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| small | 10 | 20 | 100 | 1,000 | 1,000 | 0.4 MB | 1.9 ms |
| medium | 30 | 100 | 500 | 15,000 | 15,000 | 5.4 MB | 26.7 ms |
| large | 50 | 300 | 2,000 | 100,000 | 100,000 | 35.8 MB | 178.7 ms |
| message-heavy | 80 | 500 | 6,250 | 500,000 | 500,000 | 179.5 MB | 908.6 ms |

Snapshot bytes and engine time scale linearly with the authorized message count
(~376 bytes and ~1.8 microseconds of engine time per message). The non-message
tables add a fixed overhead in the low tens of kilobytes (hundreds of channels,
members, and roles), so the message rows are more than 99% of the snapshot for
every namespace at or above the medium scale.

For contrast, the same run measures the query-aware alternative M4b would ship
instead of the whole namespace: one open channel's most-recent message window
(a `where channelId = ? orderBy id desc limit 100` query, the client's message
list). The window is a fixed ~36 KB and ~1 ms of engine time regardless of
namespace size, because it ships the limit, not the history:

| namespace | full snapshot | windowed query (1 channel, 100 msgs) | snapshot / window bytes |
| --- | ---: | ---: | ---: |
| small | 0.4 MB / 1.9 ms | 36.3 KB / 0.21 ms | 10x |
| medium | 5.4 MB / 26.7 ms | 36.4 KB / 0.47 ms | 151x |
| large | 35.8 MB / 178.7 ms | 36.4 KB / 0.99 ms | 1007x |
| message-heavy | 179.5 MB / 908.6 ms | 36.5 KB / 1.56 ms | 5034x |

## What the numbers say

- **Small and medium namespaces are fine.** A 15,000-message server is a 5.4 MB
  snapshot produced in ~27 ms of engine time. Re-shipping that on a permission
  change is acceptable for controlled short-term use, which is exactly M4c's
  scope.
- **Message-heavy namespaces are not viable under full snapshots.** A
  500,000-message server is a 179.5 MB snapshot. These are engine-only numbers;
  on Cloudflare that 179.5 MB must additionally cross the Durable Object to the
  worker to the client on every cross-row permission change (a member added or
  removed, a role edited, a channel privacy flip), and the engine time is a
  floor that storage I/O and the wasm/JavaScript crossing sit on top of. A
  179.5 MB re-snapshot per permission change is not a workable steady state.
- **The cost is the message table, and it is linear.** There is no knee in the
  curve to exploit; the snapshot grows in lockstep with message history. Any
  approach that ships whole message history per namespace has the same ceiling.

This confirms the prediction in the plan's appendix ("per-user authorized
snapshots on every permission change are unscalable for message-heavy
namespaces"). The M4c branch stays useful for its stated purpose: early Chat
integration feedback and this measurement, on small and medium servers, under
controlled short-term use. It is not the production architecture for
message-heavy servers.

## Implication for M4b narrowing

The M4b query-aware layer already exists in the engine and is the answer to the
ceiling above. Its incremental membership + refcount model ships only the rows
that enter or leave a client's active queries, and it evaluates real `limit` and
`start` cursors, so a message-heavy channel ships only the visible message
window (the client's `queryMessages` limit), not all 500,000 rows. Two things
the numbers make concrete for the narrowing work:

1. **Message queries must be served by the query-aware layer with limits, never
   by a full-namespace snapshot.** The measured windowed query (one channel, 100
   messages) is a fixed ~36 KB while the full snapshot for the message-heavy
   namespace is 179.5 MB, so the query-aware path ships 5,034x fewer bytes for a
   client actively reading one channel, and the gap grows with history because
   the window is constant. The visible window is the client's page size (tens to
   low hundreds of messages), independent of how many messages the namespace
   holds.
2. **The recomputation cost that matters is per-touched-key, not
   per-namespace.** The current engine recomputes all active queries on every
   pull (correct, and fine at the query-aware row volumes); the touched-table
   and touched-key narrowing described in the plan is the optimization to add
   once message queries run through the query-aware layer, so a single new
   message recomputes one channel's windowed query rather than re-scanning
   unrelated queries. The full-snapshot numbers here are the upper bound that
   narrowing must stay far below.

## Cross-track status (M4c chat-side deliverables)

The dataset report above is the M4c deliverable that decides M4b and is complete
and engine-grounded. The remaining M4c deliverables are chat-repo and host
integration work that depend on peers' in-flight pieces, and are not yet done:

- Chat mutators inside the host transaction reuse the DO-local adapter sol is
  building for M4a; that adapter is the shared dependency (do not fork it).
- Control and per-server clients against a local workerd deployment of the
  production host bundle, server switching with distinct namespace and
  local-storage identities, the raw client-store allow/deny tests, the full
  Chat end-to-end suite, and lost-push/replay + fresh-context hydration all sit
  on top of the host bundle and harness that sol (sync-cf-host) and opus-m2
  (harness) own. These proceed once those land; the Chat worktree work happens
  in `~/.worktrees/chat-rust-sync`.

No Chat production deploy and no publish; local workerd and lslcf test infra
only, per the brief.
