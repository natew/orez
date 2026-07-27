<!-- plan: status=active owner=orez reviewed= -->

# Sync and application-SQL latency

What Contrast's production trace exposed about Orez's request path, what
changed, and what is still owed. Every number here was measured; the lane that
produced each one is named so it can be re-run.

## The four costs

A Contrast pull paid all four, and they compound.

1. **Admission was a race, not a queue.** Callers polled
   `applicationSqlSession.begin()` every 25 ms against one ownership flag, so
   admission order was decided by whose timer fired first after a release. A 48
   second production trace caught the singleton application DO spending 74,295 ms
   across 27 sessions, six of them 6.4-12.3 s at 0-106 ms of CPU, with later
   sessions completing while sessions that arrived ten seconds earlier were
   still waiting.
2. **Every read took the exclusive write turn.** Sync authentication reads a
   session row from the authoritative application database on every pull. It
   went through the same exclusive session a write does, so reads serialized
   behind each other and behind writers.
3. **One statement cost four round trips.** `query()` opened a session, waited
   for admission, ran, and committed, each a separate RPC into the DO.
4. **Named query resolution was a waterfall.** Both TypeScript hosts called the
   consumer's resolver once per query. A consumer delegates that to its
   application over a service binding, so a screen registering eleven queries
   paid eleven sequential authenticated round trips.

## What changed

### Arrival-order admission with a read lane (55dd991)

`begin()` joins one FIFO queue and resolves when the object grants the turn.
Admission is head-first and never scans past a blocked waiter, so a steady read
load cannot hold the turn away from a queued writer and a steady write load
cannot starve readers.

Sessions declare a lane. Read sessions run alongside each other and refuse
mutating SQL; a write session still excludes everything, which is what the
row-undo journal needs to roll one transaction back without disturbing another.
Because a read session can never escalate, there is no lock upgrade and nothing
to deadlock over: a reader waits only on the writer, and the writer waits only
on the reader set draining.

A read session keeps full isolation. No write session can be admitted while any
read session is open, so a multi-statement read still observes one committed
state for its whole life. That is what makes `readTransaction()` a safe home for
a transaction that reads several rows and needs them to agree.

Cancellation is unchanged in effect and cheaper in cost: a queued session is one
queue entry and one timer, dropped by rollback or by RPC stub disposal. A
canceled session's in-flight `begin()` is deliberately left unsettled rather than
rejected — measured under workerd, rejecting it logs an uncaught error for every
routine cancellation, because the caller that canceled has already stopped
awaiting it.

`query()` now runs end to end inside a single RPC. `exec()` deliberately does
not: a read has no durable effect, so abandoning it is free, while an aborted
write must still not land.

### Patch-free pulls skip the query-pull lock (0291d80)

The per-group lock exists to keep two desired-query patches from applying out of
arrival order when one resolves slowly. It was taken on every query-aware pull,
including the ones carrying no patch — nearly all of them, because a client polls
far more often than it changes what it wants. A patch-free pull applies no query
state and reads membership inside `transactionSync`, which is already atomic
against whichever patch commits around it, so it has nothing to order.

### One resolver call per patch (bf7e0da)

`resolveQuery` became `resolveQueries`, taking the whole patch and returning one
result per request in request order. The native host already had this shape, so
this converges three hosts on one contract instead of adding a second.
`resolveQueryPatch` is shared by both TypeScript hosts because the thing they
must agree on is exactly how many times a consumer's resolver runs for one patch.
It preserves patch order, collapses duplicate `(name, args)` pairs to one request
and fans the answer back to every hash, and attributes a resolver error to the
query it came from.

## Measurements

| what | before | after | lane |
| --- | --- | --- | --- |
| read admission p95, 4 writers / 8 readers / 2 cancellations | 455 ms, strictly serialized | 209 ms, 3 reads concurrent | `sync-cf-host` platform probe |
| arrival-order inversions under that load | present by construction | 0 | same |
| warm pull while another pull in its group is 750 ms inside query resolution | 738 ms | 5 ms | `sync-cf-host` integration |
| four-query desired patch, resolver sleeping once per call | 1,209 ms | 314 ms | same |
| cold pull for one registered query, same fixture and client | 11,275 B / 90 patches (`orez-local`) | 806 B / 5 patches (`rust-local`) | `harness/src/pull-payload.ts` |

The last row is not a before/after of a change; it is the standing difference
between Orez's two pull implementations. The zero-http mount serves every mounted
table the user can see and treats the desired-query patch as an ack to echo,
so its payload grows with everything the user can see. The Rust engine resolves
the client's queries and sends their membership, so its payload grows with what
was asked for. The warm `unchanged` pull is 29 bytes on both: the gap is entirely
the cache miss.

## Negative controls

Each timing claim above has a run that fails without the change, not an argument
that it should:

- Warm pull behind query resolution: `AssertionError: patch-free pull waited
  738 ms behind another pull's query resolution` on the parent commit.
- Batched resolution: `AssertionError: four-query patch took 1209 ms` on the
  parent commit, which is 4 x the resolver's single 300 ms sleep.
- Read concurrency: the same mixed load re-run with `readLane=0` reports
  `maxConcurrentReads` exactly 1, so the concurrency counter is measuring
  admission rather than something that would read high either way.
- Admission leak: the probe reads the DO's own writer/reader/queue state after
  the load and requires it empty, so a fair queue that stranded waiters would
  fail rather than look fair.

## Still owed

**Consumers must opt into the read lane.** `readTransaction()` exists but
nothing downstream calls it yet. A consumer that routes every read through
`transaction()` gets fairness from 55dd991 but not concurrency: on the benchmark
that is the 455 ms to 209 ms difference. This is the largest remaining lever on
sync-auth latency and it is a call-site change, not an Orez change.

**Two production pull paths still exist.** Web reaches the Rust query-aware host;
native mobile reaches the zero-http mount, and the mount is what serves the
all-visible-row payload. The Rust host's namespace router already accepts the
native URL shapes, its authenticator already forwards bearer tokens, and push is
already delegated to the same application route, so the reuse is client
configuration rather than protocol. A client that moves must rotate its local
store key in the same change: the two hosts' cookies are different watermark
domains, and a numerically plausible cookie from the wrong one diffs from a
baseline that never existed.

**The batch resolver is a breaking config change.** `resolveQuery` is gone; a
consumer must supply `resolveQueries`. See the migration note below.

**Release is not done here.** Nothing in this work has been published. Exercise
it downstream with `bun release --into <consumer>`, which builds a local tarball
and unpacks it into the consumer's `node_modules` without touching npm or git.

## Migrating a consumer

```ts
// before
resolveQuery: async (name, args, claims, env) => (await transform(name, args, claims, env)).ast

// after — one call for the whole patch, results in request order
resolveQueries: async (requests, claims, env) => {
  const results = await transformAll(requests, claims, env) // one round trip
  return results.map((result) =>
    result.ast ? { ast: result.ast } : { error: result.error ?? 'unknown query' }
  )
}
```

Requirements the host enforces: exactly one entry per request, in request order.
An `{ error }` entry fails only its own query's pull and names that query, so one
unknown query among ten is attributable. Duplicate `(name, args)` pairs are
already collapsed before the resolver sees them, so a consumer never has to
dedupe.

For a read-only transaction that must keep cross-statement atomicity, swap the
method and change nothing else:

```ts
// same statements, same isolation, shared admission
await client.readTransaction(compileQuery, async (tx) => {
  const a = await tx.query('SELECT ...')
  const b = await tx.query('SELECT ...')
  return { a, b }
})
```

A mutating statement in a read session is rejected rather than escalated, which
is what lets the other readers in flight rely on the state they were admitted
against.
