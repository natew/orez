# A failed push should not take reads down with it

Status: proposal, nothing implemented. Written 2026-08-15 from a production
incident on Contrast.

## Recommendation

One mutation failing on the server currently costs the client its entire sync
connection: pulls, the wake channel, and a user-visible reconnect notice. Give
the HTTP transport a way to report a push failure as a push failure, so reads
keep flowing while the mutation queue retries.

Do not reach for the obvious escape hatch. Returning a structured push error
instead of a 5xx makes this worse, for a reason documented under "The trap"
below. Anyone picking this up should read that section before designing
anything.

## What happens today

`packages/orez-lite/src/client/transport.ts` runs a stock Zero client over
stateless HTTP by shimming its connect WebSocket. Every `/push` and `/pull`
response goes through `postJSON`, and every failure lands in `fail()`
(line 1145 as of v0.14.0).

`fail()` has four branches. Auth errors emit `Unauthorized`. A stale client
cookie emits `InvalidConnectionRequestBaseCookie`. A 4xx other than 408, 425,
and 429 emits `InvalidConnectionRequest` or `InvalidPush`. Everything else,
which is every 5xx, every 429, and every network error, emits a
`ServerOverloaded` frame. All four branches then call `this.close(1000, ...)`.

Closing the shim socket also runs `closeWake()`, so the wake channel goes with
it. The stock client reconnects after its backoff and re-pushes the same
pending mutations. Consumers that listen for connection events, such as
on-zero's `ConnectionMonitor`, see the reconnect and surface it. In Contrast
that renders as "Sync was interrupted and is reconnecting now."

So the blast radius of one failed mutation is: reads stop, the wake channel
drops, the user is told sync broke, and the identical push is sent again into
whatever wall it just hit.

## Why it matters, with numbers

Contrast, 2026-08-15, one external user, project `proj_msul2jkd_6e2uj7`.

The app's push endpoint stopped answering inside the host's delegate timeout.
`packages/sync-cf-host/src/host.ts` `#fetchDelegatedPush` burned two 30s
attempts and answered 500 at 60.2s. Cloudflare Workers Logs recorded 18 of
these with wall times clustered between 60,176ms and 61,252ms, and 19
`sync_delegated_push_retry` warns carrying "The operation was aborted due to
timeout".

The app worker's own error telemetry caught 20 of them, one every ~68 seconds,
from 16:30:55 to 17:38:19 UTC. Each closed the client's socket. The first
arrived 9 minutes 24 seconds after the user created the project, which is what
they reported.

The mutation could not succeed, so nothing converged. Reads were collateral
damage the entire time, and the wake channel reconnected on every cycle.

A separate fix (a timed-out delegated push is now terminal, and consumers size
`timeoutMs` under the client's own deadline) shortens each cycle and keeps the
host's answer inside the client's 60s request deadline. It does not change any
of the above. The push still fails, and the failure still costs the connection.

## Adjacent gap: a slow pull does exactly the same thing

Nothing above is push-specific on the client side. `postJSON` applies the same
`REQUEST_HEADER_DEADLINE_MS` (60s) to `/pull`, and a pull failure lands in the
same `fail()`. A pull that crosses 60s closes the socket, drops the wake
channel, and shows the same notice.

The host has no pull timeout at all. `AbortSignal.timeout` appears exactly once
in `packages/sync-cf-host/src/host.ts`, on the delegated push. A pull runs to
completion however long its query recompilation and row scan take, and the only
thing bounding it is the client's own deadline.

How close pulls actually get to that deadline is much less alarming than a
first look at Workers Logs suggests, and the correction matters because the
scary version of these numbers circulated first.

A Durable Object request produces TWO invocation records under one requestId:
the stateless edge worker, and the durable object. **Only the edge record is the
request's latency.** The durable-object record spans the object's activity
window, so while the object is busy with a concurrent push or an ingest round,
its `wallTimeMs` absorbs that work and over-reports every request that overlaps
it. In 201 paired pulls from the incident the edge worker finished a median of
3.7 SECONDS before "its own" durable-object record, which an awaiting caller
cannot do.

Same 201 pulls, both records:

| record                           | p50     | p90     | p99      | max      |
| -------------------------------- | ------- | ------- | -------- | -------- |
| edge worker (request latency)    | 659ms   | 1,446ms | 2,944ms  | 3,368ms  |
| durable object (object lifetime) | 4,364ms | 6,794ms | 43,058ms | 54,044ms |

The four pulls that looked like 43-54s answered in 523-792ms at the edge. They
were concurrent with the 60s delegated pushes, which is exactly why their object
records are long.

So the real headroom is 3.4s against a 60s deadline, not 54s. A slow pull can
still trip the identical teardown and the host still has no pull bound, but this
is a structural gap rather than a live one, and it should not be prioritized as
though pulls were seconds from the cliff.

## The trap

The natural fix is to stop returning 5xx for a push failure and return a
structured push error in a 200 instead. `#push` already has that shape for
application-level failures: `isStructuredPushFailed(delegatedResponse)` returns
`json({ pushResponse: delegatedResponse })`, and the transport forwards it as a
`pushResponse` message rather than an error frame.

That path makes things worse.

Read `@rocicorp/zero@1.8.0`
`out/zero-client/src/client/mutation-tracker.js`:

- `processPushResponse` (line 115) sees `"error" in response` and calls
  `#fatalErrorFromPushError`.
- `#fatalErrorFromPushError` (line 122) maps every case, including
  `"http"` and `"zeroPusher"`, to a `ProtocolError` of kind `PushFailed`.
- Any non-null result is passed to `#onFatalError`, which `zero.js` line 249
  wires to `(error) => this.#disconnect(lc, error)`.
- `out/zero-client/src/client/error.js` line 99 maps `PushFailed` to
  `status: Error`.

So a structured push error disconnects too, and it lands the client in `Error`
status. `ServerOverloaded`, by contrast, maps to `NO_STATUS_TRANSITION`
(error.js line 106) and carries a backoff. Today's behavior is the better of
the two available answers, which is why it is what the transport does.

There is no status code and no response body that lets a stock Zero client
treat a push failure as anything other than a connection event. That is the
actual constraint, and it is upstream of us.

## Option space

None of these are decided. They are the shapes worth costing.

1. **Separate the push channel from the read channel in the shim.** The shim
   presents one socket to the stock client, but push and pull are already two
   independent HTTP calls underneath. A push failure could be held, retried
   against its own backoff, and only escalated to a connection event after it
   has failed enough times or long enough to mean the server is actually gone.
   Reads never notice a mutation that eventually succeeds. The cost is that the
   shim now owns push retry state that the stock run loop currently owns, and
   the two must not both retry.

2. **Escalate on repetition, not on first failure.** Cheapest version of the
   above. Keep `fail()` closing the socket for pulls and for auth, and give
   push failures a small budget of in-shim retries before they close anything.
   A push that recovers on its own second attempt costs the user nothing. A
   genuinely dead app endpoint still ends up in the same place it does today,
   just later.

3. **Report the failure without ending the connection.** Requires an upstream
   protocol affordance in `@rocicorp/zero` that does not exist: a push-level
   error that the mutation tracker treats as retryable rather than fatal. Worth
   raising with Rocicorp, not worth waiting for.

4. **Do nothing here and make pushes cheap enough to never hit this.** The
   incident's push was one of up to `MAX_PUSH_BATCH_MUTATIONS` (64,
   transport.ts line 99) mutations in a single frame, and the same file records
   a measured 44.4s for a 64-mutation frame under load. A batch cap that scales
   with observed server latency would keep individual pushes inside any
   sensible budget. This narrows the window rather than closing it, and it
   trades throughput, so it belongs alongside one of the options above rather
   than instead of them.

## What a solution has to satisfy

- A push that fails once and succeeds on retry must not produce a reconnect
  event, because consumers render those to users.
- Pulls must keep running while a push is failing. Reads are not implicated by
  a bad mutation.
- The wake channel must survive a push failure. Dropping it turns a write
  problem into a latency problem for every other client on the namespace.
- A server that is genuinely gone must still reach the same terminal states it
  reaches today. This is about not overreacting to one failure, and not about
  hiding an outage.
- The shim and the stock run loop must not both be retrying the same mutation.
- Whatever is chosen has to be provable against a stock Zero client. The wire
  contract tests next to `transport.ts` are the bar.

## Related

- `docs/sync/delegation.md` owns the delegation model this sits inside.
- `docs/sync/configuration.md` owns `delegatedPushRetry` and how to size
  `timeoutMs` against the client deadline.
- `plans/orez-write-amplification.md` owns why these push frames are large in
  the first place.
