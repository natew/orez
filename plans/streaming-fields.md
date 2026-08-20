# Streaming fields for Orez Lite

Status: built and shipped for in-process producers; off-host producers have a
protocol and a transport but no host adapter yet.

## What exists (2026-07-27)

Everything below is in `packages/orez-lite/src/realtime/`, covered by 124 tests.

- `manifest.ts` declares which columns may stream, validated against the real
  Zero schema. `append` is inferred for string columns, `replace` otherwise, and
  the mode is fixed at manifest time so no host branches on frame shape.
- `hub.ts` is the host state machine, with no I/O. It ACCUMULATES each
  generation's value, which is what makes append frames safe for a late
  subscriber: it gets one snapshot then deltas, and no client needs a replay
  buffer.
- `store.ts` is the client overlay, with the `durable | streaming | committing |
stale` handoff.
- `publisher.ts` / `writer.ts` are the producer API. `writer.ts` is the one soot
  uses: a synchronous `set(handle, value)` with no session to track.
- `local.ts` wires all of it in one process. This is what an app whose producer
  runs in the browser needs, and it is all soot needed.
- `producer-socket.ts` + `host.ts` are the off-host producer path: an
  application server generates values while the subscribers are browsers
  elsewhere.

Consumed by soot as of `soot@0bfa3274`, against `orez-lite@0.10.7-canary.1785181620131`.
`orez/realtime` re-exports the whole module for apps on the full package.

## What is NOT built

- **No Cloudflare DO host adapter.** `realtime/host.ts` routes frames and
  `realtime/hub.ts` holds the state, but nothing wires them into a Durable
  Object. The design below is settled; only the implementation is missing.
- **No Rust producer client.** team-machine writes `session.latest_summary_body`
  from `src/pg_writer.rs`, so its producer is a Rust process and cannot use
  `producer-socket.ts`. It needs either a small Rust client for the begin/publish
  frames, or a TypeScript relay point that already sees the summary chunks.

## The DO adapter, as designed

It goes in `packages/sync-cf-host/src/host.ts`, NOT in `cf-do/worker.ts`. That
package already has the factory an application configures, so the manifest needs
no new mechanism:

- **Manifest injection**: add `streamingManifest?: StreamingManifest` to
  `SyncHostConfig` (`sync-cf-host/src/types.ts:84`). `createSyncDurableObject`
  (`host.ts:386`) already closes over `config` to build `compileQuery` and the
  caps, so the hub is constructed the same way. This sidesteps serialization
  entirely: `validate` stays a live function in the application's own bundle,
  which is why a serialized-into-the-deploy-bundle approach was rejected.
- **Frames** ride the existing wake socket. `webSocketMessage` (`host.ts:1972`)
  currently answers only `'ping'`; `subscribe`/`unsubscribe` route through
  `applyClientFrame`, and the producer frames through `applyProducerFrame`.
  Realtime frames are `[type, body]` tuples, the same shape Zero's protocol
  already uses on that socket.
- **Lifecycle**: `webSocketClose` (`host.ts:1977`) and `webSocketError`
  (`host.ts:1991`) call `hub.dropConnection` / `hub.dropProducer`.

Two things that will bite whoever implements it:

- **Hibernation.** A hibernatable socket outlives the DO's memory, so the hub is
  empty after a wake while sockets are still open. Subscriptions must live in the
  socket attachment (`host.ts:107`, today just `{ clientID: string }`) and the hub
  must rehydrate from `ctx.getWebSockets()` before serving any producer frame,
  or a client that subscribed before hibernation silently receives nothing.
  Replaying `hub.subscribe` per stored topic is the right rehydration, because it
  re-authorizes as a side effect. Generations need no such treatment: they are
  ephemeral by definition and the durable row is still truth.
- **Identity.** `RealtimeIdentity` needs `userID` + `clientGroupID`, and the wake
  socket's attachment carries only `clientID`. `#wake` (`host.ts:1712`) takes it
  from a query parameter and authenticates nothing. A subscription is an
  authorization decision (it grants a live feed of a row the caller may not be
  allowed to read), so the upgrade has to run `config.authenticate` and persist
  the resulting claims in the attachment. Do not let the client assert its own
  userID here.

## Original proposal

## Decision

Add a typed, ephemeral field overlay beside Zero. Reuse one Orez realtime
WebSocket per Zero client connection, require an explicit field subscription,
and keep the ordinary Zero row as durable truth.

During a stream, Orez broadcasts field values without writing the application
row or advancing the Zero cookie. The application commits the final value
through its existing mutation or server write path. The client keeps displaying
the overlay until the normal Zero query produces that final value.

Declare streamable fields in a parallel manifest derived from the stock Zero
schema. Do not add `streaming` metadata to Zero's schema objects or extend the
Zero wire protocol.

This gives Chat, Soot, and future consumers one implementation for:

- socket lifecycle and reconnection;
- topic subscription and fan-out;
- local, native, browser-worker, and Cloudflare hosts;
- stream generations, ordering, size limits, and cleanup;
- query-membership authorization;
- React state and final-value handoff.

The application still owns the operation that produces values and the final
database write. Orez cannot perform that write generically in delegated-push
deployments because the application worker is the write authority.

## Why this belongs beside Zero

Orez Lite currently maps a stock Zero client onto `POST /pull`, `POST /push`,
and a notification-only `/wake` WebSocket. The wake carries no row data. Any
frame asks the client to pull, and cookies remain the sole convergence
mechanism. The Cloudflare host already keeps hibernatable sockets on the same
per-namespace Durable Object that owns query membership.

That separation is useful:

- Zero rows, cookies, optimistic mutations, persistence, and recovery keep
  their existing meanings.
- A missed stream frame can affect presentation latency, but it cannot corrupt
  the Zero store.
- A reconnect always recovers durable state with an ordinary pull.
- High-frequency model output avoids the application database, CDC feed,
  ingest replica, change log, and pull path until finalization.

The write avoidance matters on Cloudflare. A logical field update writes the
application row, its indexes, CDC records, the sync replica, and its indexes.
Orez has measured small application pushes at roughly 1,300 billable rows in
Soot's production-shaped schema. Writing every token, or even every small token
batch, would turn display progress into database load.

## What Chat and Soot do today

The architectural observation is right, although the primary app transports
are HTTP streams rather than application WebSockets.

Chat runs generation on the server. It consumes the provider's text stream,
accumulates the complete message content, and writes a growing row snapshot at
most once every 200 milliseconds. Zero and Orez then distribute those durable
row changes. This gives every observer live text and makes reconnect recovery
automatic, but display progress travels through the entire database and sync
path. The implementation lives in:

- `~/chat/src/features/ai/generate.server.ts`
- `~/chat/src/features/ai/chatStreamStore.ts`
- `~/chat/src/features/ai/respond.server.ts`

Soot runs its Pi agent loop in the browser. `/api/llm` returns
`application/x-ndjson` with structured start, text, thinking, tool-call, done,
and error events. The active browser reduces those events into process-local
emitters and writes the final content, parts, and status through Zero at turn
end. This avoids per-token database traffic and isolates React updates to the
changed message. Other observers see only `streaming: active` until the final
write, and a reconnect cannot recover intermediate text. The implementation
lives in:

- `~/soot/src/ai/remote-stream.ts`
- `~/soot/src/ai/llm-proxy-stream.ts`
- `~/soot/src/ai/streamingMessage.ts`
- `~/soot/src/ai/agent-session.ts`
- `~/soot/app/api/llm+api.ts`

Soot also applies a three-attempt retry wrapper in the browser and another
three-attempt wrapper at the server proxy. Before the first meaningful delta,
one client operation can therefore create as many as nine provider attempts.
After a meaningful delta, neither layer can resume without duplicating output.
This is another useful boundary: Orez should own stream generation and delivery
state, while one application layer owns provider retries.

Soot's tool events are more durable than display tokens. It checkpoints active
tool turns and completed tool results before the whole assistant turn finishes.
Those semantic writes should remain. The generic field path removes automatic
per-token persistence; it does not prohibit an application from ending one
generation at a meaningful checkpoint, committing it, and beginning another.

Both apps already use Orez Lite's HTTP pull transport plus its advisory wake
WebSocket for durable transcript distribution. Chat's application-specific
work centers on repeated durable snapshots. Soot's centers on NDJSON parsing,
local overlay state, retry rules, final handoff, and abandoned-stream cleanup.
The proposed API combines Soot's selector-isolated overlay with Chat's
cross-client visibility.

## Goals

1. Any JSON-representable Zero column whose payload codec supports realtime can
   opt into streaming through one typed declaration.
2. The ordinary schema, query builders, mutations, and row values stay
   compatible with `@rocicorp/zero`.
3. A component pays for stream traffic only when it explicitly listens to a
   row field.
4. One shared socket carries wake events and every subscribed field.
5. Orez causes no automatic application-row writes during a stream. The
   application may retain explicit semantic checkpoints.
6. The same public API works against the Cloudflare, native, and browser hosts.
7. Authorization is at least as restrictive as the Zero query that supplied
   the row.
8. A crash or missed frame reveals the last durable Zero value rather than
   inventing recovery state.

## Non-goals

- Replacing Zero pull, push, cookies, or desired queries.
- Making uncommitted stream values durable or available offline.
- Coordinating multiple model producers that write the same field. The
  producer remains responsible for application-level ownership and final-write
  fencing.
- Streaming a row before that row is authorized and visible through Zero.
- Running an LLM request inside a Zero mutator transaction.
- Persisting every intermediate value for replay.

## Schema declaration

Keep the exact Zero schema as the first argument and derive a separate manifest:

```ts
import { defineStreamingFields } from 'orez/realtime'
import { schema } from './zero-schema'

export const streaming = defineStreamingFields(schema, {
  message: {
    content: {
      maxBytes: 512_000,
      maxUpdatesPerSecond: 20,
      maxBytesPerSecond: 2_000_000,
    },
    parts: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 10,
      maxBytesPerSecond: 2_000_000,
      validate: validateMessageParts,
    },
  },
})
```

`defineStreamingFields` accepts only real table and column names. Its generated
row key type comes from the table's Zero primary key, and its value type comes
from the column's Zero type.

The stock schema provides runtime tags for strings, numbers, booleans, null,
and JSON, so Orez can validate those outer shapes. TypeScript custom types do
not carry a runtime validator. A field whose declared TypeScript value is
narrower than its Zero runtime type supplies `validate`, as `parts` does above.
This validator is mandatory for browser-published JSON and custom fields.

The manifest is supplied to the host:

```ts
const config: SyncHostConfig = {
  schema,
  realtime: {
    fields: streaming.manifest,
    // required only for authenticated browser producers
    authorizePublish: authorizeRealtimePublish,
  },
  // existing config
}
```

The manifest stays outside `schema` for two reasons:

1. Zero's public schema shape owns tables, relationships, feature flags, and
   column type metadata. A new property would be Orez-specific even if today's
   runtime happened to ignore it.
2. Zero derives its client schema and compatibility hash from the stock column
   shape. Keeping the manifest parallel prevents an Orez concern from changing
   Zero schema identity or depending on unknown-property behavior.

The same manifest module is imported by the client and by trusted server code.
Build tooling compares its schema identity with the host schema and fails
startup on a mismatch. The builder also receives Orez's existing encrypted
column manifest at host startup. It rejects any selected encrypted field until
the realtime payload codec described below exists. Encryption cannot be
inferred from the stock Zero schema alone.

## Client API

The base value always comes from a normal Zero query:

```tsx
const [message] = useQuery(queries.message.byID({ id }))

const content = useStreamingField(
  streaming.message.content({ id: message.id }),
  message.content
)

return <Markdown>{content.value}</Markdown>
```

The hook returns:

```ts
type StreamingFieldState<Value> = {
  value: Value
  phase: 'durable' | 'streaming' | 'committing' | 'stale'
  streamID: string | null
}
```

Mounting the hook subscribes to the exact logical table, canonical primary key,
and field. Unmounting removes that subscription. A query without the hook sees
only durable Zero state and receives no stream traffic.

When the hook receives an `end` value, it enters `committing` and retains that
overlay until the base value from Zero equals the final streamed value using
schema-aware canonical equality. It then drops the overlay and returns to
`durable`. If the field's durable base changes after a generation begins, that
database value also wins and clears the overlay. This covers a successful final
commit whose `end` frame was lost and a concurrent durable writer. The hook
then ignores later frames from that same `streamID`; only a new generation can
overlay the new durable value. An `abort` drops the overlay immediately. A
local inactivity deadline changes the phase to `stale` and reveals the durable
base; a later value can resume the overlay unless durable state already fenced
that generation.

Canonical equality is exact for primitive Zero values. For a JSON value, Orez
compares canonical JSON encodings with object keys sorted recursively. It
computes that bounded encoding once for each accepted field value and once when
the durable base changes, rather than on every render. Reference equality is
never used for JSON handoff.

The explicit base value avoids hidden mutation of Zero query results. It also
makes the moment of durable handoff visible to UI that wants a subtle
"saving" state.

## Publisher API

A producer opens one generation for a field. The usual producer is trusted
server code:

```ts
const session = await streaming.message.content.begin(publisher, {
  namespace,
  key: { id: messageID },
})

let content = ''
try {
  for await (const token of model.textStream) {
    content += token
    session.set(content)
  }

  await session.finish(content, async () => {
    await db
      .update(messageTable)
      .set({ content, streaming: false })
      .where(eq(messageTable.id, messageID))
  })
} catch (error) {
  await session.abort()
  throw error
}
```

`set` accepts the complete current field value. It coalesces calls to satisfy
both the manifest's update-rate and byte-rate limits, validates the Zero value
type and byte limit, and emits a monotonically increasing sequence number.

Using complete values gives every Zero field one protocol. A client that joins
late or misses sequence numbers recovers on the next value without a replay
buffer or a field-specific reducer. It can also become expensive as a value
grows, so `maxBytesPerSecond` is a required bound rather than an optional
metric. The coalescer delays the next complete value when either limit is
exhausted. Text helpers may accumulate token deltas before calling `set`, but
the wire stays `set(value)`.

`finish(value, commit)` has a strict order:

1. flush the last value;
2. run and await the application's authoritative commit;
3. retain the producer lease and final overlay in `committing`;
4. wait until the Orez replica observes the canonical final field value;
5. emit `end` and close the stream generation.

The commit callback returns the deployment's ordinary durable-write receipt
when one exists. Delegated Cloudflare writes must keep the host's existing
mutate-response causality contract. A direct application write has no receipt,
so the host confirms it when upstream ingest applies the row. Local hosts can
confirm against their application transaction immediately.

If the commit throws, `finish` emits no successful end. If confirmation times
out, the producer reports an indeterminate finish and closes without claiming
success. Clients retain the final overlay while the producer lease is healthy,
then reveal whatever durable value Zero supplies.

The publisher is a transport-neutral capability. On Cloudflare each generation
opens one private, service-bound producer WebSocket to the namespace object for
the life of the stream. This avoids one Worker and Durable Object request per
field value. Local and native runners supply the same interface directly.

An application such as Soot may keep its model reducer in the browser. It uses
the same logical publisher over a dedicated producer socket after one
authoritative `realtime.authorizePublish(topic, claims, env)` check at `begin`.
In a delegated deployment that callback asks the application worker to apply
its existing write permission. Read membership alone cannot grant publish
authority. Later `set`, `finish`, and `abort` frames carry the accepted
`streamID` and need no additional application subrequest.

## Wire protocol

The existing wake socket evolves into a typed realtime channel. Legacy
`"wake"` frames remain accepted during migration.

Client to host:

```json
[
  "subscribe",
  {
    "topic": {
      "table": "message",
      "key": { "id": "message-1" },
      "field": "content"
    }
  }
]
```

```json
[
  "unsubscribe",
  {
    "topic": {
      "table": "message",
      "key": { "id": "message-1" },
      "field": "content"
    }
  }
]
```

Host to client:

```json
["wake", {}]
```

```json
[
  "field",
  {
    "topic": {
      "table": "message",
      "key": { "id": "message-1" },
      "field": "content"
    },
    "streamID": "019...",
    "seq": 14,
    "phase": "streaming",
    "value": "complete current value"
  }
]
```

The terminal phases are `committing` and `aborted`. Every frame is bounded by
the field manifest. The host batches logical field updates that become ready in
the same 25 to 50 millisecond window into one WebSocket frame, stopping before
the host's maximum batch-byte limit. Remaining latest values go in a later
frame.

The topic uses logical Zero names on the public API. The host validates them
against the manifest, maps renamed tables and columns internally, and
canonicalizes composite keys in declared primary-key order.

This feature has one sync-core prerequisite. `_zsync_row_refs.rowPk` currently
uses `serde_json::to_string`, while the native build preserves object insertion
order and the WASM build sorts map keys. Orez must replace that build-dependent
encoding with one schema-driven primary-key encoder in sync-core, then use it
for membership writes, membership lookup, delete IDs, and realtime topics.
Composite-key conformance vectors must run through both native and WASM builds
before field streaming ships.

## Stream generations

Each `begin` creates a random `streamID`. A dedicated producer WebSocket is the
generation's lease. Its serialized host-side attachment contains only:

```text
producer, topic, streamID, startedAt, superseded
```

The host atomically marks any older producer attachment for the topic as
superseded before accepting the new generation. It then closes the older
socket. Every field frame is accepted only from the unsuperseded producer
attachment with the matching `streamID`.

Subscribers receive `streamID` for fencing, but it is never a publish
credential. The host rejects publish frames on a subscriber socket regardless
of their contents. A browser producer's authorization is bound to its
authenticated socket attachment, topic, and generation, so copying a visible
`streamID` grants nothing.

While a host instance is awake, it also keeps the latest complete value and
sequence for each active generation in memory. A new subscriber receives that
snapshot immediately. Hibernation may discard the memory snapshot; the producer
attachment remains. The host sends a `snapshot-request` to that producer when
the first new subscriber arrives without a cached value. The producer answers
with its complete current value. The next scheduled `set` is also a complete
repair if that request races a new value.

The attachment gives restart-safe fencing without storage writes. Cloudflare
preserves it through Durable Object hibernation. Local and native process
restarts close their producer sockets, so old producers cannot deliver delayed
frames into a new host instance.

One producer WebSocket carries one generation, and WebSocket ordering owns
frame order within it. If that socket reconnects, the publisher creates a new
`streamID` and begins with its complete current value. It never resumes an old
generation after reconnect. This avoids persisting `lastSeq` for every value
while still fencing delayed frames from a disconnected producer.

Clients accept a frame only when:

- its stream generation is current for that topic; and
- its sequence number is greater than the last accepted sequence for that
  generation.

Complete-value frames make a sequence gap informational. The next accepted
frame repairs the overlay.

`end`, `abort`, or producer disconnect closes the lease. Client inactivity
deadlines remove abandoned presentation state.

## Authorization

A stream subscription should never create a second permission language.

The realtime socket extends Orez's existing wake-capability flow. The HTTP
transport already calls `wake.getToken()` before each socket attempt. The new
`realtime.getToken({clientID, clientGroupID})` asks the application's
authenticated endpoint for a short-lived signed capability containing the
namespace, user ID, client ID, and client-group ID. The application bearer token
remains on that ordinary HTTPS request.

The socket offers `orez-realtime-v1` plus the base64url capability as separate
WebSocket subprotocol values. The server selects and echoes only
`orez-realtime-v1`. The capability does not enter the URL, the serialized socket
attachment, or the echoed protocol header.

The host adds `authenticateRealtime(request, namespace, env)`, which verifies
the capability and returns its normalized identity. The outer worker calls it
before selecting the namespace object, and the Durable Object calls it again on
the forwarded upgrade. This keeps the current two-boundary authentication
property without forwarding normalized claims in a URL or private header.
Authentication runs once per socket connection, not once per field value.

`authorizeWake` and `wake.getToken()` remain valid for the legacy
notification-only `/wake` route during migration. A data-bearing subscription
is accepted only on the new authenticated realtime contract.

Every host adapter stores the verified identity with the connection:

- Cloudflare serializes user ID, client ID, and client-group ID in the client
  socket attachment.
- Native changes its wake authorization callback to return the same identity
  and records it on the connection in `RealtimeRegistry`.
- The browser-worker installer passes the identity and client-group ID with the
  trusted `MessagePort` attachment. The worker stops broadcasting realtime
  frames to every port and applies the same topic membership check per port.

These are required protocol changes. A client-supplied `clientGroupID` without a
verified capability is never accepted as identity.

For a query-aware deployment, subscription authorization uses the engine's
existing durable membership:

1. verify that the client group belongs to the authenticated user;
2. canonicalize the requested row key;
3. require a positive `_zsync_row_refs` entry for that client group and row;
4. require the field in the streaming manifest.

Chat and Soot already run query-aware hosts, so this reuses the exact rows
their transformed Zero queries authorized.

An optimistic row can reach the component before the server has recorded query
membership. The host answers that subscription as pending, and the transport
retries it after the next successful pull. The producer's in-memory latest
snapshot closes the normal placeholder-to-membership race without granting
early access.

For a visibility-filtered, non-query-aware host, the host evaluates its existing
visibility expression against the requested row. A host with neither
query-aware membership nor visibility cannot serve field streams because it
has no row-level authorization fact.

When a later pull removes row membership, the host drops matching socket
subscriptions as part of the same membership update. The query engine must
return its added and removed row-membership keys to the host as an internal
side result of that pull transaction. This does not change the public Zero pull
response. The host uses the removal set to edit matching socket attachments and
its in-memory topic index before completing the request. This mirrors when the
ordinary Zero client learns that the row left its authorized query.

Server publishing is restricted by the deployment binding. The host still
validates the topic, generation, value type, size, and rate so a producer bug
cannot create unbounded fan-out.

Authenticated browser publishing is disabled unless
`realtime.authorizePublish` is configured. Its authorization result is scoped to
one topic and one new `streamID`, and a newer generation invalidates the prior
grant.

## Cloudflare host

The existing per-namespace sync Durable Object is the right hub:

- it already owns the namespace's authenticated Zero membership;
- it already accepts hibernatable WebSockets;
- it already coalesces wake fan-out;
- it avoids another object hop for every field value.

Client and producer connections both use the Hibernation WebSocket API.
Incoming producer messages wake the object only long enough to validate,
coalesce, and fan out their values. A producer is active only for the stream's
lifetime, and its application worker is already alive to consume the model
response. This shape avoids per-value HTTP or RPC calls and avoids an
always-running publisher process.

The client socket attachment grows from `{clientID}` to the authenticated
connection identity plus a bounded list of subscribed topics. A producer socket
uses the single-generation attachment described above. Cloudflare preserves
both attachment shapes through hibernation, while ordinary in-memory indexes
are rebuilt after the constructor runs.

At runtime the host keeps an in-memory `topic -> sockets` index. The first
publish after hibernation rebuilds it by scanning the bounded attachments;
later publishes fan out only to interested sockets. Subscription changes update
both the attachment and the index. Enforce a per-socket topic count and byte
limit so the attachment remains below Cloudflare's 16 KiB limit.

The host also requires aggregate limits for:

- client and producer connections per namespace;
- active topics per namespace;
- subscriptions per socket and subscribers per topic;
- serialized bytes per batch;
- total fan-out bytes per second, counting payload bytes once per recipient;
- each socket's buffered outbound bytes.

When a field or namespace budget is exhausted, Orez retains one latest unsent
value per active topic and replaces it with newer complete values. It never
queues an unbounded history. A socket that remains over its outbound buffer
limit is closed with a retryable overload code and can reconnect. The bounded
connection and subscription counts also cap the attachment scan after
hibernation. Package defaults should come from the Slice 2 measurements rather
than the maximum limits Cloudflare permits.

The Hibernation WebSocket API keeps clients connected while an idle object
sleeps, so an inactive namespace does not accrue duration merely for open
client sockets. Incoming producer and subscriber WebSocket messages wake the
object. Batching field values also reduces per-message runtime overhead.

Cloudflare references:

- [Durable Object WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
- [Durable Object lifecycle](https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/)
- [Service bindings](https://developers.cloudflare.com/workers/runtime-apis/bindings/service-bindings/)

## Native and local hosts

The native host generalizes `WakeRegistry` into `RealtimeRegistry`:

- the existing per-namespace socket registry remains;
- each connection carries its bounded topic set;
- the same subscribe, unsubscribe, wake, and field frames are used;
- one in-memory topic index replaces Cloudflare socket attachments;
- the application receives an in-process `RealtimePublisher` when embedded, or
  the local supervisor injects the equivalent private publisher binding.

The browser-worker host uses its existing `MessagePort` event channel instead
of a network socket. Its public `RealtimePublisher` and client hook stay the
same, while the port carries identical logical frames. Browser-worker
subscriptions and stream generations are memory-only because that host and
producer share one local lifecycle.

The client transport selects its host adapter when installed. Application code
does not branch on Cloudflare, native, or browser execution.

## Failure semantics

| Event                                  | Visible result                                       | Durable result                                              |
| -------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------- |
| One field frame is lost                | Next complete value repairs the overlay              | Unchanged                                                   |
| Socket disconnects                     | Overlay becomes `stale`; reconnect resubscribes      | Next pull converges                                         |
| Client joins mid-stream                | Next complete value becomes current                  | Unchanged                                                   |
| Producer aborts                        | Overlay drops to the Zero value                      | Unchanged unless the app committed separately               |
| Producer crashes                       | Inactivity deadline drops the overlay                | Last committed Zero value remains                           |
| Final commit fails                     | No successful end; overlay drops on abort or timeout | Existing row remains                                        |
| Final commit succeeds but pull is late | Final overlay stays in `committing`                  | Zero eventually supplies the same value                     |
| Durable Object hibernates              | Attachments restore generation fencing               | Zero remains authoritative                                  |
| Two producers race                     | New `begin` supersedes the old stream generation     | Application final-write fencing decides the database winner |

The socket offers low latency. Pull remains the recovery path.

## Payload codecs and encrypted fields

Realtime values bypass `PayloadCodec.decodePull`, so a declared encrypted
column cannot silently use the clear realtime path. Initial implementation
must compare selected fields with the existing encrypted-column manifest and
reject any overlap at host startup.

A later extension can add explicit `encodeField` and `decodeField` methods to
the payload codec, bind their associated data to schema, table, key, field,
stream ID, and sequence, and then permit encrypted streaming fields. Until that
contract exists, failing startup is safer than leaking plaintext or feeding
unauthenticated values to the UI.

## Options

### Option A: ephemeral overlay on the shared realtime socket

This is the recommendation described above.

Benefits:

- no automatic application-row writes, with a final write by default;
- low latency independent of pull cadence;
- explicit subscriptions keep traffic bounded;
- query membership supplies authorization;
- ordinary Zero state and schema semantics remain intact;
- one reusable socket and hook replace each app's custom protocol.

Costs:

- consumers render `state.value` from a separate hook;
- intermediate values are intentionally unavailable offline;
- producers must send complete bounded values;
- the host gains a small field protocol and subscription index;
- encrypted fields need a field codec before they can opt in.

### Option B: throttled durable field updates through Zero

Provide the same typed publisher API, but coalesce values and commit the field
through the normal application write path every fixed interval, such as 500 or
1,000 milliseconds. Existing Zero queries update without a separate hook.

Benefits:

- maximum Zero compatibility;
- refresh, reconnect, offline state, and other clients see the latest
  checkpoint;
- no new data-bearing socket protocol;
- current authorization and persistence paths apply unchanged.

Costs:

- every checkpoint exercises application writes, indexes, CDC, upstream
  ingest, sync-engine writes, wake fan-out, and client pulls;
- Cloudflare billable writes scale with stream duration and schema index count;
- latency is bounded by the checkpoint and pull path;
- cancellation can leave a partial durable value;
- frequent JSON or text replacement can write increasingly large rows.

This is reasonable for low-frequency progress fields or short operational
status updates. It is a poor default for token streams.

### Option C: synthetic Zero row patches

The Orez browser transport could translate field frames into ordinary Zero
`rowsPatch` pokes. Existing queries would update automatically.

Do not build this. A stock Zero client treats a row patch as replicated state
and persists it with a cookie. An ephemeral value could survive reload, remain
after an abort, or conflict with the next real server cookie. Fixing those cases
would create a second replication protocol hidden inside the Zero protocol.

## Comparison

| Property                              | A: ephemeral overlay       | B: throttled durable writes        | C: synthetic Zero patches            |
| ------------------------------------- | -------------------------- | ---------------------------------- | ------------------------------------ |
| Application DB writes                 | Final by default           | Repeated                           | Final only                           |
| Existing query value changes directly | No                         | Yes                                | Yes                                  |
| Separate listening hook               | Yes                        | No                                 | No                                   |
| Offline intermediate value            | No                         | Yes                                | Accidentally                         |
| Stock Zero semantics                  | Preserved                  | Preserved                          | Violated                             |
| Cloudflare write cost                 | Low                        | Potentially high                   | Low server cost, unsafe client state |
| Reconnect model                       | Pull plus next field value | Pull                               | Ambiguous                            |
| Recommendation                        | Yes                        | Only for low-rate durable progress | No                                   |

## Implementation slices

### Slice 1: protocol and local proof

1. Add the parallel typed manifest.
2. Add a transport-owned realtime event store and `useStreamingField`.
3. Generalize the native wake socket to typed subscribe and field frames.
4. Add the realtime capability minting and verification contract.
5. Return membership additions and removals from the query engine to the host.
6. Add the publisher with begin, set, finish, and abort.
7. Prove late subscribe, missed sequence, abort, final handoff, and reconnect
   against a stock Zero client.

### Slice 2: Cloudflare hub

1. Extend the existing hibernatable socket attachment.
2. Add the bounded topic index and rebuild after hibernation.
3. Add the private producer WebSocket through a service binding.
4. Fence generations with producer socket attachments and no storage writes.
5. Measure one long text stream with 1, 10, and 100 subscribers, including
   billable storage rows, request count, CPU, bytes, and end-to-query latency.

### Slice 3: app migrations

1. Move Chat's message content and Soot's message content onto the shared
   publisher and hook.
2. Delete Chat's repeated snapshot writer and Soot's replaced local field
   emitter, final-handoff, and abandoned-overlay code.
3. Compare hard reload, mid-stream disconnect, token refresh, model failure,
   and final database state against the old implementations.
4. Migrate remaining fields only after those two paths show the generic API is
   smaller and preserves their current behavior.

The provider-facing transports stay application-owned. Chat still consumes the
model provider stream on its server, and Soot still consumes structured NDJSON
from its LLM proxy. Orez replaces field delivery from those producers to
interested clients, not the provider protocol or tool-event reducer.

Soot keeps its durable tool-turn and completed-tool-result writes. If it later
streams `parts`, each such write ends the current field generation, uses the
existing durable checkpoint, and starts a new generation from the committed
parts value.

## Acceptance criteria

- The stock Zero schema object and its compatibility hash are unchanged.
- A component with no field hook receives zero field frames.
- A subscriber cannot receive a row absent from its authorized Zero
  membership.
- A 30-second field stream with no application checkpoint performs one final
  application write and no Orez storage writes.
- Dropping arbitrary field frames cannot produce a wrong eventual value.
- A hard reload during a stream shows durable Zero data, then adopts the next
  complete stream value if the stream is still active.
- An old producer frame cannot cross a newer `streamID`.
- With healthy producer and subscriber sockets, a successful final commit keeps
  the final overlay visible until Zero catches up, with no blank or older-value
  flash.
- Losing the terminal frame still converges when the durable field changes.
- Producer traffic respects the field byte-rate limit, and total delivery
  respects the namespace fan-out budget.
- Local, native, browser-worker, and Cloudflare implementations pass the same
  protocol conformance vectors.
- Chat and Soot delete the field-delivery and overlay code replaced by the
  shared publisher and hook.

## Open decisions

1. Should the first API expose `phase`, or only `{value, streaming}`? The richer
   phase is useful for final handoff and stale state, and costs little.
2. Should `begin` supersede an active generation automatically, or require an
   explicit expected generation? Automatic supersession matches LLM retry
   behavior; applications that can race final writes still need database
   fencing.
3. What default bounds fit real Chat and Soot payloads? Measure their largest
   content and parts values before choosing package defaults.
4. Should the socket endpoint remain `/wake` with typed frames or become
   `/realtime` while `/wake` stays as a compatibility alias? One underlying
   connection matters more than the route name.
