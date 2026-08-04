# Streaming fields

A streaming field is a column whose value can be broadcast to clients _before_
it is written to the database. The row in the database stays the truth; a stream
is a way to show a value early.

The case it exists for is generated text. A model produces a message over
several seconds, and the reader should watch it arrive rather than wait for a
finished row. Writing each token durably is the obvious approach and the wrong
one: in Soot's production-shaped schema one small application push costs roughly
1,300 billable rows once you count the row, its indexes, CDC records, the sync
replica, and its indexes. Streaming a thousand tokens that way turns display
progress into database load.

So the value travels beside Zero instead of through it. Nothing about the Zero
store changes: no cookie advances, no row is written, and a client that misses
every frame still converges on the committed value with an ordinary pull. A
dropped frame costs latency, never correctness.

## The model

Three concepts, and everything else follows from them.

**A manifest** declares which columns may stream. It is validated against your
real Zero schema at startup, so a renamed column fails loudly instead of
silently never streaming.

**A generation** is one attempt at producing a value. Opening a new generation
for a row supersedes the old one, which is what a model retry needs: the
displaced producer is told, and its late frames are refused.

**The overlay** is what a client shows. It sits on top of the durable row and
gets out of the way once the database catches up.

## Declaring fields

```ts
import { defineStreamingFields } from 'orez-lite/realtime'
import { schema } from './schema'

export const streaming = defineStreamingFields(schema, {
  message: {
    content: {
      maxBytes: 2_000_000,
      maxUpdatesPerSecond: 30,
      maxBytesPerSecond: 500_000,
    },
  },
})
```

The bounds are required, not optional with defaults. A streaming field is a
channel an application writes to at whatever rate its producer happens to run,
and a field with no declared ceiling is one incident away from being the reason
a socket is saturated. Making you write them means the limit is a decision
rather than an accident.

### Append and replace

Each field has a publish mode, fixed at manifest time so no host ever branches
on frame shape at runtime:

- **`append`** is inferred for string columns. A frame carries only the new
  characters, so cost per frame stays flat however long the value grows. This is
  what makes token streaming affordable.
- **`replace`** is inferred for every other column type. A frame carries the
  complete value. Correct for anything JSON can represent, and the right cost
  model for a field that changes at semantic-event frequency rather than per
  token.

The inference is right most of the time and wrong in one specific case: a JSON
payload stored in a TEXT column. The column type says string, so `append` is
inferred, but the producer rewrites the value rather than extending it. Declare
it explicitly:

```ts
parts: {
  maxBytes: 2_000_000,
  maxUpdatesPerSecond: 20,
  maxBytesPerSecond: 1_000_000,
  mode: 'replace',
}
```

If you get this wrong the producer throws on the first non-extending value and
tells you which field and what to do, rather than shipping a corrupted suffix.

## Producing values

Every surface gives a producer the same two handles, so producer code moves
between them unchanged. This is enforced by a test, not by convention
(`realtime/surfaces.test.ts`).

```ts
// imperative: generations are managed for you. This is what most loops want.
realtime.fields.set(streaming.message.content({ id }), text)

// explicit, for a producer that controls begin/commit/end itself
const session = await realtime.publisher.begin('message', 'content', {
  namespace: 'default',
  key: { id },
})
```

`set` is synchronous and cheap on purpose. A token loop calls it on every model
event and never awaits, and never has to handle an error from it: a failed write
is reported through `onError`, because throwing would abort a model run over a
presentation concern.

### Finishing

The interesting part is the handoff. Clearing the overlay the moment you write
the row flashes stale text, because the reader's own Zero query has not produced
the new value yet.

```ts
await realtime.fields.finish(handle, finalText, async () => {
  await writeTheRowDurably(finalText)
})
```

`finish` flushes the final value, runs _your_ durable write, and then holds the
overlay in a `committing` state until each reader's own query produces that
value. Orez never performs the write itself: in a delegated-push deployment the
application worker is the write authority, so the commit is a callback you
supply.

Use `abort` when there is no value worth showing. It drops the overlay
immediately and reveals the durable row.

## Reading values

```ts
const { value, phase } = useStreamingField(
  isStreaming ? streaming.message.content({ id }) : null,
  row.content // the durable value, shown whenever no stream is active
)
```

Subscriptions are per row and per field. A token for one message can only wake
the components that asked for that message, which is a structural property
rather than something each consumer has to remember to implement with a
projection and an equality gate.

`phase` is one of:

| phase        | meaning                                                         |
| ------------ | --------------------------------------------------------------- |
| `durable`    | no stream; the value is the row's                               |
| `streaming`  | a generation is in flight                                       |
| `committing` | the producer finished; holding until your query catches up      |
| `stale`      | no frame for `staleAfterMs`; the durable value is showing again |

## Surfaces

The hub is one state machine with no I/O, so a stream behaves identically
everywhere: same generations, same accumulation, same handoff, same bounds. What
differs between surfaces is only how frames travel.

| surface                | producer                | subscribers                | use when                                                      |
| ---------------------- | ----------------------- | -------------------------- | ------------------------------------------------------------- |
| `createLocalRealtime`  | in-process              | same context               | the tab producing values is the tab rendering them            |
| `BrowserRealtime`      | in the worker           | pages over a `MessagePort` | a shared worker owns the sync engine                          |
| `createSocketProducer` | off-host, over a socket | wherever the hub is        | an application server generates values for browsers elsewhere |
| `createSocketHost`     | over a socket           | over sockets               | the hub runs in a Durable Object or a Node server             |

Choosing between them is one question: **is the code producing the value in the
same process as the code displaying it?** If yes, use the local surface and stop
there. It needs no authorization, no socket, and no host, because there is
nobody to withhold values from and nothing to send them over.

If no, the producer needs to reach a hub over a wire, and that hub needs to
decide who may subscribe.

Expect the answer to be no. It is easy to look at one deployment of an
application and conclude its producer is in-process, and be wrong about the
same application's other deployments. Soot runs its agent loop in the browser on
desktop and routes the same chat through a separate Cloudflare service on
mobile; the second one is a server-side producer whatever the first one is. Pick
the surface per deployment, not per application.

### On React Native

Every surface takes an optional `randomID`, and Hermes is the reason. A producer
names each generation with an id that defaults to `crypto.randomUUID()`, and
Hermes has no `crypto` global, so on device the default throws at `begin` and no
frame is ever produced. Pass an id source when the producer runs there:

```ts
import { randomUUID } from 'expo-crypto'

createLocalRealtime({ manifest: streaming.manifest, randomID: () => randomUUID() })
```

Nothing else in the client path needs a polyfill; the store and the hub do no
crypto at all.

### Where the hub goes

Put the hub where the subscribers already are, which in a Zero deployment means
the per-namespace object their sync sockets are attached to. It is also where
query membership lives, and membership is the authorization fact a subscription
is checked against. A hub anywhere else needs its own copy of both.

### On Cloudflare

`sync-cf-host` mounts the hub in the namespace Durable Object, which is where
both facts it needs already are: the clients' sockets, and the query membership
a subscription is checked against. Two config options turn it on.

```ts
createSyncWorker({
  streamingManifest: streaming.manifest,
  // Server-side producers only. Absent means the namespace accepts none.
  authorizeProduce: (request, env) => request.headers.get('x-internal') === env.SECRET,
  authorizeWake: async (request, env) => {
    const claims = await authenticate(request, env)
    // `{ userID }` rather than `true`: subscriptions ride this socket
    return claims && { userID: claims.userID }
  },
})
```

Subscriptions ride the existing wake socket rather than opening a second one.
That socket is a `GET` upgrade, so the client carries its existing Zero auth in
the WebSocket subprotocol. The worker presents it to `authorizeWake` as the
ordinary `Authorization` header, and the callback returns the normalized
identity.
Returning bare `true` from a namespace that sets `streamingManifest` is refused
rather than downgraded, because a wake-only socket whose subscriptions never
deliver looks exactly like a stream that is merely slow.

The client's `clientID` and `clientGroupID` stay query parameters, and stay
claims. The engine checks the group against the authenticated userID before it
reads a row, so asserting someone else's group buys nothing.

Producers connect to `/<namespace>/realtime/produce` and are authorized by
`authorizeProduce`, separately from end users, because publishing into any
row's field is a much stronger capability than reading one. On the producer
side that socket is driven by `createSocketProducer`, so the code that writes
values is the same code it would be in-process:

```ts
const socket = new WebSocket(`${origin}/realtime/produce?producerKey=${key}`)
const producer = createSocketProducer(socket, { manifest: streaming.manifest })
socket.addEventListener('message', (event) => producer.handleMessage(event.data))
socket.addEventListener('close', () => producer.fail('socket closed'))

// from here on, identical to the local surface
producer.fields.set(streaming.message.content({ id }), text)
```

Open that socket when the producer is warmed rather than on its first value.
Soot measured warm calls into its chat service at 386ms and 393ms while the
service itself reported 0ms of work, so almost all of the cost is in getting
there, and paying it on the first token would sit squarely in the latency the
stream exists to hide.

**Hibernation.** Cloudflare evicts the object while its sockets stay open, so
the hub is rebuilt on the next frame and each socket's topics are replayed from
its attachment. Replaying `subscribe` re-authorizes as a side effect, so
membership revoked during the eviction is honoured rather than restored.
Generations deliberately do not survive: they are ephemeral, the durable row is
still the truth, and a producer whose generation vanished opens a new one.

`harness/src/streaming-fields.ts` drives all of this against a real Durable
Object, eviction included.

### Frame routing

A stream has exactly three directions, and there is one function for each. Any
new transport is built from them and cannot quietly disagree with the others:

```
applyClientFrame     subscriber -> hub     subscribe, unsubscribe
applyProducerFrame   producer   -> hub     begin, publish
applyHostFrame       hub        -> store   subscribed, field, subscribe-error
```

Everything else in a surface is channel plumbing.

## Authorization

A subscription is an authorization decision. It grants a live feed of a row, so
it must be checked against the same fact the Zero query used: row membership for
the caller's client group.

Hosts answer one of three ways:

- **`active`** — membership confirmed, frames flow.
- **`pending`** — the caller owns the client group but the server has not
  recorded membership yet. This is the optimistic-row race: the client holds a
  row from its own mutation. It retries after its next pull rather than the host
  granting early access.
- **`denied`** — including when the caller does not own the client group it
  claimed. The reason deliberately does not reveal whether the row exists.

A client group id is not a bearer token. Identity must come from the host's
authenticated path, never from something the connection asserts about itself.

## What it does not do

Worth being explicit, because each of these is a deliberate choice rather than a
gap:

- **Streamed values never reach another client through the overlay if the
  producer is local.** The local surface is one context by definition. Another
  tab or another user sees the committed row, which is the same guarantee the
  overlay already makes on a reload.
- **Nothing survives a restart.** Values are ephemeral. There is no replay
  buffer on the client because the host accumulates instead: a subscriber
  arriving at token 900 gets one snapshot and then deltas.
- **Orez never writes your row.** See `finish` above.
- **`seq` does not order recovery.** Frames travel over one socket, so they
  arrive in order or the socket is gone. It exists to fence a superseded
  generation and mark where a snapshot ended.
