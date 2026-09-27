# Rooms

A room is a realtime channel for games and anything else that moves faster
than a database should: cursors, cars, players. It rides beside Zero. Zero
keeps what must survive (the level, the scores); a room carries what is only
true for the next few milliseconds.

```ts
import { connectRoom, ROOM_PATH } from 'orez-lite/room'

const room = connectRoom({
  // called for every connect, so each reconnect carries a fresh ticket.
  url: async () => {
    const { ticket } = await fetch('/api/room-ticket?room=race-42').then((r) => r.json())
    return `wss://your.app${ROOM_PATH}race-42?ticket=${ticket}`
  },
  meta: { name },
})
```

## Who gets in

A room admits a socket only with a ticket for that room. The app's server
decides who may join (a session, access to whatever the room is about) and
signs one with `signRoomTicket(secret, room, userId)` from
`orez-lite/room/ticket`; every host checks it with the same secret before the
socket joins, so a room never looks up a session itself and nobody reaches a
room the app did not admit them to. A ticket names its room and its user and
lasts a minute by default; the client asks for a new one on every connect.

The ticket's user is on every member as `user`, and can be trusted; `meta` is
whatever the member says about itself. One user holds at most
`maxMembersPerUser` places in a room, so a single account cannot fill it.

## What travels

**State** is binary and latest-wins. Each member sends its own state as often
as it likes with `room.sendState(bytes)`; the host keeps only the newest per
member and forwards what changed once per tick (30 Hz by default). A receiver
whose socket is backed up skips ticks and gets everyone's newest state when it
catches up, so a stalled connection never replays stale positions later. This
is the property UDP gives real games, delivered over a WebSocket.

**Events** are JSON, reliable and totally ordered by the host.
`room.send({ key, data })` resolves with the event stamped with its sequence
number and the host's clock, so every member agrees what happened first.

- An event with a `key` is retained as room state. Late joiners receive every
  retained event in order in their welcome, and `data: null` removes one.
- `ifAbsent: true` makes the event a claim: only the first sender wins, the
  rest are rejected, and only the holder may change or remove it. Item
  boxes, seats and "who crossed first" are claims. Any member may change or
  remove a key that is not a claim.
- A member's retained events go when it leaves, as ordered removals, unless
  sent with `persist: true`.

## Time

`room.now()` is the host's clock, estimated from pings: a burst on connect,
then every two seconds, taking the offset from the quickest recent round
trips and slewing it at most 4 ms per ping so a running game does not see
time go backwards. The welcome's own clock stands in until the first pong,
and an estimate more than 250 ms out is corrected at once. Samples stamped
more than ten seconds from the host's clock are dropped. Schedule shared moments (a race start, a round end) as host times
in an event, and every client counts down to the same instant.

## Drawing other members

Other members are drawn slightly in the past on purpose. `room.frame(id)`
returns the two samples either side of `now() - delay` and how far between
them, or an `alpha` above 1 when the newest sample is late, for a short
extrapolation. `delay` is two ticks plus three times the measured jitter, so
it grows on a bad link and shrinks back on a steady one. Interpolate with the
velocities in your state (a cubic Hermite) and a car sweeping a curve stays on
it.

Your own member is never delayed: simulate it locally and send its state.
Resolve contacts from each player's own view, pushing your car against where
the other is drawn, the way racing games do.

## Hosts

The room itself (`orez-lite/room/host`) has no I/O and no clock. Two hosts
wrap it:

- **Development**: with `roomSecret` in `orez-lite.config.ts`, the `orez()`
  Vite plugin serves rooms on the dev server's own port at
  `/__orez/room/<name>`, so web and native clients reach them at the app's
  origin.
- **Cloudflare**: bind `OrezRoomDO` (from `orez-lite/room/cloudflare`, also
  returned by `createOrezDataWorker`) as `OREZ_ROOM_DO` and the secret as
  `OREZ_ROOM_SECRET`, and the data worker routes `/__orez/room/<name>` to one
  object per room name. A host of its own calls `routeRoom(request, rooms,
  secret)`.

Every bound is a limit in `RoomLimits`: members (in all and per user), state
size and states per second, event size, events and event bytes per member per
second, retained keys and their bytes (which bound a welcome), and metadata
size. A socket that sends more than a few messages before its hello, stays
ten seconds without one, says nothing for a minute, or sends anything that is
not a room message, is closed.

The host knows how far behind each receiver is without seeing its socket's
queue (Cloudflare does not show one): every 16 KB it sends a random mark, and
the client echoes each mark as it reads it. A mark cannot be echoed before it
arrives, so a receiver that stops reading cannot hide it. A receiver more than
`maxBufferedBytes` behind skips snapshots, and one more than `maxQueuedBytes`
behind is closed, because events are reliable and cannot be skipped; it
reconnects to a welcome with the room as it is.
