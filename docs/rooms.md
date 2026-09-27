# Rooms

A room is a realtime channel for games and anything else that moves faster
than a database should: cursors, cars, players. It rides beside Zero. Zero
keeps what must survive (the level, the scores); a room carries what is only
true for the next few milliseconds.

```ts
import { connectRoom, ROOM_PATH } from 'orez-lite/room'

const room = connectRoom({ url: `wss://your.app${ROOM_PATH}race-42`, meta: { name } })
```

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
  rest are rejected. Item boxes, seats and "who crossed first" are claims.
- A member's retained events go when it leaves, as ordered removals, unless
  sent with `persist: true`.

## Time

`room.now()` is the host's clock, estimated from pings: a burst on connect,
then every two seconds, taking the offset from the quickest recent round
trips and slewing rather than jumping so a running game never sees time go
backwards. Schedule shared moments (a race start, a round end) as host times
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

- **Development**: the `orez()` Vite plugin serves rooms on the dev server's
  own port at `/__orez/room/<name>`, so web and native clients reach them at
  the app's origin.
- **Cloudflare**: bind `OrezRoomDO` (from `orez-lite/room/cloudflare`, also
  returned by `createOrezDataWorker`) as `OREZ_ROOM_DO`, and the data worker
  routes `/__orez/room/<name>` to one object per room name.

Every bound is a limit in `RoomLimits`: members, state and event size, events
per second, retained keys and metadata size.
