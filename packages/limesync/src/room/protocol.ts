/**
 * The room wire format, shared by every host and the client.
 *
 * Two kinds of traffic travel one socket:
 *
 * - **State** is binary and latest-wins. A member sends its own state as often
 *   as it likes; the host keeps only the newest per member and forwards what
 *   changed once per tick. A slow receiver skips ticks rather than queueing
 *   them, so a stalled socket never replays stale positions later.
 * - **Events** are JSON, reliable and totally ordered by the host. Every event
 *   is stamped with a sequence number and the host's clock, so all members
 *   agree what happened first. An event with a `key` is retained as room state
 *   for late joiners, and `ifAbsent` turns it into a claim that only the first
 *   sender wins.
 *
 * Times are milliseconds on the host's clock. Clients estimate it with pings.
 */

export const ROOM_PROTOCOL_VERSION = 2

// `user` is who the member's ticket was signed for, so it can be trusted;
// `meta` is whatever the member said about itself.
export type RoomMember<Meta = unknown> = { id: number; user: string; meta: Meta }

export type RoomEventInput = {
  // retained under this key for late joiners; a later event with the same key
  // replaces it and `data: null` removes it.
  key?: string
  data: unknown
  // a claim: accepted only while nothing is retained under `key`.
  ifAbsent?: boolean
  // retained after its sender leaves; otherwise it goes with them.
  persist?: boolean
}

export type RoomEvent = {
  seq: number
  at: number
  from: number
  key?: string
  data: unknown
  persist?: boolean
}

export type ClientText =
  | { t: 'hello'; v: number; meta: unknown }
  | { t: 'ping'; c: number }
  // echoes a mark as it is read.
  | { t: 'mark'; m: string }
  | ({ t: 'event'; ref: number } & RoomEventInput)

export type ServerText =
  | {
      t: 'welcome'
      id: number
      now: number
      tickHz: number
      members: RoomMember[]
      events: RoomEvent[]
    }
  | { t: 'join'; member: RoomMember }
  | { t: 'leave'; id: number }
  | { t: 'pong'; c: number; s: number }
  // how far a receiver has read: echo it back on arrival.
  | { t: 'mark'; m: string }
  | ({ t: 'event'; ref?: number } & RoomEvent)
  | { t: 'reject'; ref: number; reason: 'claimed' | 'rate' | 'size' }

// a state frame from a client: the host-clock time it was sampled, then the
// application's bytes.
export const STATE_HEADER = 8

export function encodeState(sampledAt: number, payload: Uint8Array) {
  const frame = new Uint8Array(STATE_HEADER + payload.byteLength)
  new DataView(frame.buffer).setFloat64(0, sampledAt, true)
  frame.set(payload, STATE_HEADER)
  return frame
}

// a snapshot: kind, tick and the host's clock, then each changed member's id,
// sample time and bytes.
const SNAPSHOT_KIND = 1
const SNAPSHOT_HEADER = 1 + 4 + 8
const ENTRY_HEADER = 2 + 8 + 2

export type SnapshotEntry = { id: number; sampledAt: number; payload: Uint8Array }

export function encodeSnapshot(
  tick: number,
  now: number,
  entries: ReadonlyArray<SnapshotEntry>
) {
  let size = SNAPSHOT_HEADER
  for (const entry of entries) size += ENTRY_HEADER + entry.payload.byteLength
  const frame = new Uint8Array(size)
  const view = new DataView(frame.buffer)
  view.setUint8(0, SNAPSHOT_KIND)
  view.setUint32(1, tick >>> 0, true)
  view.setFloat64(5, now, true)
  let at = SNAPSHOT_HEADER
  for (const entry of entries) {
    view.setUint16(at, entry.id, true)
    view.setFloat64(at + 2, entry.sampledAt, true)
    view.setUint16(at + 10, entry.payload.byteLength, true)
    frame.set(entry.payload, at + ENTRY_HEADER)
    at += ENTRY_HEADER + entry.payload.byteLength
  }
  return frame
}

export function decodeSnapshot(frame: Uint8Array) {
  const view = new DataView(frame.buffer, frame.byteOffset, frame.byteLength)
  if (frame.byteLength < SNAPSHOT_HEADER || view.getUint8(0) !== SNAPSHOT_KIND)
    return null
  const tick = view.getUint32(1, true)
  const now = view.getFloat64(5, true)
  const entries: SnapshotEntry[] = []
  let at = SNAPSHOT_HEADER
  while (at + ENTRY_HEADER <= frame.byteLength) {
    const length = view.getUint16(at + 10, true)
    const start = at + ENTRY_HEADER
    if (start + length > frame.byteLength) return null
    entries.push({
      id: view.getUint16(at, true),
      sampledAt: view.getFloat64(at + 2, true),
      payload: frame.subarray(start, start + length),
    })
    at = start + length
  }
  return { tick, now, entries }
}
