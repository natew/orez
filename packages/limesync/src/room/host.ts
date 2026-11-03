import {
  encodeSnapshot,
  ROOM_PROTOCOL_VERSION,
  STATE_HEADER,
  type ClientText,
  type RoomEvent,
  type RoomMember,
  type ServerText,
  type SnapshotEntry,
} from './protocol.js'

/**
 * One room's state machine, with no I/O and no clock of its own. A host
 * environment (a Node server in development, a Durable Object in production)
 * hands it sockets and calls `tick` on its interval; everything a room does
 * is decided here, so it behaves the same wherever it runs.
 */

export type RoomSocket = {
  send(data: string | Uint8Array): void
  close(code: number, reason: string): void
}

export type RoomLimits = {
  tickHz: number
  maxMembers: number
  // members one user (the ticket's holder) may have in a room at once.
  maxMembersPerUser: number
  maxStateBytes: number
  // states a member may send in a second; the rest are dropped.
  maxStatesPerSecond: number
  maxEventBytes: number
  maxEventsPerSecond: number
  // event bytes one member may send in a second. every member receives every
  // event, so this is also each member's share of what a receiver is sent,
  // and no member can use up another's.
  maxEventBytesPerSecond: number
  maxMetaBytes: number
  // retained events a room holds, and their bytes, before it refuses new ones.
  // the retained events are a joiner's welcome, so this bounds it too.
  maxRetained: number
  maxRetainedBytes: number
  // a receiver this far behind skips snapshots.
  maxBufferedBytes: number
  // a receiver this far behind is closed rather than queued behind: events
  // are reliable, so they cannot be skipped the way snapshots are. it
  // reconnects to a welcome carrying the room as it is.
  maxQueuedBytes: number
}

// a sample stamped further than this from the host's clock is refused, so one
// member's bad clock cannot hold everyone else's history of it in the future.
const MAX_SAMPLE_SKEW_MS = 10_000
// messages a socket may send before its hello; past that it is closed.
const MAX_PRE_HELLO = 32
// how long a socket may stay open without saying hello, and how long a member
// may say nothing at all (a client pings every two seconds) before it is
// taken for gone and its place given back.
const HELLO_DEADLINE_MS = 10_000
const SILENT_MS = 60_000
// pings answered per socket per second; the client sends one every two
// seconds after a short burst, so anything past this is not clock keeping.
const MAX_PINGS_PER_SECOND = 10
// how far a receiver is behind is known from marks: every this many bytes the
// host sends a random mark and the client echoes it as it reads it. a mark
// cannot be echoed before it is read, so a receiver that stops reading cannot
// hide it, and no host needs to see its socket's queue (Cloudflare cannot).
const MARK_BYTES = 16 * 1024

export const DEFAULT_ROOM_LIMITS: RoomLimits = {
  tickHz: 30,
  maxMembers: 16,
  maxMembersPerUser: 4,
  maxStateBytes: 1024,
  maxStatesPerSecond: 60,
  maxEventBytes: 8192,
  maxEventsPerSecond: 60,
  maxEventBytesPerSecond: 16 * 1024,
  maxMetaBytes: 2048,
  maxRetained: 512,
  maxRetainedBytes: 256 * 1024,
  maxBufferedBytes: 64 * 1024,
  maxQueuedBytes: 1024 * 1024,
}

// one socket and what the host has sent it, joined or not yet.
type Link = {
  socket: RoomSocket
  user: string
  openedAt: number
  heard: number
  // bytes written, and written as far as the newest mark the client echoed.
  sent: number
  read: number
  markedAt: number
  marks: Array<{ mark: string; at: number }>
  joined: boolean
  closed: boolean
}

type Member = {
  id: number
  meta: unknown
  link: Link
  // newest state from this member and whether it went out yet.
  state: SnapshotEntry | null
  dirty: boolean
  // skipped a snapshot while it was behind, so the next one it gets carries
  // every member's newest state rather than only what changed.
  behind: boolean
  // states, events and event bytes taken in the current second.
  second: number
  states: number
  events: number
  eventBytes: number
}

export type RoomHost = {
  // a socket joins once its hello arrives; everything before it is refused.
  // `user` is who the room's ticket was signed for.
  open(socket: RoomSocket, user: string): RoomConnection
  // sends one snapshot of changed states to every member.
  tick(): void
  readonly size: number
  readonly limits: RoomLimits
}

export type RoomConnection = {
  message(data: string | Uint8Array): void
  close(): void
}

const sizeOf = (data: string | Uint8Array) =>
  typeof data === 'string' ? data.length : data.byteLength

function randomMark() {
  const words = crypto.getRandomValues(new Uint32Array(2))
  return words[0]!.toString(36) + words[1]!.toString(36)
}

export function createRoomHost(options: {
  now: () => number
  limits?: Partial<RoomLimits>
}): RoomHost {
  const limits = { ...DEFAULT_ROOM_LIMITS, ...options.limits }
  // a state's length rides in the snapshot as a u16.
  if (limits.maxStateBytes > 0xffff) throw new Error('maxStateBytes is at most 65535')
  const { now } = options
  const links = new Set<Link>()
  const members = new Map<number, Member>()
  const retained = new Map<string, { event: RoomEvent; bytes: number }>()
  let retainedBytes = 0
  // keys taken by a claim: only the member holding one may change or drop it.
  const claims = new Set<string>()
  let nextId = 1
  let seq = 0
  let tick = 0

  const drop = (link: Link, code: number, reason: string) => {
    if (link.closed) return
    link.closed = true
    link.socket.close(code, reason)
  }
  const write = (link: Link, data: string | Uint8Array) => {
    if (link.closed) return
    link.socket.send(data)
    link.sent += sizeOf(data)
    if (link.sent - link.markedAt < MARK_BYTES) return
    const mark = randomMark()
    link.markedAt = link.sent
    link.marks.push({ mark, at: link.sent })
    link.socket.send(JSON.stringify({ t: 'mark', m: mark } satisfies ServerText))
  }
  const unread = (link: Link) => link.sent - link.read
  // reliable messages go to every receiver or the receiver goes.
  const deliver = (member: Member, text: string) => {
    if (unread(member.link) > limits.maxQueuedBytes)
      return drop(member.link, 1013, 'too far behind')
    write(member.link, text)
  }
  const sendText = (member: Member, message: ServerText) =>
    deliver(member, JSON.stringify(message))
  const broadcast = (message: ServerText, except?: number) => {
    const text = JSON.stringify(message)
    for (const member of members.values()) if (member.id !== except) deliver(member, text)
  }
  const allocateId = () => {
    // ids fit the snapshot's u16 and are never reused while a member holds one.
    for (;;) {
      const id = nextId
      nextId = nextId >= 0xffff ? 1 : nextId + 1
      if (!members.has(id)) return id
    }
  }
  const release = (key: string) => {
    const held = retained.get(key)
    if (held) retainedBytes -= held.bytes
    retained.delete(key)
    claims.delete(key)
  }

  function leave(member: Member) {
    if (!members.delete(member.id)) return
    broadcast({ t: 'leave', id: member.id })
    // the leaver's keys go with it, as ordered removals everyone applies.
    for (const [key, { event }] of retained) {
      if (event.from !== member.id || event.persist) continue
      release(key)
      broadcast({ t: 'event', seq: ++seq, at: now(), from: member.id, key, data: null })
    }
  }

  // a member's per-second allowances start over each second.
  function spend(member: Member) {
    const second = Math.floor(now() / 1000)
    if (second === member.second) return
    member.second = second
    member.states = 0
    member.events = 0
    member.eventBytes = 0
  }

  function event(
    member: Member,
    message: Extract<ClientText, { t: 'event' }>,
    size: number
  ) {
    spend(member)
    const reject = (reason: 'claimed' | 'rate' | 'size') =>
      sendText(member, { t: 'reject', ref: message.ref, reason })
    if (size > limits.maxEventBytes) return reject('size')
    if (
      ++member.events > limits.maxEventsPerSecond ||
      member.eventBytes + size > limits.maxEventBytesPerSecond
    )
      return reject('rate')
    const { key } = message
    const held = key === undefined ? undefined : retained.get(key)?.event
    if (held && (message.ifAbsent || (claims.has(key!) && held.from !== member.id)))
      return reject('claimed')
    if (
      key !== undefined &&
      message.data !== null &&
      ((!held && retained.size >= limits.maxRetained) ||
        retainedBytes - (held ? retained.get(key)!.bytes : 0) + size >
          limits.maxRetainedBytes)
    )
      return reject('size')
    member.eventBytes += size
    const stamped: RoomEvent = {
      seq: ++seq,
      at: now(),
      from: member.id,
      data: message.data,
      ...(key !== undefined ? { key } : {}),
      ...(message.persist ? { persist: true } : {}),
    }
    if (key !== undefined) {
      release(key)
      if (message.data !== null) {
        retained.set(key, { event: stamped, bytes: size })
        retainedBytes += size
        if (message.ifAbsent) claims.add(key)
      }
    }
    // the sender's copy carries its ref, so it learns its own event's place.
    for (const other of members.values())
      sendText(
        other,
        other === member
          ? { t: 'event', ref: message.ref, ...stamped }
          : { t: 'event', ...stamped }
      )
  }

  const recordOf = (m: Member): RoomMember => ({
    id: m.id,
    user: m.link.user,
    meta: m.meta,
  })

  return {
    limits,
    get size() {
      return members.size
    },
    open(socket, user) {
      const link: Link = {
        socket,
        user,
        openedAt: now(),
        heard: now(),
        sent: 0,
        read: 0,
        markedAt: 0,
        marks: [],
        joined: false,
        closed: false,
      }
      links.add(link)
      let member: Member | null = null
      let early = 0
      let pingSecond = 0
      let pings = 0
      return {
        message(data) {
          if (link.closed) return
          link.heard = now()
          if (typeof data !== 'string') {
            if (!member) return
            spend(member)
            if (++member.states > limits.maxStatesPerSecond) return
            if (
              data.byteLength < STATE_HEADER ||
              data.byteLength - STATE_HEADER > limits.maxStateBytes
            )
              return
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
            const sampledAt = view.getFloat64(0, true)
            if (!(Math.abs(sampledAt - now()) <= MAX_SAMPLE_SKEW_MS)) return
            // copied: the host keeps it past the socket's buffer.
            member.state = { id: member.id, sampledAt, payload: data.slice(STATE_HEADER) }
            member.dirty = true
            return
          }
          if (!member && ++early > MAX_PRE_HELLO) return drop(link, 4003, 'no hello')
          if (data.length > limits.maxEventBytes + limits.maxMetaBytes)
            return drop(link, 1009, 'too large')
          let message: ClientText
          try {
            message = JSON.parse(data)
          } catch {
            return drop(link, 1003, 'not json')
          }
          if (!isClientText(message)) return drop(link, 1003, 'not a room message')
          if (message.t === 'mark') {
            const index = link.marks.findIndex((entry) => entry.mark === message.m)
            if (index < 0) return
            link.read = link.marks[index]!.at
            link.marks.splice(0, index + 1)
            return
          }
          if (message.t === 'ping') {
            const second = Math.floor(now() / 1000)
            if (second !== pingSecond) {
              pingSecond = second
              pings = 0
            }
            if (++pings > MAX_PINGS_PER_SECOND) return
            write(
              link,
              JSON.stringify({ t: 'pong', c: message.c, s: now() } satisfies ServerText)
            )
            return
          }
          if (message.t === 'hello') {
            if (member) return
            if (message.v !== ROOM_PROTOCOL_VERSION)
              return drop(link, 4000, 'protocol version')
            if (members.size >= limits.maxMembers) return drop(link, 4001, 'room full')
            let mine = 0
            for (const other of members.values()) if (other.link.user === user) mine++
            if (mine >= limits.maxMembersPerUser)
              return drop(link, 4004, 'too many from this user')
            if (JSON.stringify(message.meta ?? null).length > limits.maxMetaBytes)
              return drop(link, 4002, 'meta too large')
            link.joined = true
            member = {
              id: allocateId(),
              meta: message.meta ?? null,
              link,
              state: null,
              dirty: false,
              behind: false,
              second: 0,
              states: 0,
              events: 0,
              eventBytes: 0,
            }
            broadcast({ t: 'join', member: recordOf(member) })
            members.set(member.id, member)
            sendText(member, {
              t: 'welcome',
              id: member.id,
              now: now(),
              tickHz: limits.tickHz,
              members: [...members.values()].map(recordOf),
              events: [...retained.values()]
                .map((entry) => entry.event)
                .sort((a, b) => a.seq - b.seq),
            })
            // what everyone else looks like now, so the joiner need not wait
            // for each to move.
            const states = [...members.values()].flatMap((m) =>
              m.state && m !== member ? [m.state] : []
            )
            if (states.length) write(link, encodeSnapshot(tick, now(), states))
            return
          }
          if (message.t === 'event' && member) event(member, message, data.length)
        },
        close() {
          link.closed = true
          links.delete(link)
          if (member) leave(member)
          member = null
        },
      }
    },
    tick() {
      tick++
      const at = now()
      for (const link of links) {
        if (!link.joined && at - link.openedAt > HELLO_DEADLINE_MS)
          drop(link, 4003, 'no hello')
        else if (at - link.heard > SILENT_MS) drop(link, 4005, 'silent')
      }
      const changed: SnapshotEntry[] = []
      for (const member of members.values())
        if (member.dirty && member.state) {
          changed.push(member.state)
          member.dirty = false
        }
      if (!changed.length && ![...members.values()].some((m) => m.behind)) return
      for (const member of members.values()) {
        const from = member.behind
          ? [...members.values()].flatMap((m) => (m.state ? [m.state] : []))
          : changed
        // a member does not need its own state back.
        const entries = from.filter((entry) => entry.id !== member.id)
        if (!entries.length) continue
        if (unread(member.link) > limits.maxBufferedBytes) {
          member.behind = true
          continue
        }
        member.behind = false
        write(member.link, encodeSnapshot(tick, at, entries))
      }
    },
  }
}

// the shape of a client message, checked before any of it is trusted.
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null

function isClientText(message: unknown): message is ClientText {
  if (!isRecord(message)) return false
  switch (message.t) {
    case 'ping':
      return typeof message.c === 'number'
    case 'mark':
      return typeof message.m === 'string'
    case 'hello':
      return typeof message.v === 'number'
    case 'event':
      return (
        typeof message.ref === 'number' &&
        (message.key === undefined || typeof message.key === 'string') &&
        'data' in message
      )
    default:
      return false
  }
}
