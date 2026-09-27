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
  // bytes queued and not yet written; a receiver this far behind skips a
  // snapshot instead of queueing another behind it.
  buffered(): number
}

export type RoomLimits = {
  tickHz: number
  maxMembers: number
  maxStateBytes: number
  maxEventBytes: number
  maxEventsPerSecond: number
  maxMetaBytes: number
  // retained events a room holds before it refuses new keys.
  maxRetained: number
  maxBufferedBytes: number
}

export const DEFAULT_ROOM_LIMITS: RoomLimits = {
  tickHz: 30,
  maxMembers: 16,
  maxStateBytes: 1024,
  maxEventBytes: 8192,
  maxEventsPerSecond: 60,
  maxMetaBytes: 2048,
  maxRetained: 512,
  maxBufferedBytes: 64 * 1024,
}

type Member = {
  id: number
  meta: unknown
  socket: RoomSocket
  // newest state from this member and whether it went out yet.
  state: SnapshotEntry | null
  dirty: boolean
  // skipped a snapshot while its socket was backed up, so the next one it
  // gets carries every member's newest state rather than only what changed.
  behind: boolean
  // events accepted in the current second.
  budget: number
  budgetSecond: number
}

export type RoomHost = {
  // a socket joins once its hello arrives; everything before it is refused.
  open(socket: RoomSocket): RoomConnection
  // sends one snapshot of changed states to every member.
  tick(): void
  readonly size: number
  readonly limits: RoomLimits
}

export type RoomConnection = {
  message(data: string | Uint8Array): void
  close(): void
}

export function createRoomHost(options: {
  now: () => number
  limits?: Partial<RoomLimits>
}): RoomHost {
  const limits = { ...DEFAULT_ROOM_LIMITS, ...options.limits }
  const { now } = options
  const members = new Map<number, Member>()
  const retained = new Map<string, RoomEvent>()
  let nextId = 1
  let seq = 0
  let tick = 0

  const sendText = (member: Member, message: ServerText) =>
    member.socket.send(JSON.stringify(message))
  const broadcast = (message: ServerText, except?: number) => {
    const text = JSON.stringify(message)
    for (const member of members.values())
      if (member.id !== except) member.socket.send(text)
  }
  const allocateId = () => {
    // ids fit the snapshot's u16 and are never reused while a member holds one.
    for (;;) {
      const id = nextId
      nextId = nextId >= 0xffff ? 1 : nextId + 1
      if (!members.has(id)) return id
    }
  }

  function leave(member: Member) {
    if (!members.delete(member.id)) return
    broadcast({ t: 'leave', id: member.id })
    // the leaver's keys go with it, as ordered removals everyone applies.
    for (const [key, event] of retained) {
      if (event.from !== member.id || event.persist) continue
      retained.delete(key)
      broadcast({ t: 'event', seq: ++seq, at: now(), from: member.id, key, data: null })
    }
  }

  function event(
    member: Member,
    message: Extract<ClientText, { t: 'event' }>,
    size: number
  ) {
    const second = Math.floor(now() / 1000)
    if (second !== member.budgetSecond) {
      member.budgetSecond = second
      member.budget = 0
    }
    const reject = (reason: 'claimed' | 'rate' | 'size') =>
      sendText(member, { t: 'reject', ref: message.ref, reason })
    if (++member.budget > limits.maxEventsPerSecond) return reject('rate')
    if (size > limits.maxEventBytes) return reject('size')
    const { key } = message
    if (key !== undefined && message.ifAbsent && retained.has(key))
      return reject('claimed')
    if (key !== undefined && !retained.has(key) && retained.size >= limits.maxRetained)
      return reject('size')
    const stamped: RoomEvent = {
      seq: ++seq,
      at: now(),
      from: member.id,
      data: message.data,
      ...(key !== undefined ? { key } : {}),
      ...(message.persist ? { persist: true } : {}),
    }
    if (key !== undefined) {
      if (message.data === null) retained.delete(key)
      else retained.set(key, stamped)
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

  return {
    limits,
    get size() {
      return members.size
    },
    open(socket) {
      let member: Member | null = null
      return {
        message(data) {
          if (typeof data !== 'string') {
            if (!member) return
            if (
              data.byteLength < STATE_HEADER ||
              data.byteLength - STATE_HEADER > limits.maxStateBytes
            )
              return
            const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
            // copied: the host keeps it past the socket's buffer.
            member.state = {
              id: member.id,
              sampledAt: view.getFloat64(0, true),
              payload: data.slice(STATE_HEADER),
            }
            member.dirty = true
            return
          }
          let message: ClientText
          try {
            message = JSON.parse(data)
          } catch {
            return socket.close(1003, 'not json')
          }
          if (message.t === 'ping') {
            socket.send(
              JSON.stringify({ t: 'pong', c: message.c, s: now() } satisfies ServerText)
            )
            return
          }
          if (message.t === 'hello') {
            if (member) return
            if (message.v !== ROOM_PROTOCOL_VERSION)
              return socket.close(4000, 'protocol version')
            if (members.size >= limits.maxMembers) return socket.close(4001, 'room full')
            if (JSON.stringify(message.meta ?? null).length > limits.maxMetaBytes)
              return socket.close(4002, 'meta too large')
            member = {
              id: allocateId(),
              meta: message.meta ?? null,
              socket,
              state: null,
              dirty: false,
              behind: false,
              budget: 0,
              budgetSecond: 0,
            }
            const joined: RoomMember = { id: member.id, meta: member.meta }
            broadcast({ t: 'join', member: joined })
            members.set(member.id, member)
            sendText(member, {
              t: 'welcome',
              id: member.id,
              now: now(),
              tickHz: limits.tickHz,
              members: [...members.values()].map((m) => ({ id: m.id, meta: m.meta })),
              events: [...retained.values()].sort((a, b) => a.seq - b.seq),
            })
            // what everyone else looks like now, so the joiner need not wait
            // for each to move.
            const states = [...members.values()].flatMap((m) =>
              m.state && m !== member ? [m.state] : []
            )
            if (states.length) socket.send(encodeSnapshot(tick, now(), states))
            return
          }
          if (message.t === 'event' && member) event(member, message, data.length)
        },
        close() {
          if (member) leave(member)
          member = null
        },
      }
    },
    tick() {
      tick++
      const changed: SnapshotEntry[] = []
      for (const member of members.values())
        if (member.dirty && member.state) {
          changed.push(member.state)
          member.dirty = false
        }
      if (!changed.length && ![...members.values()].some((m) => m.behind)) return
      const at = now()
      for (const member of members.values()) {
        const from = member.behind
          ? [...members.values()].flatMap((m) => (m.state ? [m.state] : []))
          : changed
        // a member does not need its own state back.
        const entries = from.filter((entry) => entry.id !== member.id)
        if (!entries.length) continue
        if (member.socket.buffered() > limits.maxBufferedBytes) {
          member.behind = true
          continue
        }
        member.behind = false
        member.socket.send(encodeSnapshot(tick, at, entries))
      }
    },
  }
}
