import {
  decodeSnapshot,
  encodeState,
  ROOM_PROTOCOL_VERSION,
  type ClientText,
  type RoomEvent,
  type RoomEventInput,
  type RoomMember,
  type ServerText,
} from './protocol.js'

/**
 * The client end of a room: the host's clock, the members, the ordered event
 * log, and a short history of every other member's state for interpolation.
 *
 * Games draw other players in the past on purpose. A remote state is shown at
 * `now() - delay`, between the two samples either side of that moment, so it
 * moves smoothly whatever the network does; `delay` grows with the jitter the
 * client measures and shrinks back when the link is steady. Past the newest
 * sample the game extrapolates, briefly, from the last two.
 */

export type RoomSample = { sampledAt: number; payload: Uint8Array }

// where a member stood `delay` ago: the samples either side and how far from
// `from` to `to`. `alpha` above 1 is past the newest sample, capped, for the
// game to extrapolate.
export type RoomFrame = { from: RoomSample; to: RoomSample; alpha: number }

export type RoomStatus = 'connecting' | 'open' | 'closed'

export type RoomClientOptions<Meta> = {
  url: string
  meta: Meta
  onEvent?: (event: RoomEvent) => void
  onJoin?: (member: RoomMember<Meta>) => void
  onLeave?: (id: number) => void
  onStatus?: (status: RoomStatus) => void
  // a runtime without a global WebSocket passes its own.
  WebSocket?: typeof WebSocket
}

export type RoomClient<Meta> = {
  // this member's id once welcomed, and null while connecting.
  readonly id: number | null
  readonly status: RoomStatus
  readonly members: ReadonlyMap<number, Meta>
  // events retained under a key: the room's shared state.
  readonly retained: ReadonlyMap<string, RoomEvent>
  // the host's clock now, in milliseconds.
  now(): number
  // round trip and how much it varies, in milliseconds.
  readonly rtt: number
  readonly jitter: number
  // how far in the past other members are drawn.
  readonly delay: number
  sendState(payload: Uint8Array, sampledAt?: number): void
  // resolves with the event as the host ordered it, or rejects with why not.
  send(event: RoomEventInput): Promise<RoomEvent>
  frame(id: number, at?: number): RoomFrame | null
  latest(id: number): RoomSample | null
  close(): void
}

// clock samples kept, pings at the start and then steadily.
const CLOCK_SAMPLES = 12
const BURST_PINGS = 5
const BURST_MS = 60
const PING_MS = 2000
// history kept per member, and the longest a state is extrapolated past its
// newest sample.
const HISTORY = 48
const MAX_EXTRAPOLATE_MS = 250
// the delay never drops below two snapshots' spacing, and grows with jitter.
const MIN_DELAY_MS = 60
const MAX_DELAY_MS = 400
const RECONNECT_MS = [250, 500, 1000, 2000, 4000]

export function connectRoom<Meta>(options: RoomClientOptions<Meta>): RoomClient<Meta> {
  const Socket = options.WebSocket ?? globalThis.WebSocket
  const members = new Map<number, Meta>()
  const retained = new Map<string, RoomEvent>()
  const history = new Map<number, RoomSample[]>()
  const pending = new Map<
    number,
    { resolve: (e: RoomEvent) => void; reject: (e: Error) => void }
  >()
  const clock: Array<{ rtt: number; offset: number }> = []
  let socket: WebSocket | null = null
  let id: number | null = null
  let status: RoomStatus = 'connecting'
  let offset = 0
  let synced = false
  let rtt = 0
  let jitter = 0
  let transit: number | null = null
  let tickMs = 1000 / 30
  let ref = 0
  let closed = false
  let attempt = 0
  let timers: ReturnType<typeof setTimeout>[] = []
  let pinger: ReturnType<typeof setInterval> | null = null

  const setStatus = (next: RoomStatus) => {
    if (status === next) return
    status = next
    options.onStatus?.(next)
  }
  const now = () => performance.now() + offset
  const delayOf = () =>
    Math.min(MAX_DELAY_MS, Math.max(MIN_DELAY_MS, tickMs * 2 + jitter * 3))
  const send = (message: ClientText) => {
    if (socket?.readyState === 1) socket.send(JSON.stringify(message))
  }
  const ping = () => send({ t: 'ping', c: performance.now() })

  function onClock(c: number, s: number) {
    const local = performance.now()
    const sample = { rtt: local - c, offset: s + (local - c) / 2 - local }
    clock.push(sample)
    if (clock.length > CLOCK_SAMPLES) clock.shift()
    // the quickest round trips carry the least queueing, so the offset comes
    // from the best of the recent ones.
    const best = clock.reduce((a, b) => (b.rtt < a.rtt ? b : a))
    rtt = clock.reduce((sum, x) => sum + x.rtt, 0) / clock.length
    if (!synced || Math.abs(best.offset - offset) > 250) {
      offset = best.offset
      // transit measured against the old clock means nothing now.
      transit = null
      jitter = 0
    }
    // slewed, so the clock never runs backwards under a playing game.
    else offset += Math.max(-4, Math.min(4, best.offset - offset))
    synced = true
  }

  function onSnapshot(frame: Uint8Array) {
    const snapshot = decodeSnapshot(frame)
    if (!snapshot) return
    // jitter: how much the one-way transit of snapshots varies.
    // measured once the first burst of pings has settled the clock.
    if (synced && clock.length >= BURST_PINGS) {
      const sample = now() - snapshot.now
      transit ??= sample
      jitter += (Math.abs(sample - transit) - jitter) / 16
      transit += (sample - transit) / 16
    }
    for (const entry of snapshot.entries) {
      let samples = history.get(entry.id)
      if (!samples) history.set(entry.id, (samples = []))
      const last = samples[samples.length - 1]
      if (last && entry.sampledAt <= last.sampledAt) continue
      samples.push({ sampledAt: entry.sampledAt, payload: entry.payload.slice() })
      if (samples.length > HISTORY) samples.shift()
    }
  }

  function onText(message: ServerText) {
    switch (message.t) {
      case 'welcome':
        id = message.id
        tickMs = 1000 / message.tickHz
        members.clear()
        for (const member of message.members) members.set(member.id, member.meta as Meta)
        retained.clear()
        for (const event of message.events) if (event.key) retained.set(event.key, event)
        attempt = 0
        setStatus('open')
        for (const member of message.members)
          if (member.id !== id) options.onJoin?.(member as RoomMember<Meta>)
        for (const event of message.events) options.onEvent?.(event)
        return
      case 'join':
        members.set(message.member.id, message.member.meta as Meta)
        options.onJoin?.(message.member as RoomMember<Meta>)
        return
      case 'leave':
        members.delete(message.id)
        history.delete(message.id)
        options.onLeave?.(message.id)
        return
      case 'pong':
        onClock(message.c, message.s)
        return
      case 'event': {
        const { ref: own, t: _, ...event } = message
        if (event.key !== undefined) {
          if (event.data === null) retained.delete(event.key)
          else retained.set(event.key, event)
        }
        if (own !== undefined) {
          pending.get(own)?.resolve(event)
          pending.delete(own)
        }
        options.onEvent?.(event)
        return
      }
      case 'reject':
        pending
          .get(message.ref)
          ?.reject(new Error(`room event rejected: ${message.reason}`))
        pending.delete(message.ref)
        return
    }
  }

  function open() {
    setStatus('connecting')
    const ws = new Socket(options.url)
    ws.binaryType = 'arraybuffer'
    socket = ws
    ws.onopen = () => {
      send({ t: 'hello', v: ROOM_PROTOCOL_VERSION, meta: options.meta })
      clock.length = 0
      synced = false
      timers = []
      for (let i = 0; i < BURST_PINGS; i++) timers.push(setTimeout(ping, i * BURST_MS))
      pinger = setInterval(ping, PING_MS)
    }
    ws.onmessage = (message) => {
      if (typeof message.data === 'string') onText(JSON.parse(message.data))
      else onSnapshot(new Uint8Array(message.data as ArrayBuffer))
    }
    ws.onclose = () => {
      if (socket !== ws) return
      socket = null
      id = null
      if (pinger) clearInterval(pinger)
      pinger = null
      for (const waiting of pending.values()) waiting.reject(new Error('room closed'))
      pending.clear()
      for (const member of members.keys()) options.onLeave?.(member)
      members.clear()
      history.clear()
      if (closed) return setStatus('closed')
      setStatus('connecting')
      const wait = RECONNECT_MS[Math.min(attempt++, RECONNECT_MS.length - 1)]!
      timers.push(setTimeout(open, wait))
    }
  }
  open()

  return {
    get id() {
      return id
    },
    get status() {
      return status
    },
    members,
    retained,
    now,
    get rtt() {
      return rtt
    },
    get jitter() {
      return jitter
    },
    get delay() {
      return delayOf()
    },
    sendState(payload, sampledAt = now()) {
      if (socket?.readyState === 1 && id !== null)
        socket.send(encodeState(sampledAt, payload))
    },
    send(event) {
      return new Promise((resolve, reject) => {
        if (socket?.readyState !== 1 || id === null)
          return reject(new Error('room not open'))
        const n = ++ref
        pending.set(n, { resolve, reject })
        send({ t: 'event', ref: n, ...event })
      })
    },
    frame(member, at = now() - delayOf()) {
      const samples = history.get(member)
      if (!samples?.length) return null
      const newest = samples[samples.length - 1]!
      if (samples.length === 1) return { from: newest, to: newest, alpha: 0 }
      if (at >= newest.sampledAt) {
        const before = samples[samples.length - 2]!
        const span = newest.sampledAt - before.sampledAt || 1
        const past =
          Math.min(at, newest.sampledAt + MAX_EXTRAPOLATE_MS) - before.sampledAt
        return { from: before, to: newest, alpha: past / span }
      }
      for (let i = samples.length - 2; i >= 0; i--) {
        const from = samples[i]!
        if (from.sampledAt <= at) {
          const to = samples[i + 1]!
          return {
            from,
            to,
            alpha: (at - from.sampledAt) / (to.sampledAt - from.sampledAt || 1),
          }
        }
      }
      return { from: samples[0]!, to: samples[0]!, alpha: 0 }
    },
    latest(member) {
      const samples = history.get(member)
      return samples?.[samples.length - 1] ?? null
    },
    close() {
      closed = true
      for (const timer of timers) clearTimeout(timer)
      timers = []
      if (pinger) clearInterval(pinger)
      if (socket) socket.close(1000, 'bye')
      else setStatus('closed')
    },
  }
}
