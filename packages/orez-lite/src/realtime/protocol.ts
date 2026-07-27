// Wire protocol for orez realtime field streaming.
//
// Field values are ephemeral: they never enter the Zero store, never advance a
// cookie, and never survive a reload. The durable value is whatever the
// ordinary Zero row says. A stream is a presentation-latency optimization over
// that row, so a dropped frame costs latency and can never cost correctness.
//
// Two publish modes, declared per field in the manifest and fixed for the life
// of the field, so no host ever branches on frame shape at runtime:
//
// - `append` (string columns): a frame carries only the new characters. Cost
//   per frame stays flat as the value grows, which is the whole point for token
//   streams. The host accumulates, so a subscriber that arrives at token 900
//   gets one snapshot and then appends.
// - `replace` (every other column type): a frame carries the complete value.
//   Correct for any JSON-representable column, and the right cost model for
//   fields that change at semantic-event frequency rather than per token.
//
// Recovery is the host's accumulator, not a client-side replay buffer. Frames
// travel over one WebSocket, so they arrive in order or the socket is gone; a
// reconnect resubscribes and gets a fresh snapshot. `seq` therefore only has to
// fence a superseded generation and mark where a snapshot ended.

export type RealtimeKeyValue = string | number | boolean | null

export type RealtimeTopic = {
  readonly table: string
  readonly key: Readonly<Record<string, RealtimeKeyValue>>
  readonly field: string
}

// The canonical primary-key encoding, mirroring sync_core::value::canonical_pk
// byte for byte: primary-key columns in the schema's declared order, each key
// and value serialized as JSON. Both sides derive topics independently and must
// land on the same string, so harness/fixtures/canonical-pk-vectors.json is
// asserted here as well as in the native and wasm builds.
export function canonicalPrimaryKey(
  primaryKey: readonly string[],
  key: Readonly<Record<string, RealtimeKeyValue>>
): string {
  let out = '{'
  for (let index = 0; index < primaryKey.length; index++) {
    const column = primaryKey[index]!
    if (index > 0) out += ','
    out += `${JSON.stringify(column)}:${JSON.stringify(key[column] ?? null)}`
  }
  return `${out}}`
}

// ASCII unit separator. A Zero table or column name cannot contain it, and the
// encoded primary key is JSON where it would appear escaped, so the three parts
// of a topic can never run together ambiguously. Written as an escape rather
// than a literal so it stays visible in source.
export const TOPIC_SEPARATOR = '\u001f'

// A topic's wire identity. Field frames carry this string rather than the
// structured topic: it is the map key on both sides, and re-deriving it per
// frame would be pure overhead.
export function canonicalTopic(
  primaryKey: readonly string[],
  topic: RealtimeTopic
): string {
  const key = canonicalPrimaryKey(primaryKey, topic.key)
  return `${topic.table}${TOPIC_SEPARATOR}${topic.field}${TOPIC_SEPARATOR}${key}`
}

// ---- client -> host -------------------------------------------------------

export type SubscribeFrame = readonly ['subscribe', { readonly topic: RealtimeTopic }]
export type UnsubscribeFrame = readonly ['unsubscribe', { readonly topic: RealtimeTopic }]
export type ClientFrame = SubscribeFrame | UnsubscribeFrame

// ---- host -> client -------------------------------------------------------

// A subscription is acknowledged with the canonical topic string the HOST
// derived. The client compares it with its own: agreement is the running check
// that both canonicalizers still match, and a mismatch surfaces as a loud
// client error instead of a subscription that silently receives nothing.
//
// `pending` is the optimistic-row race: the client has a row from a local
// mutation that the server has not yet recorded query membership for. The
// transport retries after the next successful pull rather than the host
// granting early access.
export type SubscribedFrame = readonly [
  'subscribed',
  {
    readonly topic: string
    readonly status: 'active' | 'pending'
  },
]

export type SubscribeErrorFrame = readonly [
  'subscribe-error',
  { readonly topic: string; readonly reason: string },
]

export type FieldUpdate =
  // complete current value: opens a generation, or catches up a new subscriber
  | {
      readonly topic: string
      readonly streamID: string
      readonly seq: number
      readonly op: 'snapshot'
      readonly value: unknown
    }
  // append-mode delta; `text` concatenates onto the accumulated string
  | {
      readonly topic: string
      readonly streamID: string
      readonly seq: number
      readonly op: 'append'
      readonly text: string
    }
  // terminal: the producer's commit landed, hold the overlay until Zero catches up
  | {
      readonly topic: string
      readonly streamID: string
      readonly seq: number
      readonly op: 'end'
    }
  // terminal: drop the overlay now and reveal the durable row
  | {
      readonly topic: string
      readonly streamID: string
      readonly seq: number
      readonly op: 'abort'
    }

export type FieldFrame = readonly ['field', { readonly updates: readonly FieldUpdate[] }]
export type WakeFrame = readonly ['wake', Record<string, never>]

export type HostFrame = FieldFrame | SubscribeErrorFrame | SubscribedFrame | WakeFrame

// The legacy notification-only channel sent the bare text "wake". It stays
// accepted so an old client against a new host, or a new client against the
// /wake route during migration, still pulls.
export const LEGACY_WAKE_FRAME = 'wake'

export function isLegacyWake(raw: string): boolean {
  return raw === LEGACY_WAKE_FRAME
}

export function encodeFrame(frame: ClientFrame | HostFrame): string {
  return JSON.stringify(frame)
}

// Parsing is total: a malformed frame returns undefined rather than throwing,
// because a socket message is attacker-influenced input on the host side and a
// throw inside a message handler would tear down an otherwise healthy socket.
export function decodeFrame(raw: string): ClientFrame | HostFrame | undefined {
  if (isLegacyWake(raw)) return ['wake', {}]
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return undefined
  }
  if (!Array.isArray(parsed) || parsed.length !== 2) return undefined
  const [kind, body] = parsed as [unknown, unknown]
  if (typeof kind !== 'string' || !body || typeof body !== 'object') return undefined
  switch (kind) {
    case 'subscribe':
    case 'unsubscribe': {
      const topic = (body as { topic?: unknown }).topic
      return isTopic(topic) ? ([kind, { topic }] as ClientFrame) : undefined
    }
    case 'subscribed': {
      const { topic, status } = body as { topic?: unknown; status?: unknown }
      if (typeof topic !== 'string') return undefined
      if (status !== 'active' && status !== 'pending') return undefined
      return ['subscribed', { topic, status }]
    }
    case 'subscribe-error': {
      const { topic, reason } = body as { topic?: unknown; reason?: unknown }
      if (typeof topic !== 'string' || typeof reason !== 'string') return undefined
      return ['subscribe-error', { topic, reason }]
    }
    case 'field': {
      const updates = (body as { updates?: unknown }).updates
      if (!Array.isArray(updates)) return undefined
      const decoded: FieldUpdate[] = []
      for (const update of updates) {
        const valid = decodeUpdate(update)
        if (!valid) return undefined
        decoded.push(valid)
      }
      return ['field', { updates: decoded }]
    }
    case 'wake':
      return ['wake', {}]
    default:
      return undefined
  }
}

function decodeUpdate(update: unknown): FieldUpdate | undefined {
  if (!update || typeof update !== 'object') return undefined
  const { topic, streamID, seq, op } = update as Record<string, unknown>
  if (typeof topic !== 'string' || typeof streamID !== 'string') return undefined
  if (typeof seq !== 'number' || !Number.isSafeInteger(seq) || seq < 0) return undefined
  switch (op) {
    case 'snapshot':
      return { topic, streamID, seq, op, value: (update as { value: unknown }).value }
    case 'append': {
      const text = (update as { text?: unknown }).text
      return typeof text === 'string' ? { topic, streamID, seq, op, text } : undefined
    }
    case 'abort':
    case 'end':
      return { topic, streamID, seq, op }
    default:
      return undefined
  }
}

function isTopic(value: unknown): value is RealtimeTopic {
  if (!value || typeof value !== 'object') return false
  const { table, key, field } = value as Record<string, unknown>
  if (typeof table !== 'string' || !table) return false
  if (typeof field !== 'string' || !field) return false
  if (!key || typeof key !== 'object' || Array.isArray(key)) return false
  for (const entry of Object.values(key as Record<string, unknown>)) {
    const type = typeof entry
    if (entry !== null && type !== 'string' && type !== 'number' && type !== 'boolean') {
      return false
    }
  }
  return true
}
