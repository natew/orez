// The host-side realtime hub: one pure state machine shared by every
// TypeScript host (the Cloudflare Durable Object and the browser worker).
//
// It owns no I/O. Connections are opaque handles with a `send`, so the same
// logic drives a hibernatable WebSocket, a MessagePort, and a test harness. A
// host adapter is then only socket lifecycle plus authentication.
//
// The hub ACCUMULATES each generation's value. That is what makes append-mode
// frames safe: a subscriber arriving at token 900 gets one snapshot of what the
// hub has, then appends, and no client ever needs a replay buffer. It is also
// why a lost frame cannot produce a wrong value, since the next snapshot after
// any reconnect is authoritative.

import { resolveTopic } from './manifest.js'
import { canonicalEncode } from './store.js'

import type { StreamingFieldSpec, StreamingManifest } from './manifest.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'

export type RealtimeIdentity = {
  readonly userID: string
  readonly clientID: string
  readonly clientGroupID: string
}

export interface HubConnection {
  readonly id: string
  readonly identity: RealtimeIdentity
  send(frame: unknown): void
}

export interface HubProducer {
  readonly id: string
  send(frame: unknown): void
}

// A subscription is authorized against the SAME fact the Zero query used: a
// positive row-membership entry for this client group. `pending` is the
// optimistic-row race, where the client has a locally-mutated row the server
// has not recorded membership for yet.
export type SubscribeAuthorization =
  | { readonly status: 'active' | 'pending' }
  | { readonly status: 'denied'; readonly reason: string }

export type HubLimits = {
  readonly maxTopicsPerConnection: number
  readonly maxSubscribersPerTopic: number
  readonly maxTopicsPerNamespace: number
  readonly maxBatchBytes: number
  // how long ready updates wait to be coalesced into one frame
  readonly batchWindowMs: number
}

export const DEFAULT_HUB_LIMITS: HubLimits = {
  maxTopicsPerConnection: 256,
  maxSubscribersPerTopic: 512,
  maxTopicsPerNamespace: 4096,
  maxBatchBytes: 512 * 1024,
  batchWindowMs: 25,
}

export type HubOptions = {
  readonly manifest: StreamingManifest
  readonly authorizeSubscribe: (
    identity: RealtimeIdentity,
    topic: RealtimeTopic,
    spec: StreamingFieldSpec
  ) => SubscribeAuthorization
  readonly limits?: Partial<HubLimits>
  // injected so tests drive batching deterministically
  readonly scheduleFlush?: (flush: () => void, ms: number) => () => void
}

type Generation = {
  readonly topicID: string
  readonly spec: StreamingFieldSpec
  streamID: string
  seq: number
  // accumulated value: the concatenated string in append mode, or the last
  // complete value in replace mode
  value: unknown
  producer: HubProducer | undefined
  superseded: boolean
}

export class RealtimeHub {
  readonly #manifest: StreamingManifest
  readonly #authorize: HubOptions['authorizeSubscribe']
  readonly #limits: HubLimits
  readonly #schedule: (flush: () => void, ms: number) => () => void

  readonly #generations = new Map<string, Generation>()
  readonly #subscribers = new Map<string, Set<HubConnection>>()
  readonly #connectionTopics = new Map<string, Set<string>>()
  readonly #connections = new Map<string, HubConnection>()
  // producer id -> the one generation it leases. One producer socket carries
  // one generation, so the socket closing is what releases the lease.
  readonly #producerGenerations = new Map<string, string>()

  // per-connection outbound batch, flushed on the batching window
  readonly #outbox = new Map<string, FieldUpdate[]>()
  #cancelFlush: (() => void) | undefined

  constructor(options: HubOptions) {
    this.#manifest = options.manifest
    this.#authorize = options.authorizeSubscribe
    this.#limits = { ...DEFAULT_HUB_LIMITS, ...options.limits }
    this.#schedule =
      options.scheduleFlush ??
      ((flush, ms) => {
        const timer = setTimeout(flush, ms)
        return () => clearTimeout(timer)
      })
  }

  // ---- subscribers --------------------------------------------------------

  subscribe(connection: HubConnection, topic: RealtimeTopic): void {
    this.#connections.set(connection.id, connection)
    const resolved = resolveTopic(this.#manifest, topic)
    if ('reason' in resolved) {
      connection.send([
        'subscribe-error',
        { topic: describe(topic), reason: resolved.reason },
      ])
      return
    }
    const { spec, id } = resolved

    const owned = this.#connectionTopics.get(connection.id) ?? new Set<string>()
    // a resubscribe after reconnect is not a new subscription
    if (!owned.has(id)) {
      if (owned.size >= this.#limits.maxTopicsPerConnection) {
        connection.send([
          'subscribe-error',
          { topic: id, reason: 'connection is at its subscription limit' },
        ])
        return
      }
      if (
        !this.#subscribers.has(id) &&
        this.#subscribers.size >= this.#limits.maxTopicsPerNamespace
      ) {
        connection.send([
          'subscribe-error',
          { topic: id, reason: 'namespace is at its active topic limit' },
        ])
        return
      }
      const existing = this.#subscribers.get(id)
      if (existing && existing.size >= this.#limits.maxSubscribersPerTopic) {
        connection.send([
          'subscribe-error',
          { topic: id, reason: 'topic is at its subscriber limit' },
        ])
        return
      }
    }

    const authorization = this.#authorize(connection.identity, topic, spec)
    if (authorization.status === 'denied') {
      connection.send(['subscribe-error', { topic: id, reason: authorization.reason }])
      return
    }

    owned.add(id)
    this.#connectionTopics.set(connection.id, owned)
    if (authorization.status === 'active') {
      let subscribers = this.#subscribers.get(id)
      if (!subscribers) {
        subscribers = new Set()
        this.#subscribers.set(id, subscribers)
      }
      subscribers.add(connection)
    }

    connection.send(['subscribed', { topic: id, status: authorization.status }])

    // catch a new subscriber up on a generation already in flight. This is what
    // lets append-mode frames be safe for late joiners, and it is sent
    // immediately rather than batched so a mid-stream mount paints at once.
    const generation = this.#generations.get(id)
    if (authorization.status === 'active' && generation && !generation.superseded) {
      connection.send([
        'field',
        {
          updates: [
            {
              topic: id,
              streamID: generation.streamID,
              seq: generation.seq,
              op: 'snapshot',
              value: generation.value,
            },
          ],
        },
      ])
    }
  }

  unsubscribe(connection: HubConnection, topic: RealtimeTopic): void {
    const resolved = resolveTopic(this.#manifest, topic)
    if ('reason' in resolved) return
    this.#removeSubscription(connection.id, resolved.id)
  }

  dropConnection(connectionID: string): void {
    for (const topicID of this.#connectionTopics.get(connectionID) ?? []) {
      this.#subscribers.get(topicID)?.forEach((connection) => {
        if (connection.id === connectionID)
          this.#subscribers.get(topicID)!.delete(connection)
      })
      if (this.#subscribers.get(topicID)?.size === 0) this.#subscribers.delete(topicID)
    }
    this.#connectionTopics.delete(connectionID)
    this.#connections.delete(connectionID)
    this.#outbox.delete(connectionID)
  }

  #removeSubscription(connectionID: string, topicID: string): void {
    this.#connectionTopics.get(connectionID)?.delete(topicID)
    const subscribers = this.#subscribers.get(topicID)
    if (!subscribers) return
    for (const connection of subscribers) {
      if (connection.id === connectionID) subscribers.delete(connection)
    }
    if (subscribers.size === 0) this.#subscribers.delete(topicID)
  }

  // A pull removed rows from a client group's query membership. The client is
  // about to learn the row left its query, so its field subscriptions for those
  // rows go with it, in the same update rather than on a later sweep.
  revokeMembership(clientGroupID: string, removed: readonly RealtimeTopic[]): void {
    for (const topic of removed) {
      const resolved = resolveTopic(this.#manifest, topic)
      if ('reason' in resolved) continue
      const subscribers = this.#subscribers.get(resolved.id)
      if (!subscribers) continue
      for (const connection of [...subscribers]) {
        if (connection.identity.clientGroupID !== clientGroupID) continue
        this.#removeSubscription(connection.id, resolved.id)
        connection.send([
          'subscribe-error',
          { topic: resolved.id, reason: 'row left your authorized query membership' },
        ])
      }
    }
  }

  // Every row a client group can currently subscribe to left its membership.
  // Cheaper than enumerating rows when a whole query is dropped.
  revokeAllForGroup(clientGroupID: string): void {
    for (const [topicID, subscribers] of [...this.#subscribers]) {
      for (const connection of [...subscribers]) {
        if (connection.identity.clientGroupID !== clientGroupID) continue
        this.#removeSubscription(connection.id, topicID)
        connection.send([
          'subscribe-error',
          { topic: topicID, reason: 'row left your authorized query membership' },
        ])
      }
    }
  }

  // ---- producers ----------------------------------------------------------

  // Open a generation. A newer generation always supersedes an older one for
  // the same topic, matching how an LLM retry replaces its predecessor. The
  // application still owns final-write fencing in the database; this only
  // decides who may deliver display values.
  beginGeneration(
    producer: HubProducer,
    topic: RealtimeTopic,
    streamID: string
  ):
    | { readonly ok: true; readonly topicID: string }
    | { readonly ok: false; readonly reason: string } {
    const resolved = resolveTopic(this.#manifest, topic)
    if ('reason' in resolved) return { ok: false, reason: resolved.reason }
    const { spec, id } = resolved

    const previous = this.#generations.get(id)
    if (previous && previous.streamID !== streamID) {
      previous.superseded = true
      if (previous.producer) {
        this.#producerGenerations.delete(previous.producer.id)
        previous.producer.send(['superseded', { topic: id, streamID: previous.streamID }])
      }
    }

    this.#generations.set(id, {
      topicID: id,
      spec,
      streamID,
      seq: -1,
      value: spec.mode === 'append' ? '' : null,
      producer,
      superseded: false,
    })
    this.#producerGenerations.set(producer.id, id)
    return { ok: true, topicID: id }
  }

  // Accept one update from a producer, accumulate it, and fan it out. Returns
  // false when the frame was rejected, which the adapter turns into a producer
  // error rather than silently dropping.
  publish(producer: HubProducer, update: FieldUpdate): boolean {
    const generation = this.#generations.get(update.topic)
    if (!generation) return false
    // Only the unsuperseded producer holding this generation may publish. A
    // subscriber socket is never a producer, so a leaked streamID grants
    // nothing: the check is on the producer handle, not the frame contents.
    if (generation.superseded) return false
    if (generation.producer?.id !== producer.id) return false
    if (generation.streamID !== update.streamID) return false
    if (update.seq <= generation.seq) return false

    switch (update.op) {
      case 'snapshot': {
        if (!this.#validate(generation.spec, update.value)) return false
        generation.value = update.value
        break
      }
      case 'append': {
        if (generation.spec.mode !== 'append') return false
        const next = (generation.value as string) + update.text
        if (byteLength(next) > generation.spec.maxBytes) return false
        generation.value = next
        break
      }
      case 'end':
      case 'abort':
        break
    }
    generation.seq = update.seq
    this.#fanOut(update)

    if (update.op === 'end' || update.op === 'abort') {
      this.#generations.delete(update.topic)
      this.#producerGenerations.delete(producer.id)
    }
    return true
  }

  // A producer socket closed without a terminal frame (a crash, or a lost
  // connection). The generation's lease is released so the next `begin` is
  // clean; subscribers fall back to their inactivity deadline, which reveals
  // the durable value rather than inventing a recovery value here.
  dropProducer(producerID: string): void {
    const topicID = this.#producerGenerations.get(producerID)
    this.#producerGenerations.delete(producerID)
    if (!topicID) return
    const generation = this.#generations.get(topicID)
    if (generation?.producer?.id === producerID) this.#generations.delete(topicID)
  }

  #validate(spec: StreamingFieldSpec, value: unknown): boolean {
    if (spec.mode === 'append' && typeof value !== 'string') return false
    if (spec.validate && !spec.validate(value)) return false
    return byteLength(canonicalEncode(value)) <= spec.maxBytes
  }

  #fanOut(update: FieldUpdate): void {
    const subscribers = this.#subscribers.get(update.topic)
    if (!subscribers || subscribers.size === 0) return
    for (const connection of subscribers) {
      const batch = this.#outbox.get(connection.id)
      if (batch) batch.push(update)
      else this.#outbox.set(connection.id, [update])
    }
    if (!this.#cancelFlush) {
      this.#cancelFlush = this.#schedule(() => this.flush(), this.#limits.batchWindowMs)
    }
  }

  // Deliver every pending batch. Public so an adapter can force a flush before
  // it lets its runtime go idle.
  flush(): void {
    this.#cancelFlush = undefined
    if (this.#outbox.size === 0) return
    const outbox = [...this.#outbox]
    this.#outbox.clear()
    let requeued = false
    for (const [connectionID, updates] of outbox) {
      const connection = this.#connections.get(connectionID)
      if (!connection) continue
      // Stop a batch before it exceeds the frame ceiling; the rest goes in the
      // next frame rather than being dropped or sent as one oversized frame.
      let bytes = 0
      let cut = updates.length
      for (let index = 0; index < updates.length; index++) {
        bytes += updateBytes(updates[index]!)
        if (bytes > this.#limits.maxBatchBytes && index > 0) {
          cut = index
          break
        }
      }
      connection.send(['field', { updates: updates.slice(0, cut) }])
      if (cut < updates.length) {
        this.#outbox.set(connectionID, updates.slice(cut))
        requeued = true
      }
    }
    if (requeued && !this.#cancelFlush) {
      this.#cancelFlush = this.#schedule(() => this.flush(), this.#limits.batchWindowMs)
    }
  }

  // ---- introspection for adapters and tests -------------------------------

  get activeTopics(): number {
    return this.#generations.size
  }

  get subscribedTopics(): number {
    return this.#subscribers.size
  }

  generationFor(
    topicID: string
  ): { readonly streamID: string; readonly seq: number } | undefined {
    const generation = this.#generations.get(topicID)
    return generation ? { streamID: generation.streamID, seq: generation.seq } : undefined
  }
}

function describe(topic: RealtimeTopic): string {
  return `${topic.table}.${topic.field}`
}

function updateBytes(update: FieldUpdate): number {
  if (update.op === 'append') return byteLength(update.text) + 96
  if (update.op === 'snapshot') return byteLength(canonicalEncode(update.value)) + 96
  return 96
}

function byteLength(value: string): number {
  let bytes = 0
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index)
    if (code < 0x80) bytes += 1
    else if (code < 0x800) bytes += 2
    else if (code >= 0xd800 && code < 0xdc00) {
      bytes += 4
      index++
    } else bytes += 3
  }
  return bytes
}
