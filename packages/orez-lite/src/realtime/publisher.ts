// The producer side of a streaming field.
//
// A producer opens one generation per field, pushes values, and finishes by
// running the application's authoritative durable write. Orez never performs
// that write: in a delegated-push deployment the application worker is the
// write authority, so the commit is a callback the application supplies.
//
// The transport is injected. Trusted server code gets an in-process publisher
// on the native and local hosts; on Cloudflare the same interface is backed by
// one private service-bound producer WebSocket per generation.

import { canonicalTopic } from './protocol.js'

import type { StreamingFieldSpec, StreamingManifest } from './manifest.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'

export type PublisherTransport = {
  // deliver one update to the host; ordering within a generation is the
  // transport's responsibility (one socket, or a direct in-process call)
  readonly publish: (update: FieldUpdate) => Promise<void> | void
  // open a generation, returning nothing on success and throwing if the host
  // refuses (unknown topic, unauthorized publisher, namespace at capacity)
  readonly begin: (topic: RealtimeTopic, streamID: string) => Promise<void> | void
  readonly end: (streamID: string) => Promise<void> | void
}

export type BeginOptions = {
  readonly namespace: string
  readonly key: Readonly<Record<string, boolean | null | number | string>>
}

export type StreamSession<Value> = {
  readonly streamID: string
  readonly topic: RealtimeTopic
  // replace the field's current value. In append mode the session ships only
  // the new suffix, so a growing text value costs new bytes rather than total
  // bytes per frame.
  set(value: Value): void
  // flush, run the application's durable write, then close the generation.
  finish(value: Value, commit: () => Promise<void>): Promise<void>
  abort(): Promise<void>
}

// Coalescing state for one generation. `set` is synchronous and cheap so a
// token loop never awaits; the scheduler decides when bytes actually go out.
type Pending<Value> = {
  value: Value
  // bytes already delivered for this generation in the current second-window
  windowBytes: number
  windowUpdates: number
  windowStartedAt: number
  timer: ReturnType<typeof setTimeout> | undefined
  flushing: boolean
}

export class RealtimePublisher {
  readonly #transport: PublisherTransport
  readonly #manifest: StreamingManifest
  readonly #now: () => number
  readonly #randomID: () => string

  constructor(
    transport: PublisherTransport,
    manifest: StreamingManifest,
    options: { readonly now?: () => number; readonly randomID?: () => string } = {}
  ) {
    this.#transport = transport
    this.#manifest = manifest
    this.#now = options.now ?? (() => Date.now())
    this.#randomID = options.randomID ?? (() => crypto.randomUUID())
  }

  async begin<Value = string>(
    table: string,
    field: string,
    options: BeginOptions
  ): Promise<StreamSession<Value>> {
    const spec = this.#manifest.fields.get(`${table}.${field}`)
    if (!spec) {
      throw new TypeError(`'${table}.${field}' is not a streaming field in this manifest`)
    }
    const topic: RealtimeTopic = { table, key: options.key, field }
    for (const column of spec.primaryKey) {
      if (options.key[column] === undefined) {
        throw new TypeError(
          `streaming topic '${table}.${field}' is missing primary key column '${column}'`
        )
      }
    }

    const streamID = this.#randomID()
    await this.#transport.begin(topic, streamID)
    return new Session<Value>(
      this.#transport,
      spec,
      topic,
      canonicalTopic(spec.primaryKey, topic),
      streamID,
      this.#now
    )
  }
}

class Session<Value> implements StreamSession<Value> {
  readonly streamID: string
  readonly topic: RealtimeTopic

  readonly #transport: PublisherTransport
  readonly #spec: StreamingFieldSpec
  readonly #id: string
  readonly #now: () => number

  #seq = 0
  // what the subscribers have been told, so append mode can ship the suffix
  #delivered = ''
  #started = false
  #closed = false
  #pending: Pending<Value>
  #inFlight: Promise<void> = Promise.resolve()
  // A background flush cannot reject into the caller's stack, so a transport
  // failure (most often: this generation was superseded by a retry) is held
  // here and raised on the next set/finish. Without this the failure would
  // surface as an unhandled rejection and the producer would keep writing into
  // a stream nobody accepts.
  #failure: Error | undefined

  constructor(
    transport: PublisherTransport,
    spec: StreamingFieldSpec,
    topic: RealtimeTopic,
    id: string,
    streamID: string,
    now: () => number
  ) {
    this.#transport = transport
    this.#spec = spec
    this.topic = topic
    this.#id = id
    this.streamID = streamID
    this.#now = now
    this.#pending = {
      value: undefined as Value,
      windowBytes: 0,
      windowUpdates: 0,
      windowStartedAt: now(),
      timer: undefined,
      flushing: false,
    }
  }

  set(value: Value): void {
    this.#throwIfUnusable()
    this.#validate(value)
    this.#pending.value = value
    this.#schedule()
  }

  async finish(value: Value, commit: () => Promise<void>): Promise<void> {
    this.#throwIfUnusable()
    this.#validate(value)
    this.#pending.value = value
    this.#cancelTimer()

    // 1. flush the final value so subscribers see it before the commit round-trip
    await this.#flush(true)
    // 2. the application's authoritative durable write. Orez cannot do this
    //    generically: in a delegated-push deployment the application worker is
    //    the write authority.
    await commit()
    // 3. `end` puts every subscriber in `committing`, holding the final overlay
    //    until their own Zero query produces the committed value. No waiting on
    //    replication here: the client's durable-equality check is what ends the
    //    overlay, and blocking the generation on replica observation would
    //    couple producer lifetime to pull lag for no extra safety.
    this.#closed = true
    await this.#emit({
      topic: this.#id,
      streamID: this.streamID,
      seq: ++this.#seq,
      op: 'end',
    })
    await this.#transport.end(this.streamID)
  }

  async abort(): Promise<void> {
    if (this.#closed) return
    this.#closed = true
    this.#cancelTimer()
    await this.#emit({
      topic: this.#id,
      streamID: this.streamID,
      seq: ++this.#seq,
      op: 'abort',
    })
    await this.#transport.end(this.streamID)
  }

  #throwIfUnusable(): void {
    if (this.#failure) throw this.#failure
    if (this.#closed) throw new Error('streaming field generation is already closed')
  }

  #validate(value: Value): void {
    if (this.#spec.mode === 'append' && typeof value !== 'string') {
      throw new TypeError(
        `streaming field '${this.#spec.table}.${this.#spec.field}' is append mode and takes a string`
      )
    }
    if (this.#spec.validate && !this.#spec.validate(value)) {
      throw new TypeError(
        `streaming field '${this.#spec.table}.${this.#spec.field}' rejected a value in validate()`
      )
    }
    const bytes = byteLength(
      typeof value === 'string' ? value : JSON.stringify(value ?? null)
    )
    if (bytes > this.#spec.maxBytes) {
      throw new RangeError(
        `streaming field '${this.#spec.table}.${this.#spec.field}' value is ${bytes} bytes, over its ${this.#spec.maxBytes} maxBytes`
      )
    }
    if (this.#spec.mode === 'append' && this.#started) {
      // append ships suffixes, so the value must only ever grow. A producer that
      // rewrites history needs `mode: 'replace'`; silently shipping a bogus
      // suffix would leave every subscriber with a corrupted value.
      const next = value as unknown as string
      if (!next.startsWith(this.#delivered)) {
        throw new TypeError(
          `streaming field '${this.#spec.table}.${this.#spec.field}' is append mode and its value stopped being an extension of what was already sent`
        )
      }
    }
  }

  #schedule(): void {
    if (this.#pending.timer || this.#pending.flushing || this.#failure) return
    const delay = this.#delayUntilAllowed()
    if (delay === 0) {
      void this.#flush(false).catch((error: unknown) => this.#fail(error))
      return
    }
    this.#pending.timer = setTimeout(() => {
      this.#pending.timer = undefined
      void this.#flush(false).catch((error: unknown) => this.#fail(error))
    }, delay)
  }

  // How long before another update may go out, honouring BOTH manifest bounds.
  // In append mode the byte budget is charged against new bytes only, which is
  // what keeps a long generation's update rate flat instead of degrading as the
  // accumulated value grows.
  #delayUntilAllowed(): number {
    const now = this.#now()
    const elapsed = now - this.#pending.windowStartedAt
    if (elapsed >= 1000) return 0

    const newBytes = this.#pendingBytes()
    const overUpdates = this.#pending.windowUpdates >= this.#spec.maxUpdatesPerSecond
    const overBytes = this.#pending.windowBytes + newBytes > this.#spec.maxBytesPerSecond
    if (!overUpdates && !overBytes) {
      // spread updates evenly rather than bursting then starving
      const minGap = Math.floor(1000 / this.#spec.maxUpdatesPerSecond)
      const sinceWindowStart = elapsed
      const expected = this.#pending.windowUpdates * minGap
      return Math.max(0, expected - sinceWindowStart)
    }
    return 1000 - elapsed
  }

  #pendingBytes(): number {
    const value = this.#pending.value
    if (this.#spec.mode === 'append') {
      const text = (value as unknown as string) ?? ''
      return byteLength(text.slice(this.#delivered.length))
    }
    return byteLength(JSON.stringify(value ?? null))
  }

  async #flush(final: boolean): Promise<void> {
    const value = this.#pending.value
    if (value === undefined) return
    this.#pending.flushing = true
    try {
      const now = this.#now()
      if (now - this.#pending.windowStartedAt >= 1000) {
        this.#pending.windowStartedAt = now
        this.#pending.windowBytes = 0
        this.#pending.windowUpdates = 0
      }

      let update: FieldUpdate
      if (this.#spec.mode === 'append' && this.#started) {
        const text = (value as unknown as string).slice(this.#delivered.length)
        if (!text && !final) return
        update = {
          topic: this.#id,
          streamID: this.streamID,
          seq: ++this.#seq,
          op: 'append',
          text,
        }
        this.#delivered = value as unknown as string
      } else {
        update = {
          topic: this.#id,
          streamID: this.streamID,
          seq: ++this.#seq,
          op: 'snapshot',
          value,
        }
        this.#started = true
        this.#delivered = this.#spec.mode === 'append' ? (value as unknown as string) : ''
      }

      this.#pending.windowBytes += this.#pendingBytes()
      this.#pending.windowUpdates++
      await this.#emit(update)
    } finally {
      this.#pending.flushing = false
    }
    // a value that arrived while this flush was in flight still needs sending
    if (!final && this.#pendingHasNewBytes()) this.#schedule()
  }

  #pendingHasNewBytes(): boolean {
    if (this.#spec.mode !== 'append') return false
    const text = (this.#pending.value as unknown as string) ?? ''
    return text.length > this.#delivered.length
  }

  #cancelTimer(): void {
    if (this.#pending.timer) {
      clearTimeout(this.#pending.timer)
      this.#pending.timer = undefined
    }
  }

  // Serialize every emit for the generation. WebSocket ordering owns frame
  // order on the wire, but two overlapping flushes could otherwise hand the
  // transport its frames out of sequence.
  #emit(update: FieldUpdate): Promise<void> {
    this.#inFlight = this.#inFlight.then(() => this.#transport.publish(update))
    return this.#inFlight
  }

  // Record a background failure and stop the generation. `finish` and `abort`
  // await their emits directly, so they still reject into the caller; only the
  // scheduled flushes route through here.
  #fail(error: unknown): void {
    this.#failure = error instanceof Error ? error : new Error(String(error))
    this.#closed = true
    this.#cancelTimer()
  }
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
