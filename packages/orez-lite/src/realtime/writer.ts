// Imperative field writing, for producers that think in "here is the current
// value of this row's field" rather than in stream generations.
//
// The session API underneath is explicit on purpose: a generation has a
// beginning, a commit, and an end, and a cross-client producer needs to control
// all three. But most producer code is a loop that already holds the whole
// current value and just wants it on screen. Soot's agent loop is exactly this
// shape (`setStreamingMessageText(id, text)` on every model event), and making
// it open, track, and close a session per message would be bookkeeping the
// writer can do instead.
//
// Generations are opened lazily on the first `set` for a topic and closed by
// `finish` or `abort`. A `set` after a finish opens a NEW generation, which is
// the same rule the hub applies: a later generation supersedes an earlier one.

import { canonicalTopic } from './protocol.js'

import type { StreamingManifest } from './manifest.js'
import type { RealtimeTopic } from './protocol.js'
import type { RealtimePublisher, StreamSession } from './publisher.js'

type Entry = {
  session: StreamSession<unknown> | undefined
  // the value that arrived while `begin` was still in flight
  queued: unknown
  opening: Promise<void> | undefined
  closed: boolean
}

export type FieldWriterOptions = {
  // A failed write is reported rather than thrown: `set` is called from inside
  // a token loop where throwing would abort the model run over a presentation
  // concern. The durable write is what matters, and it is unaffected.
  readonly onError?: (error: Error, topic: RealtimeTopic) => void
}

export class FieldWriter {
  readonly #publisher: RealtimePublisher
  readonly #manifest: StreamingManifest
  readonly #onError: (error: Error, topic: RealtimeTopic) => void
  readonly #entries = new Map<string, Entry>()

  constructor(
    publisher: RealtimePublisher,
    manifest: StreamingManifest,
    options: FieldWriterOptions = {}
  ) {
    this.#publisher = publisher
    this.#manifest = manifest
    this.#onError =
      options.onError ??
      ((error, topic) => {
        console.error(
          `orez realtime write failed for ${topic.table}.${topic.field}`,
          error
        )
      })
  }

  #id(topic: RealtimeTopic): string {
    const spec = this.#manifest.fields.get(`${topic.table}.${topic.field}`)
    if (!spec) {
      throw new TypeError(
        `'${topic.table}.${topic.field}' is not a streaming field in this manifest`
      )
    }
    return canonicalTopic(spec.primaryKey, topic)
  }

  // Publish the field's current value. Synchronous and safe to call on every
  // token: the first call opens a generation in the background, later calls
  // reach the open session directly.
  set(topic: RealtimeTopic, value: unknown): void {
    const id = this.#id(topic)
    const existing = this.#entries.get(id)

    if (existing && !existing.closed) {
      if (existing.session) {
        try {
          existing.session.set(value)
        } catch (error) {
          this.#fail(id, error, topic)
        }
      } else {
        existing.queued = value
      }
      return
    }

    const entry: Entry = {
      session: undefined,
      queued: value,
      opening: undefined,
      closed: false,
    }
    this.#entries.set(id, entry)
    entry.opening = this.#publisher
      .begin(topic.table, topic.field, { namespace: '', key: topic.key })
      .then((session) => {
        // the writer may have been closed while begin was in flight
        if (this.#entries.get(id) !== entry) {
          void session.abort()
          return
        }
        entry.session = session as StreamSession<unknown>
        if (entry.queued !== undefined) session.set(entry.queued as never)
      })
      .catch((error: unknown) => this.#fail(id, error, topic))
  }

  // Deliver pending values for one topic, honouring its rate bounds.
  async flush(topic: RealtimeTopic): Promise<void> {
    const entry = this.#entries.get(this.#id(topic))
    if (!entry) return
    await entry.opening
    await entry.session?.flush()
  }

  // Close the generation: flush the final value, run the application's durable
  // write, then tell subscribers to hold the overlay until Zero produces it.
  async finish(
    topic: RealtimeTopic,
    value: unknown,
    commit: () => Promise<void>
  ): Promise<void> {
    const id = this.#id(topic)
    const entry = this.#entries.get(id)
    if (!entry || entry.closed) {
      // nothing was ever streamed for this row; the durable write still has to
      // happen, and it is the only thing that matters
      await commit()
      return
    }
    entry.closed = true
    await entry.opening
    this.#entries.delete(id)
    if (!entry.session) {
      await commit()
      return
    }
    await entry.session.finish(value as never, commit)
  }

  // Drop the overlay now and reveal the durable row. Used when a run is
  // cancelled or errors: there is no value worth showing.
  async abort(topic: RealtimeTopic): Promise<void> {
    const id = this.#id(topic)
    const entry = this.#entries.get(id)
    if (!entry || entry.closed) return
    entry.closed = true
    this.#entries.delete(id)
    await entry.opening
    await entry.session?.abort()
  }

  isStreaming(topic: RealtimeTopic): boolean {
    const entry = this.#entries.get(this.#id(topic))
    return !!entry && !entry.closed
  }

  // Abort every open generation. For a producer shutting down: without it,
  // subscribers would wait out their inactivity deadline before revealing the
  // durable value.
  async abortAll(): Promise<void> {
    const entries = [...this.#entries.values()]
    this.#entries.clear()
    await Promise.all(
      entries.map(async (entry) => {
        if (entry.closed) return
        entry.closed = true
        await entry.opening
        await entry.session?.abort()
      })
    )
  }

  #fail(id: string, error: unknown, topic: RealtimeTopic): void {
    this.#entries.delete(id)
    this.#onError(error instanceof Error ? error : new Error(String(error)), topic)
  }
}
