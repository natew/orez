// Single-client realtime: a hub, a publisher, and a store wired together in
// one JavaScript context, with no socket, no host, and no authorization.
//
// This is the shape an application needs when the thing producing the values
// and the thing displaying them are the same client. Soot's agent loop runs in
// the browser: the tab running the agent is the tab rendering its output, so
// routing those values through a server and back would add latency and an auth
// surface to reach the component sitting next to the producer.
//
// It is the same RealtimeHub every other host runs, so a stream behaves
// identically here and over a socket: same generations, same append
// accumulation, same durable handoff, same manifest bounds. Moving an
// application from local to a shared host changes which realtime it constructs
// and nothing about its components or its producer code.
//
// What it deliberately does NOT do is reach other clients. Values live in this
// context only. The durable Zero row remains the cross-client story, which is
// exactly the guarantee the overlay already makes: a reload, another tab, or
// another user sees the committed value, never the in-flight one.

import { RealtimeHub } from './hub.js'
import { RealtimePublisher } from './publisher.js'
import { RealtimeStore } from './store.js'
import { FieldWriter } from './writer.js'

import type { HubConnection, HubProducer } from './hub.js'
import type { StreamingManifest } from './manifest.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

export type LocalRealtimeOptions = {
  readonly manifest: StreamingManifest
  // how long without a frame before an overlay reveals the durable value
  readonly staleAfterMs?: number
  readonly onError?: (message: string) => void
}

export type LocalRealtime = {
  // hand to createUseStreamingField / createUseStreamingFields
  readonly store: RealtimeStore
  // imperative writing: set a row's field to its current value, generations
  // managed for you. This is what most producer loops want.
  readonly fields: FieldWriter
  // the explicit generation API underneath, when a producer needs to control
  // begin, commit, and end itself
  readonly publisher: RealtimePublisher
  // Deliver whatever the batching window has ready. Called automatically on a
  // timer; exposed so a test can be deterministic without faking timers.
  readonly flush: () => void
  readonly close: () => void
}

export function createLocalRealtime(options: LocalRealtimeOptions): LocalRealtime {
  // Every row is authorized: there is one client, it is the producer, and it
  // already holds the data it is about to display. There is nothing to
  // authorize against and nobody to withhold it from.
  const hub = new RealtimeHub({
    manifest: options.manifest,
    authorizeSubscribe: () => ({ status: 'active' }),
    // No batching: the window exists to collapse several updates into one
    // WebSocket frame, and there is no socket here. Deferring an in-process
    // call would only add a tick of latency between the producer and the
    // component next to it.
    scheduleFlush: (flush) => flush(),
  })

  const store = new RealtimeStore({
    send: (frame) => {
      const [kind, body] = frame as [string, { topic: RealtimeTopic }]
      if (kind === 'subscribe') void hub.subscribe(connection, body.topic)
      else if (kind === 'unsubscribe') void hub.unsubscribe(connection, body.topic)
    },
    staleAfterMs: options.staleAfterMs,
    onError: options.onError,
  })

  const connection: HubConnection = {
    id: 'local',
    identity: { userID: 'local', clientID: 'local', clientGroupID: 'local' },
    // Frames are handed straight to the store. They are not serialized: this
    // is one context, and a JSON round trip per token would be pure cost.
    send: (frame) => {
      const [kind, body] = frame as [string, Record<string, unknown>]
      if (kind === 'field') {
        store.applyUpdates(body.updates as readonly FieldUpdate[])
      } else if (kind === 'subscribed') {
        store.handleSubscribed(body.topic as string, body.status as 'active' | 'pending')
      } else if (kind === 'subscribe-error') {
        store.handleSubscribeError(body.topic as string, body.reason as string)
      }
    },
  }

  const producer: HubProducer = { id: 'local-producer', send: () => {} }
  const transport: PublisherTransport = {
    begin: (topic, streamID) => {
      const result = hub.beginGeneration(producer, topic, streamID)
      if (!result.ok) throw new Error(result.reason)
    },
    publish: (update: FieldUpdate) => {
      if (!hub.publish(producer, update)) {
        throw new Error(
          `realtime hub refused a ${update.op} frame for ${update.topic}: the generation was superseded, or the value failed its manifest bounds`
        )
      }
    },
    end: () => {},
  }

  const publisher = new RealtimePublisher(transport, options.manifest)
  return {
    store,
    publisher,
    fields: new FieldWriter(publisher, {
      onError: (error, topic) =>
        options.onError?.(
          `realtime write failed for ${topic.table}.${topic.field}: ${error.message}`
        ),
    }),
    flush: () => hub.flush(),
    close: () => {
      hub.dropProducer(producer.id)
      hub.dropConnection(connection.id)
    },
  }
}
