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

import { applyClientFrame, applyHostFrame } from './host.js'
import { RealtimeHub } from './hub.js'
import { createProducer, inProcessTransport } from './producer.js'
import { RealtimeStore } from './store.js'

import type { HubConnection, HubProducer } from './hub.js'
import type { ProducerOptions, RealtimeProducer } from './producer.js'
import type { ClientFrame, HostFrame } from './protocol.js'

export type LocalRealtimeOptions = ProducerOptions & {
  // how long without a frame before an overlay reveals the durable value
  readonly staleAfterMs?: number
}

// The producer role is the shared one, so a loop written against a local
// realtime moves to a worker or a socket host unchanged.
export type LocalRealtime = RealtimeProducer & {
  // hand to createUseStreamingField / createUseStreamingFields
  readonly store: RealtimeStore
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
    send: (frame) => void applyClientFrame(hub, connection, frame as ClientFrame),
    staleAfterMs: options.staleAfterMs,
    onError: options.onError,
  })

  const connection: HubConnection = {
    id: 'local',
    identity: { userID: 'local', clientID: 'local', clientGroupID: 'local' },
    // Frames are handed straight to the store, not serialized: this is one
    // context, and a JSON round trip per token would be pure cost. The routing
    // is still the shared one, so a frame means the same thing here as it does
    // over a socket.
    send: (frame) => applyHostFrame(store, frame as HostFrame),
  }

  const producerHandle: HubProducer = { id: 'local-producer', send: () => {} }
  const producer = createProducer(inProcessTransport(hub, producerHandle), options)

  return {
    store,
    ...producer,
    flush: () => hub.flush(),
    close: () => {
      hub.dropProducer(producerHandle.id)
      hub.dropConnection(connection.id)
    },
  }
}
