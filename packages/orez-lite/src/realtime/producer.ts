// The producer role, identical on every surface.
//
// A producer is whatever generates values: a browser agent loop, an application
// server, a Rust daemon. What differs between them is only how their frames
// reach the hub (a direct call, a MessagePort, a WebSocket). What must NOT
// differ is the API they write through, because otherwise moving a producer
// between surfaces means rewriting it.
//
// So the transport varies and this does not. Every surface builds its producer
// here, and every producer gets the same two handles:
//
//   fields     imperative: set(handle, value), generations managed for you.
//              This is what a token loop wants and what most code should use.
//   publisher  the explicit generation API underneath, for a producer that
//              needs to control begin, commit, and end itself.

import { RealtimePublisher } from './publisher.js'
import { FieldWriter } from './writer.js'

import type { RealtimeHub, HubProducer } from './hub.js'
import type { StreamingManifest } from './manifest.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

export type RealtimeProducer = {
  readonly fields: FieldWriter
  readonly publisher: RealtimePublisher
}

export type ProducerOptions = {
  readonly manifest: StreamingManifest
  // A failed write is reported, never thrown into the caller's loop: `set` runs
  // inside a token loop where throwing would abort the model run over a
  // presentation concern. The durable write is what matters and is unaffected.
  readonly onError?: (message: string) => void
}

export function createProducer(
  transport: PublisherTransport,
  options: ProducerOptions
): RealtimeProducer {
  const publisher = new RealtimePublisher(transport, options.manifest)
  const fields = new FieldWriter(publisher, {
    onError: (error, topic) => {
      options.onError?.(`${topic.table}.${topic.field}: ${error.message}`)
    },
  })
  return { fields, publisher }
}

// The transport for a producer that shares a process with the hub: the local
// single-client host and the browser worker. `begin` and `publish` are direct
// calls, so there is nothing to serialize and nothing to await.
//
// A rejection throws, which is what the publisher's own failure handling
// expects; it converts that into a reported error rather than letting it reach
// the producer's loop.
export function inProcessTransport(
  hub: RealtimeHub,
  producer: HubProducer
): PublisherTransport {
  return {
    begin: (topic: RealtimeTopic, streamID: string) => {
      const result = hub.beginGeneration(producer, topic, streamID)
      if (!result.ok) throw new Error(result.reason)
    },
    publish: (update: FieldUpdate) => {
      const result = hub.publish(producer, update)
      if (!result.ok) throw new Error(result.reason)
    },
    // Nothing to release: the generation closed when its terminal update was
    // published, and there is no socket whose bookkeeping could outlive it.
    end: () => {},
  }
}
