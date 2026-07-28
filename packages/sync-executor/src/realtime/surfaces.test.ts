// The cross-surface contract.
//
// The promise this engine makes is that a producer is written once and runs
// anywhere: in the tab that renders it, in a worker, on an application server
// reaching the hub over a socket. That promise is only worth something if it is
// enforced, because the surfaces are separate files and nothing else stops one
// from drifting.
//
// So the producer body below is written ONCE and replayed against every
// surface. A surface that renames `fields`, forgets to expose it, or delivers a
// different value fails here rather than in whichever application adopts it
// next. This is exactly the drift that had already happened once: the browser
// worker exposed `publisher` but no `fields`, so a loop written against the
// local surface could not run on it.

import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, it } from 'vitest'

import { applyProducerFrame } from './host.js'
import { RealtimeHub } from './hub.js'
import { createLocalRealtime } from './local.js'
import { defineStreamingFields } from './manifest.js'
import { BrowserRealtime } from './message-port.js'
import { createSocketProducer } from './producer-socket.js'
import { decodeFrame } from './protocol.js'

import type { HubConnection, HubProducer } from './hub.js'
import type { RealtimeProducer } from './producer.js'
import type { FieldUpdate, ProducerFrame } from './protocol.js'

const message = table('message')
  .columns({ id: string(), content: string() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 100_000, maxUpdatesPerSecond: 60, maxBytesPerSecond: 500_000 },
  },
})

const handle = streaming.message.content({ id: 'm1' })
const identity = { userID: 'u1', clientID: 'c1', clientGroupID: 'g1' }

// The application's producer code. It knows only about the shared producer
// role, so whatever surface it is handed must satisfy the same shape.
async function runProducer(producer: RealtimeProducer): Promise<void> {
  let content = ''
  for (const token of ['Streaming ', 'the ', 'same ', 'way']) {
    content += token
    producer.fields.set(handle, content)
  }
  await producer.fields.flush(handle)
}

// Each surface, reduced to "give me a producer, and tell me what a subscriber
// of that row ended up seeing".
const surfaces: Array<{
  name: string
  build: () => { producer: RealtimeProducer; read: () => unknown }
}> = [
  {
    name: 'local (producer and subscriber in one context)',
    build: () => {
      const realtime = createLocalRealtime({ manifest: streaming.manifest })
      realtime.store.subscribe(handle, () => {})
      return { producer: realtime, read: () => realtime.store.read(handle, '').value }
    },
  },
  {
    name: 'browser worker (producer in the worker, hub in the worker)',
    build: () => {
      const realtime = new BrowserRealtime({
        manifest: streaming.manifest,
        readMembership: async () => ({ ownsGroup: true, authorized: true }),
      })
      const seen: FieldUpdate[] = []
      const connection: HubConnection = {
        id: 'worker-subscriber',
        identity,
        send: (frame) => {
          const [kind, body] = frame as [string, { updates?: FieldUpdate[] }]
          if (kind === 'field') seen.push(...(body.updates ?? []))
        },
      }
      void realtime.hub.subscribe(connection, handle.topic)
      return {
        producer: realtime,
        read: () => {
          realtime.flush()
          return accumulate(seen)
        },
      }
    },
  },
  {
    name: 'socket (producer off-host, reaching the hub over a wire)',
    build: () => {
      const hub = new RealtimeHub({
        manifest: streaming.manifest,
        authorizeSubscribe: () => ({ status: 'active' as const }),
        scheduleFlush: (flush) => flush(),
      })
      const seen: FieldUpdate[] = []
      const connection: HubConnection = {
        id: 'socket-subscriber',
        identity,
        send: (frame) => {
          const [kind, body] = frame as [string, { updates?: FieldUpdate[] }]
          if (kind === 'field') seen.push(...(body.updates ?? []))
        },
      }
      void hub.subscribe(connection, handle.topic)

      const producerHandle: HubProducer = {
        id: 'off-host',
        send: (frame) => remote.handleMessage(JSON.stringify(frame)),
      }
      const remote = createSocketProducer(
        {
          send: (raw) => {
            const frame = decodeFrame(raw)
            if (frame) applyProducerFrame(hub, producerHandle, frame as ProducerFrame)
          },
        },
        { manifest: streaming.manifest }
      )
      return { producer: remote, read: () => accumulate(seen) }
    },
  },
]

// Replay the wire the way a client's store does, so each surface is compared on
// the value a subscriber actually ends up with rather than on frame shapes.
function accumulate(updates: readonly FieldUpdate[]): string {
  let value = ''
  for (const update of updates) {
    if (update.op === 'snapshot') value = String(update.value)
    else if (update.op === 'append') value += update.text
  }
  return value
}

describe('every surface honours the same producer contract', () => {
  for (const surface of surfaces) {
    it(`delivers the same value: ${surface.name}`, async () => {
      const { producer, read } = surface.build()
      await runProducer(producer)
      expect(read()).toBe('Streaming the same way')
    })

    it(`exposes both producer handles: ${surface.name}`, () => {
      const { producer } = surface.build()
      // the imperative handle a token loop uses, and the explicit generation
      // API underneath it. A surface missing either cannot host code written
      // against another.
      expect(typeof producer.fields.set).toBe('function')
      expect(typeof producer.publisher.begin).toBe('function')
    })
  }
})
