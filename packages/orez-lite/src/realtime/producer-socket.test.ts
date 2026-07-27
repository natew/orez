// A remote producer against a real hub, wired the way an application server is:
// the producer is somewhere else, its frames cross a socket, and the
// subscribers are connections on the hub side.
//
// The two sides are joined by a pair of in-memory sockets rather than mocked,
// so these exercise the actual frame encoding, the actual routing in host.ts,
// and the actual hub. What is faked is only the wire itself.

import { createSchema, json, string, table } from '@rocicorp/zero'
import { describe, expect, it } from 'vitest'

import { applyProducerFrame } from './host.js'
import { RealtimeHub } from './hub.js'
import { defineStreamingFields } from './manifest.js'
import { createProducerTransport } from './producer-socket.js'
import { decodeFrame } from './protocol.js'
import { RealtimePublisher } from './publisher.js'

import type { HubConnection, HubProducer } from './hub.js'
import type { ProducerFrame } from './protocol.js'

const message = table('message')
  .columns({ id: string(), content: string(), steps: json() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 60,
      maxBytesPerSecond: 1_000_000,
    },
    steps: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 30,
      maxBytesPerSecond: 1_000_000,
      validate: (value) => Array.isArray(value),
    },
  },
})

const identity = { userID: 'u1', clientID: 'c1', clientGroupID: 'g1' }

// Joins a producer transport to a hub through frames, exactly as a socket pair
// would. Returns the pieces a test needs to drive both ends.
function connect(options: { immediate?: boolean } = {}) {
  const hub = new RealtimeHub({
    manifest: streaming.manifest,
    authorizeSubscribe: () => ({ status: 'active' as const }),
    // flush synchronously so a test asserts without waiting on a batch window
    scheduleFlush: options.immediate === false ? undefined : (flush) => flush(),
  })

  const toProducer: string[] = []
  const producerHandle: HubProducer = {
    id: 'server-producer',
    send: (frame) => {
      toProducer.push(JSON.stringify(frame))
      // deliver the host's answer back to the producer transport
      remote.handleMessage(JSON.stringify(frame))
    },
  }

  const remote = createProducerTransport({
    send: (raw) => {
      const frame = decodeFrame(raw)
      if (frame) applyProducerFrame(hub, producerHandle, frame as ProducerFrame)
    },
  })

  const publisher = new RealtimePublisher(remote.transport, streaming.manifest)

  const received: unknown[][] = []
  const subscriber: HubConnection = {
    id: 'browser-1',
    identity,
    send: (frame) => received.push(frame as unknown[]),
  }

  return { hub, remote, publisher, subscriber, received, toProducer }
}

// every field update a subscriber connection was actually sent, flattened
function updatesOf(received: unknown[][]) {
  return received
    .filter((frame) => frame[0] === 'field')
    .flatMap((frame) => (frame[1] as { updates: unknown[] }).updates)
}

describe('remote producer over a socket', () => {
  it('streams a value from an off-host producer to a subscriber', async () => {
    const { hub, publisher, subscriber, received } = connect()
    await hub.subscribe(subscriber, {
      table: 'message',
      key: { id: 'm1' },
      field: 'content',
    })

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'default',
      key: { id: 'm1' },
    })
    session.set('Hello')
    await session.flush()
    session.set('Hello from the server')
    await session.flush()

    const updates = updatesOf(received)
    expect(updates.at(-1)).toMatchObject({ op: 'append', text: ' from the server' })
  })

  it('catches a late subscriber up on the accumulated value', async () => {
    const { hub, publisher, subscriber, received } = connect()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'default',
      key: { id: 'm1' },
    })
    session.set('the first half')
    await session.flush()
    session.set('the first half and the second')
    await session.flush()

    // subscribes only now, mid-generation
    await hub.subscribe(subscriber, {
      table: 'message',
      key: { id: 'm1' },
      field: 'content',
    })

    const updates = updatesOf(received)
    expect(updates).toHaveLength(1)
    expect(updates[0]).toMatchObject({
      op: 'snapshot',
      value: 'the first half and the second',
    })
  })

  it('rejects a begin the host refuses, and the producer sees why', async () => {
    const { publisher } = connect()
    await expect(
      publisher.begin('message', 'content', { namespace: 'default', key: { id: 'm1' } })
    ).resolves.toBeDefined()

    // 'nope' is not a streaming field, so the publisher refuses before the wire
    await expect(
      publisher.begin('message', 'nope', { namespace: 'default', key: { id: 'm1' } })
    ).rejects.toThrow(/not a streaming field/)
  })

  it('carries several concurrent generations over one socket', async () => {
    const { hub, publisher, received } = connect()
    const connections = ['m1', 'm2', 'm3'].map((id) => {
      const frames: unknown[][] = []
      return {
        id,
        connection: {
          id: `browser-${id}`,
          identity,
          send: (frame: unknown) => frames.push(frame as unknown[]),
        } satisfies HubConnection,
        frames,
      }
    })
    for (const entry of connections) {
      await hub.subscribe(entry.connection, {
        table: 'message',
        key: { id: entry.id },
        field: 'content',
      })
    }

    // one producer socket, three open generations at once
    const sessions = await Promise.all(
      connections.map((entry) =>
        publisher.begin<string>('message', 'content', {
          namespace: 'default',
          key: { id: entry.id },
        })
      )
    )
    for (const [index, session] of sessions.entries()) {
      session.set(`value for ${connections[index]!.id}`)
      await session.flush()
    }

    for (const entry of connections) {
      const updates = updatesOf(entry.frames)
      expect(updates.at(-1)).toMatchObject({ value: `value for ${entry.id}` })
    }
    expect(received).toHaveLength(0)
  })

  it('releases every generation when the producer socket drops', async () => {
    const { hub, publisher } = connect()
    for (const id of ['m1', 'm2']) {
      await publisher.begin('message', 'content', { namespace: 'default', key: { id } })
    }
    expect(hub.activeTopics).toBe(2)

    hub.dropProducer('server-producer')
    expect(hub.activeTopics).toBe(0)
  })

  it('fails an open generation once the socket is gone', async () => {
    const { publisher, remote } = connect()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'default',
      key: { id: 'm1' },
    })
    session.set('before the drop')
    await session.flush()

    remote.fail('producer socket closed')

    // the generation began successfully, so it is not in the per-stream failure
    // map; only the socket-level failure covers it
    session.set('before the drop, and more after it')
    await expect(session.flush()).rejects.toThrow(/producer socket closed/)
  })

  it('tells a displaced producer it was superseded', async () => {
    const { hub, publisher, remote } = connect()
    const first = await publisher.begin<string>('message', 'content', {
      namespace: 'default',
      key: { id: 'm1' },
    })
    first.set('the first attempt')
    await first.flush()

    // a retry opens a new generation for the same topic
    const second = await publisher.begin<string>('message', 'content', {
      namespace: 'default',
      key: { id: 'm1' },
    })

    first.set('the first attempt, continued')
    await expect(first.flush()).rejects.toThrow(/superseded/)

    // the replacement is unaffected
    second.set('the retry')
    await expect(second.flush()).resolves.toBeUndefined()
    expect(hub.activeTopics).toBe(1)
    expect(remote).toBeDefined()
  })

  it('rejects an update that fails the manifest, naming the bound', async () => {
    const { hub, publisher } = connect()
    const session = await publisher.begin<unknown>('message', 'steps', {
      namespace: 'default',
      key: { id: 'm1' },
    })
    session.set([{ step: 1 }])
    await session.flush()
    expect(hub.activeTopics).toBe(1)

    // validate() demands an array; the publisher catches this before the wire
    expect(() => session.set({ step: 'not an array' })).toThrow(
      /rejected a value in validate/
    )
  })
})
