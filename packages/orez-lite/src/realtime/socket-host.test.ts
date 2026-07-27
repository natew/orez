// The socket host, exercised on the two things a runtime cannot easily test
// itself: role isolation, and surviving a cold start with sockets still open.
//
// Hibernation is the reason `rehydrate` exists and is also the hardest case to
// reproduce in a real Durable Object, so it is driven here instead: build a
// host, take its subscriptions, throw the host away, and rebuild from what a
// runtime would have persisted.

import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, it } from 'vitest'

import { defineStreamingFields } from './manifest.js'
import { canonicalTopic, encodeFrame } from './protocol.js'
import { createSocketHost } from './socket-host.js'

import type { SubscribeAuthorization } from './hub.js'
import type { FieldUpdate, RealtimeTopic } from './protocol.js'

const message = table('message')
  .columns({ id: string(), content: string() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 100_000, maxUpdatesPerSecond: 60, maxBytesPerSecond: 500_000 },
  },
})

const identity = { userID: 'u1', clientID: 'c1', clientGroupID: 'g1' }
const topic: RealtimeTopic = { table: 'message', key: { id: 'm1' }, field: 'content' }

// a socket that records what the host sent it
function socket() {
  const sent: string[] = []
  return {
    sent,
    send: (data: string) => sent.push(data),
    frames: () => sent.map((raw) => JSON.parse(raw) as [string, Record<string, unknown>]),
    updates: () =>
      sent
        .map((raw) => JSON.parse(raw) as [string, { updates?: FieldUpdate[] }])
        .filter(([kind]) => kind === 'field')
        .flatMap(([, body]) => body.updates ?? []),
  }
}

const build = (authorize: () => SubscribeAuthorization = () => ({ status: 'active' })) =>
  createSocketHost({
    manifest: streaming.manifest,
    authorizeSubscribe: authorize,
    scheduleFlush: (flush) => flush(),
  })

// drive a producer through its socket, as a remote producer's frames would
async function stream(
  host: ReturnType<typeof build>,
  producerSocket: ReturnType<typeof socket>,
  value: string
) {
  const producer = host.acceptProducer(producerSocket, 'producer-1')
  producer.handleMessage(encodeFrame(['begin', { topic, streamID: 's1' }]))
  producer.handleMessage(
    encodeFrame([
      'publish',
      { update: { topic: topicID(), streamID: 's1', seq: 0, op: 'snapshot', value } },
    ])
  )
  return producer
}

// the real encoder, never a hand-rolled copy. A test that derives the topic id
// its own way can agree with nothing the host does and still pass, which is
// exactly what the first version of this file did.
const topicID = () => canonicalTopic(streaming.message.content.spec.primaryKey, topic)

describe('socket host', () => {
  it('delivers a producer socket value to a subscriber socket', async () => {
    const host = build()
    const sub = socket()
    const connection = host.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()

    const prod = socket()
    await stream(host, prod, 'from the producer')

    expect(sub.updates().at(-1)).toMatchObject({ value: 'from the producer' })
  })

  // the security property: possessing a streamID is not a publish credential
  it('refuses producer frames arriving on a subscriber socket', async () => {
    const host = build()
    const sub = socket()
    const connection = host.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()

    // the same frames a legitimate producer sends, on a subscriber's channel
    connection.handleMessage(encodeFrame(['begin', { topic, streamID: 'stolen' }]))
    connection.handleMessage(
      encodeFrame([
        'publish',
        {
          update: {
            topic: topicID(),
            streamID: 'stolen',
            seq: 0,
            op: 'snapshot',
            value: 'injected',
          },
        },
      ])
    )

    expect(host.hub.activeTopics).toBe(0)
    expect(sub.updates()).toHaveLength(0)
  })

  it('ignores subscriber frames arriving on a producer socket', () => {
    const host = build()
    const prod = socket()
    const producer = host.acceptProducer(prod, 'producer-1')
    producer.handleMessage(encodeFrame(['subscribe', { topic }]))
    expect(host.hub.subscribedTopics).toBe(0)
  })

  it('survives a malformed frame without dropping the socket', async () => {
    const host = build()
    const sub = socket()
    const connection = host.acceptSubscriber(sub, identity, 'conn-1')

    expect(() => connection.handleMessage('not json at all')).not.toThrow()
    expect(() => connection.handleMessage('{"half":')).not.toThrow()

    // still usable afterwards
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()
    expect(host.hub.subscribedTopics).toBe(1)
  })

  it('reports its topics in the shape rehydrate accepts', async () => {
    const host = build()
    const sub = socket()
    const connection = host.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()

    expect(connection.topics()).toEqual([topic])

    await connection.handleMessage(encodeFrame(['unsubscribe', { topic }]))
    await Promise.resolve()
    expect(connection.topics()).toEqual([])
  })

  // the hibernation case: the runtime evicted the object holding the hub, the
  // socket stayed open, and a producer starts streaming after the cold start
  it('restores subscriptions for a socket that outlived the hub', async () => {
    const first = build()
    const sub = socket()
    const connection = first.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()
    const persisted = connection.topics()

    // the object is evicted: a brand new host, nothing carried over in memory
    const revived = build()
    await revived.rehydrate([
      { socket: sub, identity, connectionID: 'conn-1', topics: persisted },
    ])

    const prod = socket()
    await stream(revived, prod, 'after the cold start')

    expect(sub.updates().at(-1)).toMatchObject({ value: 'after the cold start' })
  })

  // rehydration re-authorizes rather than restoring state, so access lost
  // during the gap is not resurrected
  it('does not restore a subscription whose authorization was revoked', async () => {
    const first = build()
    const sub = socket()
    const connection = first.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()
    const persisted = connection.topics()

    const revived = build(() => ({ status: 'denied', reason: 'no longer a member' }))
    await revived.rehydrate([
      { socket: sub, identity, connectionID: 'conn-1', topics: persisted },
    ])

    const prod = socket()
    await stream(revived, prod, 'must not be delivered')

    expect(sub.updates()).toHaveLength(0)
    expect(sub.frames().some(([kind]) => kind === 'subscribe-error')).toBe(true)
  })

  it('drops a connection when its socket closes', async () => {
    const host = build()
    const sub = socket()
    const connection = host.acceptSubscriber(sub, identity, 'conn-1')
    await connection.handleMessage(encodeFrame(['subscribe', { topic }]))
    await Promise.resolve()
    expect(host.hub.subscribedTopics).toBe(1)

    connection.close()
    expect(host.hub.subscribedTopics).toBe(0)
  })

  it('releases a producer generations when its socket closes', async () => {
    const host = build()
    const prod = socket()
    const producer = await stream(host, prod, 'interrupted')
    expect(host.hub.activeTopics).toBe(1)

    producer.close()
    expect(host.hub.activeTopics).toBe(0)
  })
})
