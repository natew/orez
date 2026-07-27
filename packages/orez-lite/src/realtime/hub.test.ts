// End-to-end proof of the realtime loop with no mocking between layers: a real
// RealtimePublisher drives a real RealtimeHub, whose frames feed real
// RealtimeStore subscribers. Only the sockets are stand-ins, and they are just
// function calls.

import { createSchema, json, string, table } from '@rocicorp/zero'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { RealtimeHub } from './hub.js'
import { defineStreamingFields } from './manifest.js'
import { canonicalTopic, decodeFrame, encodeFrame } from './protocol.js'
import { RealtimePublisher } from './publisher.js'
import { RealtimeStore } from './store.js'

import type {
  HubConnection,
  HubProducer,
  RealtimeIdentity,
  SubscribeAuthorization,
} from './hub.js'
import type { RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

const message = table('message')
  .columns({ id: string(), content: string(), parts: json() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 100_000, maxUpdatesPerSecond: 50, maxBytesPerSecond: 1_000_000 },
    parts: {
      maxBytes: 100_000,
      maxUpdatesPerSecond: 10,
      maxBytesPerSecond: 100_000,
      validate: Array.isArray,
    },
  },
})

const contentSpec = streaming.message.content.spec
const topicOf = (id: string): RealtimeTopic => ({
  table: 'message',
  key: { id },
  field: 'content',
})

const identity = (suffix: string): RealtimeIdentity => ({
  userID: `user-${suffix}`,
  clientID: `client-${suffix}`,
  clientGroupID: `group-${suffix}`,
})

describe('realtime hub', () => {
  let hub: RealtimeHub
  let clock: number
  let authorize: (
    identity: RealtimeIdentity,
    topic: RealtimeTopic
  ) => SubscribeAuthorization
  let flushes: Array<() => void>

  // A client: its store, plus the wiring that turns hub frames into store calls
  // exactly as a real transport would.
  const client = (suffix: string) => {
    const errors: string[] = []
    const sentToHub: unknown[] = []
    const store = new RealtimeStore({
      send: (frame) => sentToHub.push(frame),
      onError: (message) => errors.push(message),
      staleAfterMs: 10_000,
    })
    const connection: HubConnection = {
      id: `conn-${suffix}`,
      identity: identity(suffix),
      // frames go through the real codec, so a shape the wire cannot carry
      // fails here rather than in production
      send: (frame) => {
        const decoded = decodeFrame(encodeFrame(frame as never))
        if (!decoded)
          throw new Error(`hub emitted an undecodable frame: ${JSON.stringify(frame)}`)
        const [kind, body] = decoded
        if (kind === 'field') store.applyUpdates((body as { updates: never[] }).updates)
        else if (kind === 'subscribed') {
          const { topic, status } = body as {
            topic: string
            status: 'active' | 'pending'
          }
          store.handleSubscribed(topic, status)
        } else if (kind === 'subscribe-error') {
          const { topic, reason } = body as { topic: string; reason: string }
          store.handleSubscribeError(topic, reason)
        }
      },
    }
    // the store's outbound frames drive the hub, closing the loop
    const pump = async () => {
      for (const frame of sentToHub.splice(0)) {
        const [kind, body] = frame as [string, { topic: RealtimeTopic }]
        if (kind === 'subscribe') await hub.subscribe(connection, body.topic)
        else if (kind === 'unsubscribe') await hub.unsubscribe(connection, body.topic)
      }
    }
    const mount = async (topic: RealtimeTopic, spec = contentSpec) => {
      const release = store.subscribe(spec, topic, () => {})
      await pump()
      return release
    }
    return { store, connection, errors, mount, pump }
  }

  const producerTransport = (producer: HubProducer): PublisherTransport => ({
    begin: (topic, streamID) => {
      const result = hub.beginGeneration(producer, topic, streamID)
      if (!result.ok) throw new Error(result.reason)
    },
    publish: (update) => {
      if (!hub.publish(producer, update)) {
        throw new Error(`hub rejected a producer frame: ${update.op}`)
      }
    },
    end: () => {},
  })

  const makePublisher = (id = 'producer-1') => {
    const sent: unknown[] = []
    const producer: HubProducer = { id, send: (frame) => sent.push(frame) }
    let counter = 0
    return {
      producer,
      sent,
      publisher: new RealtimePublisher(producerTransport(producer), streaming.manifest, {
        now: () => clock,
        randomID: () => `${id}-stream-${++counter}`,
      }),
    }
  }

  const advance = async (ms: number) => {
    clock += ms
    await vi.advanceTimersByTimeAsync(ms)
    // deliver whatever the batching window has ready
    for (const flush of flushes.splice(0)) flush()
  }

  beforeEach(() => {
    vi.useFakeTimers()
    clock = 0
    flushes = []
    authorize = () => ({ status: 'active' })
    hub = new RealtimeHub({
      manifest: streaming.manifest,
      authorizeSubscribe: (id, topic) => authorize(id, topic),
      scheduleFlush: (flush) => {
        flushes.push(flush)
        return () => {
          const index = flushes.indexOf(flush)
          if (index >= 0) flushes.splice(index, 1)
        }
      },
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('streams a token sequence from producer to subscriber', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    let content = ''
    for (const token of ['Hello', ', ', 'world', '!']) {
      content += token
      session.set(content)
      await advance(30)
    }

    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe('Hello, world!')
    expect(alice.errors).toEqual([])
  })

  it('fans one stream out to every subscriber of that row', async () => {
    const alice = client('alice')
    const bob = client('bob')
    await alice.mount(topicOf('m1'))
    await bob.mount(topicOf('m1'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('shared text')
    await advance(50)

    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe('shared text')
    expect(bob.store.read(contentSpec, topicOf('m1'), '').value).toBe('shared text')
  })

  it('sends nothing to a subscriber of a different row', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('other-row'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('not for alice')
    await advance(50)

    expect(alice.store.read(contentSpec, topicOf('other-row'), 'durable')).toMatchObject({
      value: 'durable',
      phase: 'durable',
    })
  })

  // the acceptance criterion: a component with no field hook receives nothing
  it('sends nothing when no one subscribed', async () => {
    const alice = client('alice')
    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('unobserved')
    await advance(50)

    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')
  })

  it('catches a mid-stream subscriber up with one snapshot, then appends', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    let content = ''
    for (const token of ['one ', 'two ', 'three ']) {
      content += token
      session.set(content)
      await advance(30)
    }

    // bob arrives 3 tokens late and must still see the whole value
    const bob = client('bob')
    await bob.mount(topicOf('m1'))
    expect(bob.store.read(contentSpec, topicOf('m1'), '').value).toBe('one two three ')

    content += 'four'
    session.set(content)
    await advance(30)
    expect(bob.store.read(contentSpec, topicOf('m1'), '').value).toBe(
      'one two three four'
    )
    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe(
      'one two three four'
    )
  })

  it('refuses a subscription the authorizer denies', async () => {
    authorize = () => ({
      status: 'denied',
      reason: 'row is not in your query membership',
    })
    const alice = client('alice')
    await alice.mount(topicOf('m1'))

    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('secret')
    await advance(50)

    expect(alice.errors[0]).toContain('row is not in your query membership')
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')
  })

  it('holds a pending subscription out of fan-out until it is retried', async () => {
    authorize = () => ({ status: 'pending' })
    const alice = client('alice')
    await alice.mount(topicOf('m1'))

    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('optimistic row is not authorized yet')
    await advance(50)
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')

    // the pull lands, membership is recorded, and the retry succeeds
    authorize = () => ({ status: 'active' })
    alice.store.retryPending()
    await alice.pump()
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').value).toBe(
      'optimistic row is not authorized yet'
    )
  })

  it('drops subscriptions when a pull removes the row from membership', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('visible for now')
    await advance(50)
    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe('visible for now')

    hub.revokeMembership('group-alice', [topicOf('m1')])
    session.set('visible for now, plus more')
    await advance(50)

    expect(alice.errors.at(-1)).toContain('left your authorized query membership')
    // the overlay is dropped rather than frozen: the client is no longer
    // authorized for this row, so it must not keep displaying streamed content
    // for it. the durable value is whatever its next pull says.
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable')).toMatchObject({
      value: 'durable',
      phase: 'durable',
    })
    expect(hub.subscribedTopics).toBe(0)
  })

  it('supersedes an older generation and rejects its late frames', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))

    const first = makePublisher('producer-1')
    const second = makePublisher('producer-2')

    const stale = await first.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    stale.set('first attempt')
    await advance(30)

    const retry = await second.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    retry.set('retried attempt')
    await advance(30)
    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe('retried attempt')

    // the superseded producer is told, and its frames are refused at the hub
    expect(first.sent.at(-1)).toMatchObject(['superseded', { streamID: stale.streamID }])

    // the refusal reaches the stale producer rather than becoming an unhandled
    // rejection: its next set throws, so a retry loop stops writing into a
    // stream nobody accepts
    stale.set('first attempt continues')
    await advance(30)
    expect(() => stale.set('and more')).toThrow(/hub rejected a producer frame/)
    expect(alice.store.read(contentSpec, topicOf('m1'), '').value).toBe('retried attempt')
  })

  it('refuses a producer frame carrying another generation stream id', async () => {
    const { producer, publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    const topicID = canonicalTopic(contentSpec.primaryKey, topicOf('m1'))
    expect(hub.generationFor(topicID)?.streamID).toBe(session.streamID)

    // a leaked streamID is not a publish credential: the check is on the
    // producer handle and the generation it leases
    const impostor: HubProducer = { id: 'impostor', send: () => {} }
    expect(
      hub.publish(impostor, {
        topic: topicID,
        streamID: session.streamID,
        seq: 999,
        op: 'append',
        text: 'injected',
      })
    ).toBe(false)
  })

  it('holds the final overlay until Zero produces the committed value', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('the complete answer')
    await advance(30)

    let committed: string | undefined
    await session.finish('the complete answer', async () => {
      committed = 'the complete answer'
    })
    await advance(30)

    // the row in Zero is still the old one: no flash of stale text
    expect(alice.store.read(contentSpec, topicOf('m1'), 'old row')).toMatchObject({
      value: 'the complete answer',
      phase: 'committing',
    })
    // the pull lands and the overlay hands off
    expect(alice.store.read(contentSpec, topicOf('m1'), committed!)).toMatchObject({
      value: 'the complete answer',
      phase: 'durable',
      streamID: null,
    })
  })

  it('reveals the durable value when the producer aborts', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()

    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('half an answer')
    await advance(30)
    await session.abort()
    await advance(30)

    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable row')).toMatchObject({
      value: 'durable row',
      phase: 'durable',
    })
    expect(hub.activeTopics).toBe(0)
  })

  it('releases a generation when the producer socket dies without a terminal frame', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    const { producer, publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('interrupted')
    await advance(30)

    hub.dropProducer(producer.id)
    expect(hub.activeTopics).toBe(0)

    // a new subscriber gets no snapshot of the abandoned generation
    const bob = client('bob')
    await bob.mount(topicOf('m1'))
    expect(bob.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')
  })

  it('stops fan-out once the last subscriber of a topic unmounts', async () => {
    const alice = client('alice')
    const release = await alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('watched')
    await advance(30)
    expect(hub.subscribedTopics).toBe(1)

    release()
    await alice.pump()
    expect(hub.subscribedTopics).toBe(0)

    session.set('watched no longer')
    await advance(30)
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')
  })

  it('drops every subscription when a connection goes away', async () => {
    const alice = client('alice')
    await alice.mount(topicOf('m1'))
    await alice.mount(topicOf('m2'))
    expect(hub.subscribedTopics).toBe(2)

    hub.dropConnection(alice.connection.id)
    expect(hub.subscribedTopics).toBe(0)
  })

  it('coalesces updates for one connection into a single batched frame', async () => {
    const frames: unknown[] = []
    const connection: HubConnection = {
      id: 'counter',
      identity: identity('counter'),
      send: (frame) => frames.push(frame),
    }
    await hub.subscribe(connection, topicOf('m1'))
    await hub.subscribe(connection, topicOf('m2'))
    frames.length = 0

    const { producer } = makePublisher()
    const topicID = (id: string) => canonicalTopic(contentSpec.primaryKey, topicOf(id))
    hub.beginGeneration(producer, topicOf('m1'), 's1')
    hub.beginGeneration(producer, topicOf('m2'), 's2')
    hub.publish(producer, {
      topic: topicID('m1'),
      streamID: 's1',
      seq: 0,
      op: 'snapshot',
      value: 'a',
    })
    hub.publish(producer, {
      topic: topicID('m2'),
      streamID: 's2',
      seq: 0,
      op: 'snapshot',
      value: 'b',
    })
    expect(frames).toEqual([])

    hub.flush()
    expect(frames).toHaveLength(1)
    expect((frames[0] as [string, { updates: unknown[] }])[1].updates).toHaveLength(2)
  })

  // authorization can await (the browser worker serializes every database
  // access through its write queue), so the hub must not fan out to a
  // connection whose authorization has not come back yet.
  it('delivers nothing until a slow authorization resolves', async () => {
    let release!: (value: SubscribeAuthorization) => void
    const gate = new Promise<SubscribeAuthorization>((resolve) => {
      release = resolve
    })
    authorize = () => gate

    const alice = client('alice')
    const pending = alice.mount(topicOf('m1'))
    const { publisher } = makePublisher()
    const session = await publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('streamed while authorization is in flight')
    await advance(50)
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').phase).toBe('durable')

    release({ status: 'active' })
    await pending
    // the catch-up snapshot arrives with the subscription, not before it
    expect(alice.store.read(contentSpec, topicOf('m1'), 'durable').value).toBe(
      'streamed while authorization is in flight'
    )
  })

  // the reason subscribe/unsubscribe are queued per connection: with an
  // awaiting authorizer, an unqueued unsubscribe would resolve first and the
  // later subscribe would leave the connection subscribed to a topic it had
  // already dropped.
  it('applies subscribe and unsubscribe in arrival order despite slow authorization', async () => {
    let resolveAuth!: (value: SubscribeAuthorization) => void
    const gate = new Promise<SubscribeAuthorization>((resolve) => {
      resolveAuth = resolve
    })
    authorize = () => gate

    const connection: HubConnection = {
      id: 'ordered',
      identity: identity('ordered'),
      send: () => {},
    }
    const subscribed = hub.subscribe(connection, topicOf('m1'))
    const unsubscribed = hub.unsubscribe(connection, topicOf('m1'))
    resolveAuth({ status: 'active' })
    await Promise.all([subscribed, unsubscribed])

    expect(hub.subscribedTopics).toBe(0)
  })

  it('drops a subscription whose connection went away mid-authorization', async () => {
    let resolveAuth!: (value: SubscribeAuthorization) => void
    const gate = new Promise<SubscribeAuthorization>((resolve) => {
      resolveAuth = resolve
    })
    authorize = () => gate

    const connection: HubConnection = {
      id: 'vanishing',
      identity: identity('vanishing'),
      send: () => {},
    }
    const subscribed = hub.subscribe(connection, topicOf('m1'))
    hub.dropConnection(connection.id)
    resolveAuth({ status: 'active' })
    await subscribed

    expect(hub.subscribedTopics).toBe(0)
  })

  it('refuses a topic outside the manifest', async () => {
    const errors: string[] = []
    const connection: HubConnection = {
      id: 'c',
      identity: identity('c'),
      send: (frame) => {
        const [kind, body] = frame as [string, { reason?: string }]
        if (kind === 'subscribe-error') errors.push(body.reason!)
      },
    }
    await hub.subscribe(connection, { table: 'message', key: { id: 'm1' }, field: 'id' })
    expect(errors[0]).toContain("'message.id' is not a streaming field")
  })

  it('enforces the per-connection subscription limit', async () => {
    const limited = new RealtimeHub({
      manifest: streaming.manifest,
      authorizeSubscribe: () => ({ status: 'active' }),
      limits: { maxTopicsPerConnection: 2 },
    })
    const errors: string[] = []
    const connection: HubConnection = {
      id: 'c',
      identity: identity('c'),
      send: (frame) => {
        const [kind, body] = frame as [string, { reason?: string }]
        if (kind === 'subscribe-error') errors.push(body.reason!)
      },
    }
    await limited.subscribe(connection, topicOf('m1'))
    await limited.subscribe(connection, topicOf('m2'))
    await limited.subscribe(connection, topicOf('m3'))
    expect(errors).toEqual(['connection is at its subscription limit'])
  })

  it('lets a resubscribe after reconnect through the per-connection limit', async () => {
    const limited = new RealtimeHub({
      manifest: streaming.manifest,
      authorizeSubscribe: () => ({ status: 'active' }),
      limits: { maxTopicsPerConnection: 1 },
    })
    const errors: string[] = []
    const connection: HubConnection = {
      id: 'c',
      identity: identity('c'),
      send: (frame) => {
        const [kind, body] = frame as [string, { reason?: string }]
        if (kind === 'subscribe-error') errors.push(body.reason!)
      },
    }
    await limited.subscribe(connection, topicOf('m1'))
    await limited.subscribe(connection, topicOf('m1'))
    expect(errors).toEqual([])
  })

  it('validates a replace-mode value at the hub, not only at the producer', async () => {
    const { producer } = makePublisher()
    const partsTopic: RealtimeTopic = {
      table: 'message',
      key: { id: 'm1' },
      field: 'parts',
    }
    hub.beginGeneration(producer, partsTopic, 's1')
    const topicID = canonicalTopic(streaming.message.parts.spec.primaryKey, partsTopic)

    // a producer bug (or a compromised browser producer) cannot push a value
    // the manifest's validator rejects
    expect(
      hub.publish(producer, {
        topic: topicID,
        streamID: 's1',
        seq: 0,
        op: 'snapshot',
        value: { not: 'an array' },
      })
    ).toBe(false)
    expect(
      hub.publish(producer, {
        topic: topicID,
        streamID: 's1',
        seq: 0,
        op: 'snapshot',
        value: [{ type: 'text' }],
      })
    ).toBe(true)
  })

  it('refuses an append that would carry the value past its byte ceiling', async () => {
    const { producer } = makePublisher()
    hub.beginGeneration(producer, topicOf('m1'), 's1')
    const topicID = canonicalTopic(contentSpec.primaryKey, topicOf('m1'))

    expect(
      hub.publish(producer, {
        topic: topicID,
        streamID: 's1',
        seq: 0,
        op: 'append',
        text: 'x'.repeat(100_001),
      })
    ).toBe(false)
  })
})
