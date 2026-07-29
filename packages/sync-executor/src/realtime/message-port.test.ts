// The MessagePort transport over a REAL MessageChannel: frames cross a genuine
// structured-clone boundary and are serialized by the real codec, so anything
// the channel cannot carry fails here rather than in a browser.

import { createSchema, string, table } from '@rocicorp/zero'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { defineStreamingFields } from './manifest.js'
import { BrowserRealtime, connectRealtimePort } from './message-port.js'

import type { RealtimeIdentity } from './hub.js'
import type { MembershipReader } from './message-port.js'

const message = table('message')
  .columns({ id: string(), content: string() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 100_000, maxUpdatesPerSecond: 50, maxBytesPerSecond: 1_000_000 },
  },
})

const contentSpec = streaming.message.content.spec
const topicOf = (id: string) =>
  ({ table: 'message', key: { id }, field: 'content' }) as const

const identity: RealtimeIdentity = {
  userID: 'alice',
  clientID: 'client-1',
  clientGroupID: 'group-1',
}

// let queued port messages and the hub's per-connection queue drain
const settle = async () => {
  for (let index = 0; index < 8; index++) await new Promise((r) => setTimeout(r, 0))
}

describe('realtime over a MessagePort', () => {
  let members: Set<string>
  let ownedGroups: Set<string>
  let realtime: BrowserRealtime
  let channels: MessageChannel[]

  const readMembership: MembershipReader = async (id, table, key) => ({
    ownsGroup: ownedGroups.has(id.clientGroupID),
    authorized: members.has(`${table}:${String(key.id)}`),
  })

  const connect = (who: RealtimeIdentity = identity) => {
    const channel = new MessageChannel()
    channels.push(channel)
    const detach = realtime.connect(channel.port2, who)
    channel.port2.start()
    const client = connectRealtimePort(channel.port1, { staleAfterMs: 60_000 })
    return { ...client, detach }
  }

  beforeEach(() => {
    members = new Set(['message:m1'])
    ownedGroups = new Set(['group-1'])
    channels = []
    realtime = new BrowserRealtime({ manifest: streaming.manifest, readMembership })
  })

  afterEach(() => {
    for (const channel of channels) {
      channel.port1.close()
      channel.port2.close()
    }
  })

  it('streams a value from the in-process publisher to a port subscriber', async () => {
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('Hello from the worker')
    await settle()
    realtime.flush()
    await settle()

    expect(client.store.read({ spec: contentSpec, topic: topicOf('m1') }, '').value).toBe(
      'Hello from the worker'
    )
  })

  it('appends deltas rather than resending the accumulated value', async () => {
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    let content = ''
    for (const token of ['one ', 'two ', 'three ', 'four']) {
      content += token
      session.set(content)
      // session.flush waits out the manifest's rate bound (updates after the
      // first defer ~20ms at 50/s), so each token really leaves as its own
      // delta instead of coalescing under a fixed-length settle
      await session.flush()
      realtime.flush()
      await settle()
    }

    expect(client.store.read({ spec: contentSpec, topic: topicOf('m1') }, '').value).toBe(
      'one two three four'
    )
  })

  it('refuses a subscription for a client group the user does not own', async () => {
    const errors: string[] = []
    const channel = new MessageChannel()
    channels.push(channel)
    realtime.connect(channel.port2, { ...identity, clientGroupID: 'someone-elses-group' })
    channel.port2.start()
    const client = connectRealtimePort(channel.port1, { onError: (m) => errors.push(m) })

    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    expect(errors[0]).toContain('client group does not belong to this user')
  })

  it('answers pending for a row whose membership the server has not recorded', async () => {
    members.delete('message:m1')
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('not yet authorized')
    await settle()
    realtime.flush()
    await settle()
    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'durable').phase
    ).toBe('durable')

    // the pull lands, membership is recorded, the client retries
    members.add('message:m1')
    client.store.retryPending()
    await settle()
    realtime.flush()
    await settle()
    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'durable').value
    ).toBe('not yet authorized')
  })

  // the acceptance criterion: no hook, no traffic
  it('sends nothing to a port that subscribed to nothing', async () => {
    const client = connect()
    const received: unknown[] = []
    channels[0]!.port1.addEventListener('message', (event) => received.push(event.data))
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('unobserved')
    await settle()
    realtime.flush()
    await settle()

    expect(received).toEqual([])
    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'durable').phase
    ).toBe('durable')

    // positive control: the same listener DOES receive frames once this port
    // subscribes, so the emptiness above is the hub withholding traffic rather
    // than a listener that could never have observed anything.
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()
    session.set('unobserved, then observed')
    // second update in the rate window: only session.flush waits out the bound
    await session.flush()
    realtime.flush()
    await settle()
    expect(received.length).toBeGreaterThan(0)
    expect(client.store.read({ spec: contentSpec, topic: topicOf('m1') }, '').value).toBe(
      'unobserved, then observed'
    )
  })

  it('isolates ports: one subscriber does not receive another row', async () => {
    members.add('message:m2')
    const first = connect()
    const second = connect()
    first.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    second.store.subscribe({ spec: contentSpec, topic: topicOf('m2') }, () => {})
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('only for m1')
    await settle()
    realtime.flush()
    await settle()

    expect(first.store.read({ spec: contentSpec, topic: topicOf('m1') }, '').value).toBe(
      'only for m1'
    )
    expect(
      second.store.read({ spec: contentSpec, topic: topicOf('m2') }, 'durable').phase
    ).toBe('durable')
  })

  it('catches a port that subscribes mid-stream up to the accumulated value', async () => {
    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    let content = ''
    for (const token of ['already ', 'in ', 'progress']) {
      content += token
      session.set(content)
      // deterministic delivery: updates after the first sit in the publisher's
      // rate limiter (50/s = 20ms gap), longer than settle's zero-timers
      await session.flush()
    }
    realtime.flush()
    await settle()

    const late = connect()
    late.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    expect(late.store.read({ spec: contentSpec, topic: topicOf('m1') }, '').value).toBe(
      'already in progress'
    )
  })

  it('holds the final overlay through commit and hands off when Zero catches up', async () => {
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('the answer')
    await settle()
    await session.finish('the answer', async () => {})
    await settle()
    realtime.flush()
    await settle()

    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'old row')
    ).toMatchObject({
      value: 'the answer',
      phase: 'committing',
    })
    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'the answer')
    ).toMatchObject({
      phase: 'durable',
      streamID: null,
    })
  })

  it('drops a subscription when a pull removes the row from membership', async () => {
    const errors: string[] = []
    const channel = new MessageChannel()
    channels.push(channel)
    realtime.connect(channel.port2, identity)
    channel.port2.start()
    const client = connectRealtimePort(channel.port1, { onError: (m) => errors.push(m) })
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    realtime.revokeMembership('group-1', [topicOf('m1')])
    await settle()

    expect(errors.at(-1)).toContain('left your authorized query membership')
  })

  it('ignores a publish frame arriving on a subscriber port', async () => {
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    // a compromised page cannot become a producer by sending producer frames:
    // the port handler only accepts subscribe and unsubscribe
    channels[0]!.port1.postMessage({
      event: 'realtime',
      frame: JSON.stringify([
        'field',
        {
          updates: [
            {
              topic: 'anything',
              streamID: 'forged',
              seq: 1,
              op: 'append',
              text: 'injected',
            },
          ],
        },
      ]),
    })
    await settle()

    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'durable').value
    ).toBe('durable')
    expect(realtime.hub.activeTopics).toBe(0)
  })

  it('stops delivering after the port detaches', async () => {
    const client = connect()
    client.store.subscribe({ spec: contentSpec, topic: topicOf('m1') }, () => {})
    await settle()

    client.detach()
    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set('after detach')
    await settle()
    realtime.flush()
    await settle()

    expect(
      client.store.read({ spec: contentSpec, topic: topicOf('m1') }, 'durable').phase
    ).toBe('durable')
    expect(realtime.hub.subscribedTopics).toBe(0)
  })
})
