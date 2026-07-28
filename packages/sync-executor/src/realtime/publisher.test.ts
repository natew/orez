import { createSchema, json, string, table } from '@rocicorp/zero'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { defineStreamingFields } from './manifest.js'
import { RealtimePublisher } from './publisher.js'
import { RealtimeStore } from './store.js'

import type { FieldUpdate, RealtimeTopic } from './protocol.js'
import type { PublisherTransport } from './publisher.js'

const message = table('message')
  .columns({ id: string(), content: string(), parts: json() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 100_000, maxUpdatesPerSecond: 20, maxBytesPerSecond: 10_000 },
    parts: {
      maxBytes: 100_000,
      maxUpdatesPerSecond: 10,
      maxBytesPerSecond: 100_000,
      validate: Array.isArray,
    },
  },
})

describe('realtime publisher', () => {
  let published: FieldUpdate[]
  let begun: Array<{ topic: RealtimeTopic; streamID: string }>
  let ended: string[]
  let clock: number
  let transport: PublisherTransport
  let publisher: RealtimePublisher

  beforeEach(() => {
    vi.useFakeTimers()
    published = []
    begun = []
    ended = []
    clock = 0
    transport = {
      publish: (update) => {
        published.push(update)
      },
      begin: (topic, streamID) => {
        begun.push({ topic, streamID })
      },
      end: (streamID) => {
        ended.push(streamID)
      },
    }
    let counter = 0
    publisher = new RealtimePublisher(transport, streaming.manifest, {
      now: () => clock,
      randomID: () => `stream-${++counter}`,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  const advance = async (ms: number) => {
    clock += ms
    await vi.advanceTimersByTimeAsync(ms)
  }

  const begin = (field: 'content' | 'parts' = 'content') =>
    publisher.begin(field === 'content' ? 'message' : 'message', field, {
      namespace: 'ns',
      key: { id: 'm1' },
    })

  it('rejects a field outside the manifest', async () => {
    await expect(
      publisher.begin('message', 'id', { namespace: 'ns', key: { id: 'm1' } })
    ).rejects.toThrow(/is not a streaming field/)
  })

  it('rejects a topic missing a primary key column', async () => {
    await expect(
      publisher.begin('message', 'content', { namespace: 'ns', key: {} })
    ).rejects.toThrow(/missing primary key column 'id'/)
  })

  it('opens a generation before any value goes out', async () => {
    await begin()
    expect(begun).toEqual([
      {
        topic: { table: 'message', key: { id: 'm1' }, field: 'content' },
        streamID: 'stream-1',
      },
    ])
    expect(published).toEqual([])
  })

  it('sends the first value as a snapshot and later values as appends', async () => {
    const session = await begin()
    session.set('Hello')
    await advance(100)
    session.set('Hello world')
    await advance(100)

    expect(published.map((u) => u.op)).toEqual(['snapshot', 'append'])
    expect(published[0]).toMatchObject({ op: 'snapshot', value: 'Hello', seq: 1 })
    expect(published[1]).toMatchObject({ op: 'append', text: ' world', seq: 2 })
  })

  // the reason append mode exists: per-frame cost must not grow with the value
  it('keeps per-frame bytes flat as the accumulated value grows', async () => {
    const session = await begin()
    let content = ''
    for (let index = 0; index < 40; index++) {
      content += 'x'.repeat(500)
      session.set(content)
      await advance(60)
    }

    const appends = published.filter((u) => u.op === 'append')
    expect(appends.length).toBeGreaterThan(5)
    const sizes = appends.map((u) => (u as { text: string }).text.length)
    // every append carries only new characters, never the 20KB accumulation
    expect(Math.max(...sizes)).toBeLessThanOrEqual(2000)
    const totalDelivered = published.reduce(
      (sum, u) =>
        sum +
        (u.op === 'append'
          ? (u as { text: string }).text.length
          : u.op === 'snapshot'
            ? String((u as { value: unknown }).value).length
            : 0),
      0
    )
    // total delivered tracks the final value, not value x frames
    expect(totalDelivered).toBe(content.length)
  })

  it('coalesces a burst into the manifest update rate', async () => {
    const session = await begin()
    for (let index = 0; index < 200; index++) session.set('x'.repeat(index + 1))
    await advance(1000)

    // maxUpdatesPerSecond is 20; the burst collapses rather than sending 200
    expect(published.length).toBeLessThanOrEqual(21)
    expect(published.length).toBeGreaterThan(0)
  })

  it('delivers the complete final value even when the burst was coalesced', async () => {
    const session = await begin()
    for (let index = 0; index < 50; index++) session.set('y'.repeat(index + 1))
    const final = 'y'.repeat(50)
    await session.finish(final, async () => {})

    const rebuilt = published.reduce((text, update) => {
      if (update.op === 'snapshot') return String((update as { value: unknown }).value)
      if (update.op === 'append') return text + (update as { text: string }).text
      return text
    }, '')
    expect(rebuilt).toBe(final)
  })

  it('runs the commit between the final value and the end frame', async () => {
    const order: string[] = []
    const session = await begin()
    session.set('partial')
    await advance(100)

    await session.finish('partial and complete', async () => {
      order.push(`commit-after-${published.length}-frames`)
    })

    // snapshot, the final append, THEN the commit, then end
    expect(order).toEqual(['commit-after-2-frames'])
    expect(published.at(-1)).toMatchObject({ op: 'end' })
    expect(ended).toEqual(['stream-1'])
  })

  it('emits no end frame when the commit throws', async () => {
    const session = await begin()
    session.set('partial')
    await advance(100)

    await expect(
      session.finish('partial and complete', async () => {
        throw new Error('write rejected')
      })
    ).rejects.toThrow('write rejected')

    expect(published.some((u) => u.op === 'end')).toBe(false)
  })

  it('emits abort and closes the generation', async () => {
    const session = await begin()
    session.set('half')
    await advance(100)
    await session.abort()

    expect(published.at(-1)).toMatchObject({ op: 'abort' })
    expect(ended).toEqual(['stream-1'])
  })

  it('refuses to set after the generation closed', async () => {
    const session = await begin()
    await session.finish('done', async () => {})
    expect(() => session.set('more')).toThrow(/already closed/)
  })

  it('rejects a value over the field byte ceiling', async () => {
    const session = await begin()
    expect(() => session.set('x'.repeat(100_001))).toThrow(/over its 100000 maxBytes/)
  })

  it('rejects a non-string value on an append-mode field', async () => {
    const session = await begin()
    expect(() => session.set({ not: 'a string' } as never)).toThrow(
      /append mode and takes a string/
    )
  })

  // append ships suffixes; a rewritten value would corrupt every subscriber
  it('rejects an append-mode value that stops extending what was sent', async () => {
    const session = await begin()
    session.set('hello world')
    await advance(100)
    expect(() => session.set('goodbye')).toThrow(
      /is not an extension of what was already sent/
    )
  })

  it('sends complete values for a replace-mode field', async () => {
    const session = await publisher.begin<unknown[]>('message', 'parts', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    session.set([{ type: 'text' }])
    await advance(200)
    session.set([{ type: 'text' }, { type: 'tool' }])
    await advance(200)

    expect(published.map((u) => u.op)).toEqual(['snapshot', 'snapshot'])
    expect(published[1]).toMatchObject({ value: [{ type: 'text' }, { type: 'tool' }] })
  })

  it('runs a replace-mode value through the field validator', async () => {
    const session = await publisher.begin<unknown>('message', 'parts', {
      namespace: 'ns',
      key: { id: 'm1' },
    })
    expect(() => session.set({ not: 'an array' })).toThrow(/rejected a value in validate/)
  })

  it('produces frames a subscriber store reconstructs exactly', async () => {
    // the two halves of the protocol, checked against each other rather than
    // against a hand-written expectation of the wire
    const store = new RealtimeStore({ send: () => {}, staleAfterMs: 10_000 })
    const topic = { table: 'message', key: { id: 'm1' }, field: 'content' } as const
    store.subscribe({ spec: streaming.message.content.spec, topic }, () => {})

    const session = await begin()
    let content = ''
    for (const token of ['The ', 'quick ', 'brown ', 'fox ', 'jumps']) {
      content += token
      session.set(content)
      await advance(60)
    }
    await session.finish(content, async () => {})

    store.applyUpdates(published)
    expect(
      store.read({ spec: streaming.message.content.spec, topic }, 'stale row')
    ).toMatchObject({
      value: 'The quick brown fox jumps',
      phase: 'committing',
    })
  })

  it('lets a late subscriber catch up from the accumulated value alone', async () => {
    const session = await begin()
    let content = ''
    for (const token of ['one ', 'two ', 'three ']) {
      content += token
      session.set(content)
      await advance(60)
    }

    // a subscriber that missed everything so far sees only the frames from here
    const store = new RealtimeStore({ send: () => {}, staleAfterMs: 10_000 })
    const topic = { table: 'message', key: { id: 'm1' }, field: 'content' } as const
    store.subscribe({ spec: streaming.message.content.spec, topic }, () => {})
    const alreadySent = published.length

    // the host repairs a new subscriber with a snapshot of what it accumulated,
    // stamped with the generation's CURRENT sequence number so the appends that
    // follow are strictly newer. simulate that, then continue the stream.
    store.applyUpdates([
      {
        topic: published[0]!.topic,
        streamID: session.streamID,
        seq: published.at(-1)!.seq,
        op: 'snapshot',
        value: content,
      },
    ])
    content += 'four'
    session.set(content)
    await advance(60)
    store.applyUpdates(published.slice(alreadySent))

    expect(store.read({ spec: streaming.message.content.spec, topic }, '').value).toBe(
      'one two three four'
    )
  })
})
