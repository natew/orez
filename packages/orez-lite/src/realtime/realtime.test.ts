import { createSchema, json, number, string, table } from '@rocicorp/zero'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import canonicalPkVectors from '../../../../harness/fixtures/canonical-pk-vectors.json' with { type: 'json' }
import { defineStreamingFields, resolveTopic } from './manifest.js'
import {
  canonicalPrimaryKey,
  canonicalTopic,
  decodeFrame,
  encodeFrame,
  TOPIC_SEPARATOR,
} from './protocol.js'
import { RealtimeStore } from './store.js'

import type { StreamingFieldSpec } from './manifest.js'
import type { FieldUpdate } from './protocol.js'

const message = table('message')
  .columns({
    id: string(),
    content: string(),
    parts: json(),
    tokens: number(),
  })
  .primaryKey('id')

const revision = table('revision')
  .columns({
    workspaceID: string(),
    id: string(),
    body: string(),
  })
  .primaryKey('workspaceID', 'id')

const schema = createSchema({ tables: [message, revision] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 512_000, maxUpdatesPerSecond: 20, maxBytesPerSecond: 200_000 },
    parts: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 10,
      maxBytesPerSecond: 2_000_000,
      validate: (value) => Array.isArray(value),
    },
  },
  revision: {
    body: { maxBytes: 10_000, maxUpdatesPerSecond: 20, maxBytesPerSecond: 100_000 },
  },
})

const contentSpec = streaming.message.content.spec
const partsSpec = streaming.message.parts.spec

describe('canonical topic encoding', () => {
  // the JS canonicalizer is the third implementation of this encoding, beside
  // the native and wasm rust builds. the client derives topics locally, so a
  // disagreement here is a subscription that silently receives nothing.
  it('matches the shared cross-build primary key vectors', () => {
    for (const vector of canonicalPkVectors.vectors) {
      expect(
        canonicalPrimaryKey(vector.primaryKey, vector.pk as Record<string, never>),
        vector.name
      ).toBe(vector.expected)
    }
  })

  it('exercises key ordering rather than agreeing with JSON.stringify by luck', () => {
    const disagreements = canonicalPkVectors.vectors.filter(
      (vector) => JSON.stringify(vector.pk) !== vector.expected
    )
    expect(disagreements.length).toBeGreaterThan(0)
  })

  it('ignores the key order of the supplied object', () => {
    const forward = canonicalTopic(['workspaceID', 'id'], {
      table: 'revision',
      key: { workspaceID: 'w', id: 'r' },
      field: 'body',
    })
    const reversed = canonicalTopic(['workspaceID', 'id'], {
      table: 'revision',
      key: { id: 'r', workspaceID: 'w' },
      field: 'body',
    })
    expect(forward).toBe(reversed)
  })

  it('separates two fields of the same row', () => {
    const content = canonicalTopic(['id'], {
      table: 'message',
      key: { id: 'm' },
      field: 'content',
    })
    const parts = canonicalTopic(['id'], {
      table: 'message',
      key: { id: 'm' },
      field: 'parts',
    })
    expect(content).not.toBe(parts)
  })
})

describe('manifest', () => {
  it('infers append for string columns and replace for everything else', () => {
    expect(contentSpec.mode).toBe('append')
    expect(partsSpec.mode).toBe('replace')
    expect(streaming.message.content.spec.primaryKey).toEqual(['id'])
  })

  it('carries the table composite primary key onto the field spec', () => {
    expect(streaming.revision.body.spec.primaryKey).toEqual(['workspaceID', 'id'])
  })

  it('rejects a table absent from the Zero schema', () => {
    expect(() =>
      defineStreamingFields(schema, {
        ghost: { content: { maxBytes: 1, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 } },
      })
    ).toThrow(/not in the Zero schema/)
  })

  it('rejects a column absent from the table', () => {
    expect(() =>
      defineStreamingFields(schema, {
        message: { ghost: { maxBytes: 1, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 } },
      })
    ).toThrow(/not in the Zero schema/)
  })

  it('rejects streaming a primary key column', () => {
    expect(() =>
      defineStreamingFields(schema, {
        message: { id: { maxBytes: 1, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 } },
      })
    ).toThrow(/primary key column/)
  })

  it('requires a validator for a json column', () => {
    expect(() =>
      defineStreamingFields(schema, {
        message: { parts: { maxBytes: 1, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 } },
      })
    ).toThrow(/must supply validate/)
  })

  it('rejects append mode on a non-string column', () => {
    expect(() =>
      defineStreamingFields(schema, {
        message: {
          tokens: {
            maxBytes: 1,
            maxUpdatesPerSecond: 1,
            maxBytesPerSecond: 1,
            mode: 'append',
          },
        },
      })
    ).toThrow(/append mode concatenates/)
  })

  it('lets a string column opt out of append', () => {
    const replaced = defineStreamingFields(schema, {
      message: {
        content: {
          maxBytes: 10,
          maxUpdatesPerSecond: 1,
          maxBytesPerSecond: 1,
          mode: 'replace',
        },
      },
    })
    expect(replaced.message.content.spec.mode).toBe('replace')
  })

  it('rejects a non-positive bound', () => {
    expect(() =>
      defineStreamingFields(schema, {
        message: {
          content: { maxBytes: 0, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 },
        },
      })
    ).toThrow(/positive maxBytes/)
  })

  it('changes schema identity when a streamed column type changes', () => {
    const other = defineStreamingFields(schema, {
      message: {
        tokens: { maxBytes: 10, maxUpdatesPerSecond: 1, maxBytesPerSecond: 1 },
      },
    })
    expect(other.manifest.schemaKey).not.toBe(streaming.manifest.schemaKey)
  })

  it('refuses a topic missing a primary key column', () => {
    expect(() => streaming.revision.body({ workspaceID: 'w' } as never)).toThrow(
      /missing primary key column 'id'/
    )
  })

  it('resolves a manifest topic to its canonical id', () => {
    const resolved = resolveTopic(streaming.manifest, {
      table: 'message',
      key: { id: 'm1' },
      field: 'content',
    })
    expect(resolved).toMatchObject({
      id: `message${TOPIC_SEPARATOR}content${TOPIC_SEPARATOR}{"id":"m1"}`,
    })
  })

  it('refuses to resolve a field outside the manifest', () => {
    const resolved = resolveTopic(streaming.manifest, {
      table: 'message',
      key: { id: 'm1' },
      field: 'tokens',
    })
    expect(resolved).toEqual({ reason: "'message.tokens' is not a streaming field" })
  })
})

describe('frame codec', () => {
  it('round-trips a subscribe frame', () => {
    const frame = [
      'subscribe',
      { topic: { table: 'message', key: { id: 'm' }, field: 'content' } },
    ]
    expect(decodeFrame(encodeFrame(frame as never))).toEqual(frame)
  })

  it('round-trips a batched field frame', () => {
    const frame = [
      'field',
      {
        updates: [
          { topic: 't1', streamID: 's', seq: 1, op: 'append', text: 'hi' },
          { topic: 't2', streamID: 's', seq: 2, op: 'snapshot', value: { a: 1 } },
          { topic: 't3', streamID: 's', seq: 3, op: 'end' },
        ],
      },
    ]
    expect(decodeFrame(encodeFrame(frame as never))).toEqual(frame)
  })

  it('accepts the legacy bare wake frame', () => {
    expect(decodeFrame('wake')).toEqual(['wake', {}])
  })

  it('returns undefined rather than throwing on malformed input', () => {
    expect(decodeFrame('{')).toBeUndefined()
    expect(decodeFrame('"scalar"')).toBeUndefined()
    expect(decodeFrame('["field",{"updates":"nope"}]')).toBeUndefined()
    expect(decodeFrame('["field",{"updates":[{"topic":1}]}]')).toBeUndefined()
    expect(decodeFrame('["subscribe",{"topic":{"table":"m"}}]')).toBeUndefined()
    expect(decodeFrame('["nonsense",{}]')).toBeUndefined()
  })

  it('rejects a negative or fractional sequence number', () => {
    expect(
      decodeFrame(
        '["field",{"updates":[{"topic":"t","streamID":"s","seq":-1,"op":"end"}]}]'
      )
    ).toBeUndefined()
    expect(
      decodeFrame(
        '["field",{"updates":[{"topic":"t","streamID":"s","seq":1.5,"op":"end"}]}]'
      )
    ).toBeUndefined()
  })
})

describe('realtime store', () => {
  let sent: unknown[]
  let errors: string[]
  let store: RealtimeStore

  const contentTopic = { table: 'message', key: { id: 'm1' }, field: 'content' } as const
  const id = canonicalTopic(['id'], contentTopic)

  const mount = (spec: StreamingFieldSpec = contentSpec, topic = contentTopic) => {
    const listener = vi.fn()
    const release = store.subscribe(spec, topic, listener)
    return { listener, release }
  }

  const apply = (...updates: FieldUpdate[]) => store.applyUpdates(updates)

  beforeEach(() => {
    vi.useFakeTimers()
    sent = []
    errors = []
    store = new RealtimeStore({
      send: (frame) => sent.push(frame),
      onError: (message) => errors.push(message),
      staleAfterMs: 1000,
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('sends no traffic and reports durable state without a subscription', () => {
    expect(store.read(contentSpec, contentTopic, 'durable value')).toEqual({
      value: 'durable value',
      phase: 'durable',
      streamID: null,
    })
    expect(sent).toEqual([])
  })

  it('subscribes once for several listeners and unsubscribes on the last release', () => {
    const first = mount()
    const second = mount()
    expect(sent).toEqual([['subscribe', { topic: contentTopic }]])

    first.release()
    expect(sent).toHaveLength(1)
    second.release()
    expect(sent).toEqual([
      ['subscribe', { topic: contentTopic }],
      ['unsubscribe', { topic: contentTopic }],
    ])
  })

  it('accumulates appends onto a snapshot', () => {
    const { listener } = mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'Hel' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: 'lo ' })
    apply({ topic: id, streamID: 's1', seq: 2, op: 'append', text: 'world' })

    expect(store.read(contentSpec, contentTopic, '')).toEqual({
      value: 'Hello world',
      phase: 'streaming',
      streamID: 's1',
    })
    expect(listener).toHaveBeenCalledTimes(3)
  })

  it('drops a replayed or stale sequence number', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'a' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: 'b' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: 'DUPLICATE' })

    expect(store.read(contentSpec, contentTopic, '').value).toBe('ab')
  })

  it('ignores frames from a superseded generation', () => {
    mount()
    apply({ topic: id, streamID: 'old', seq: 0, op: 'snapshot', value: 'old value' })
    apply({ topic: id, streamID: 'new', seq: 0, op: 'snapshot', value: 'new value' })
    apply({ topic: id, streamID: 'old', seq: 9, op: 'append', text: ' LATE' })

    expect(store.read(contentSpec, contentTopic, '').value).toBe('new value')
  })

  it('holds the final value in committing until Zero catches up', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'final text' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'end' })

    // Zero still has the old row: the overlay stays visible, no flash of stale
    expect(store.read(contentSpec, contentTopic, 'old row')).toEqual({
      value: 'final text',
      phase: 'committing',
      streamID: 's1',
    })

    // the pull lands and the durable value takes over
    expect(store.read(contentSpec, contentTopic, 'final text')).toEqual({
      value: 'final text',
      phase: 'durable',
      streamID: null,
    })
  })

  it('hands off when the durable value already matches before the first read', () => {
    // the pull can land before the component renders, so the base never
    // "changes" from the store's point of view. handoff has to be an equality
    // check against the streamed value, not change detection.
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'final text' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'end' })

    expect(store.read(contentSpec, contentTopic, 'final text')).toEqual({
      value: 'final text',
      phase: 'durable',
      streamID: null,
    })
  })

  it('converges when the terminal frame is lost and the durable field changes', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'partial' })
    store.read(contentSpec, contentTopic, 'old row')

    // no `end` ever arrives, but the commit landed and Zero produced it
    expect(store.read(contentSpec, contentTopic, 'committed text')).toEqual({
      value: 'committed text',
      phase: 'durable',
      streamID: null,
    })
  })

  it('ignores later frames from a generation the database already fenced', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'partial' })
    store.read(contentSpec, contentTopic, 'old row')
    store.read(contentSpec, contentTopic, 'committed text')

    apply({ topic: id, streamID: 's1', seq: 5, op: 'append', text: ' MORE' })
    expect(store.read(contentSpec, contentTopic, 'committed text').value).toBe(
      'committed text'
    )
  })

  it('lets a NEW generation overlay a value the database already settled', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'first' })
    store.read(contentSpec, contentTopic, 'old row')
    store.read(contentSpec, contentTopic, 'committed first')

    apply({ topic: id, streamID: 's2', seq: 0, op: 'snapshot', value: 'second run' })
    expect(store.read(contentSpec, contentTopic, 'committed first')).toEqual({
      value: 'second run',
      phase: 'streaming',
      streamID: 's2',
    })
  })

  it('drops the overlay immediately on abort', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'half a sentence' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'abort' })

    expect(store.read(contentSpec, contentTopic, 'durable')).toEqual({
      value: 'durable',
      phase: 'durable',
      streamID: null,
    })
  })

  it('reveals the durable value when a producer goes quiet, and resumes on the next frame', () => {
    const { listener } = mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'stalled' })

    vi.advanceTimersByTime(1001)
    expect(listener).toHaveBeenCalledTimes(2)
    expect(store.read(contentSpec, contentTopic, 'durable')).toEqual({
      value: 'durable',
      phase: 'stale',
      streamID: 's1',
    })

    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: ' resumed' })
    expect(store.read(contentSpec, contentTopic, 'durable')).toEqual({
      value: 'stalled resumed',
      phase: 'streaming',
      streamID: 's1',
    })
  })

  it('goes stale on disconnect and resubscribes every topic on reconnect', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'mid-stream' })

    store.handleDisconnect()
    expect(store.read(contentSpec, contentTopic, 'durable').phase).toBe('stale')

    sent.length = 0
    store.handleReconnect()
    expect(sent).toEqual([['subscribe', { topic: contentTopic }]])

    // the host answers with the live generation's accumulated value
    apply({
      topic: id,
      streamID: 's1',
      seq: 7,
      op: 'snapshot',
      value: 'mid-stream and more',
    })
    expect(store.read(contentSpec, contentTopic, 'durable')).toEqual({
      value: 'mid-stream and more',
      phase: 'streaming',
      streamID: 's1',
    })
  })

  it('retries only the subscriptions the host answered pending', () => {
    mount()
    mount(contentSpec, { table: 'message', key: { id: 'm2' }, field: 'content' })
    store.handleSubscribed(id, 'pending')
    store.handleSubscribed(
      canonicalTopic(['id'], { table: 'message', key: { id: 'm2' }, field: 'content' }),
      'active'
    )

    sent.length = 0
    store.retryPending()
    expect(sent).toEqual([['subscribe', { topic: contentTopic }]])
  })

  it('stops retrying once a pending subscription becomes active', () => {
    mount()
    store.handleSubscribed(id, 'pending')
    store.handleSubscribed(id, 'active')

    sent.length = 0
    store.retryPending()
    expect(sent).toEqual([])
  })

  it('reports a host topic that does not match our canonical encoding', () => {
    mount()
    store.handleSubscribed('message content {"id":"DIFFERENT"}', 'active')
    expect(errors).toEqual([
      'host acknowledged an unknown streaming topic: message content {"id":"DIFFERENT"}',
    ])
  })

  it('reports a refused subscription', () => {
    mount()
    store.handleSubscribeError(id, 'row is not in your authorized query membership')
    expect(errors[0]).toContain('row is not in your authorized query membership')
  })

  it('drops updates for a topic it never subscribed to', () => {
    apply({
      topic: 'message content {"id":"other"}',
      streamID: 's',
      seq: 0,
      op: 'snapshot',
      value: 'x',
    })
    expect(errors).toEqual([])
  })

  it('rejects a snapshot that fails the field validator', () => {
    const partsTopic = { table: 'message', key: { id: 'm1' }, field: 'parts' } as const
    mount(partsSpec, partsTopic)
    const partsID = canonicalTopic(['id'], partsTopic)

    apply({
      topic: partsID,
      streamID: 's1',
      seq: 0,
      op: 'snapshot',
      value: { not: 'an array' },
    })
    expect(errors[0]).toContain('failed its validate()')
    expect(store.read(partsSpec, partsTopic, []).phase).toBe('durable')

    apply({
      topic: partsID,
      streamID: 's1',
      seq: 1,
      op: 'snapshot',
      value: [{ type: 'text' }],
    })
    expect(store.read(partsSpec, partsTopic, []).value).toEqual([{ type: 'text' }])
  })

  it('compares json values structurally, not by reference, for the durable handoff', () => {
    const partsTopic = { table: 'message', key: { id: 'm1' }, field: 'parts' } as const
    mount(partsSpec, partsTopic)
    const partsID = canonicalTopic(['id'], partsTopic)

    apply({
      topic: partsID,
      streamID: 's1',
      seq: 0,
      op: 'snapshot',
      value: [{ type: 'text', text: 'done' }],
    })
    apply({ topic: partsID, streamID: 's1', seq: 1, op: 'end' })

    // a structurally equal value with keys in a different order is the SAME
    // durable value: the overlay must hand off rather than treat it as a
    // concurrent write
    const fromZero = [{ text: 'done', type: 'text' }]
    store.read(partsSpec, partsTopic, fromZero)
    expect(store.read(partsSpec, partsTopic, fromZero)).toEqual({
      value: fromZero,
      phase: 'durable',
      streamID: null,
    })
  })

  it('drops an append that would exceed the field byte ceiling', () => {
    mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'x' })
    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: 'y'.repeat(512_001) })

    expect(errors[0]).toContain('exceeded maxBytes')
    expect(store.read(contentSpec, contentTopic, 'durable').phase).toBe('durable')
  })

  it('rejects a non-string snapshot on an append-mode field', () => {
    mount()
    apply({
      topic: id,
      streamID: 's1',
      seq: 0,
      op: 'snapshot',
      value: { not: 'a string' },
    })
    expect(errors[0]).toContain('received a non-string snapshot')
    expect(store.read(contentSpec, contentTopic, 'durable').phase).toBe('durable')
  })

  it('releases all state when the last listener leaves mid-stream', () => {
    const { release } = mount()
    apply({ topic: id, streamID: 's1', seq: 0, op: 'snapshot', value: 'streaming' })
    expect(store.topicCount).toBe(1)

    release()
    expect(store.topicCount).toBe(0)
    apply({ topic: id, streamID: 's1', seq: 1, op: 'append', text: 'more' })
    expect(store.read(contentSpec, contentTopic, 'durable').phase).toBe('durable')
  })
})
