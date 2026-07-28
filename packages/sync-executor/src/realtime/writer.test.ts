// The FieldWriter against the call pattern soot's agent loop actually uses:
// a synchronous set(id, value) on every model event, with no session
// bookkeeping, then a durable write at turn end.

import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, it } from 'vitest'

import { createLocalRealtime } from './local.js'
import { defineStreamingFields } from './manifest.js'

const message = table('message')
  .columns({ id: string(), content: string(), parts: string() })
  .primaryKey('id')

const schema = createSchema({ tables: [message] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 60,
      maxBytesPerSecond: 1_000_000,
    },
    parts: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 30,
      maxBytesPerSecond: 1_000_000,
      mode: 'replace',
    },
  },
})

const contentSpec = streaming.message.content.spec
const settle = async () => {
  for (let index = 0; index < 6; index++) await new Promise((r) => setTimeout(r, 0))
}

describe('field writer', () => {
  it('opens a generation lazily on the first set', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(topic, () => {})

    expect(realtime.fields.isStreaming(topic)).toBe(false)
    realtime.fields.set(topic, 'first token')
    expect(realtime.fields.isStreaming(topic)).toBe(true)

    await realtime.fields.flush(topic)
    expect(realtime.store.read(topic, '').value).toBe('first token')
  })

  // the shape of soot's message_update handler: no awaits in the token loop
  it('accepts synchronous sets before the generation has opened', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(topic, () => {})

    let content = ''
    for (const token of ['The ', 'agent ', 'writes ', 'without ', 'awaiting']) {
      content += token
      realtime.fields.set(topic, content)
    }
    await realtime.fields.flush(topic)

    expect(realtime.store.read(topic, '').value).toBe('The agent writes without awaiting')
  })

  it('runs the durable write and hands the overlay off', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(topic, () => {})

    realtime.fields.set(topic, 'the whole answer')
    await realtime.fields.flush(topic)

    let committed: string | undefined
    await realtime.fields.finish(topic, 'the whole answer', async () => {
      committed = 'the whole answer'
    })

    expect(committed).toBe('the whole answer')
    expect(realtime.store.read(topic, 'old row').phase).toBe('committing')
    expect(realtime.store.read(topic, committed!)).toMatchObject({
      phase: 'durable',
      streamID: null,
    })
    expect(realtime.fields.isStreaming(topic)).toBe(false)
  })

  // a row that streamed nothing still has to be written
  it('runs the commit even when nothing was ever streamed', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'never-streamed' })
    let committed = false
    await realtime.fields.finish(topic, 'value', async () => {
      committed = true
    })
    expect(committed).toBe(true)
  })

  it('starts a new generation when a row streams again after finishing', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(topic, () => {})

    realtime.fields.set(topic, 'first turn')
    await realtime.fields.flush(topic)
    await realtime.fields.finish(topic, 'first turn', async () => {})

    realtime.fields.set(topic, 'first turn, second pass')
    await realtime.fields.flush(topic)

    expect(realtime.store.read(topic, 'first turn')).toMatchObject({
      value: 'first turn, second pass',
      phase: 'streaming',
    })
  })

  it('drops the overlay on abort, revealing the durable row', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(topic, () => {})

    realtime.fields.set(topic, 'half an answer')
    await realtime.fields.flush(topic)
    await realtime.fields.abort(topic)

    expect(realtime.store.read(topic, 'durable row')).toMatchObject({
      value: 'durable row',
      phase: 'durable',
    })
  })

  it('aborts every open generation when a producer shuts down', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topics = ['m1', 'm2', 'm3'].map((id) => streaming.message.content({ id }))
    for (const topic of topics) {
      realtime.store.subscribe(topic, () => {})
      realtime.fields.set(topic, 'interrupted')
    }
    await settle()
    await realtime.fields.abortAll()

    for (const topic of topics) {
      expect(realtime.store.read(topic, 'durable').phase).toBe('durable')
      expect(realtime.fields.isStreaming(topic)).toBe(false)
    }
  })

  it('keeps several rows independent', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const first = streaming.message.content({ id: 'm1' })
    const second = streaming.message.content({ id: 'm2' })
    realtime.store.subscribe(first, () => {})
    realtime.store.subscribe(second, () => {})

    realtime.fields.set(first, 'agent one')
    realtime.fields.set(second, 'agent two')
    await realtime.fields.flush(first)
    await realtime.fields.flush(second)

    expect(realtime.store.read(first, '').value).toBe('agent one')
    expect(realtime.store.read(second, '').value).toBe('agent two')
  })

  // a presentation failure must never take down the model run that is writing
  it('reports a bad value instead of throwing into the producer loop', async () => {
    const errors: string[] = []
    const realtime = createLocalRealtime({
      manifest: streaming.manifest,
      onError: (message) => errors.push(message),
    })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.fields.set(topic, 'hello world')
    await realtime.fields.flush(topic)

    // append mode: a value that stops extending is a producer bug, but it is
    // still only a display concern
    expect(() => realtime.fields.set(topic, 'goodbye')).not.toThrow()
    await settle()
    expect(errors.some((message) => message.includes('not an extension'))).toBe(true)
  })
})
