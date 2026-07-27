// Local realtime against the shapes three real apps actually stream.
//
// The columns and modes here are taken from soot's schema
// (~/soot/src/database/schema-public.ts): message.content and
// sootTask.description are text columns that grow token by token, and
// message.parts is a text column holding a JSON array that is REPLACED rather
// than extended. That last one is the trap this file is most concerned with.

import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, it } from 'vitest'

import { createLocalRealtime } from './local.js'
import { defineStreamingFields } from './manifest.js'

const message = table('message')
  .columns({ id: string(), content: string(), parts: string(), streaming: string() })
  .primaryKey('id')

const sootTask = table('sootTask')
  .columns({ id: string(), title: string(), description: string() })
  .primaryKey('id')

const schema = createSchema({ tables: [message, sootTask] })

const streaming = defineStreamingFields(schema, {
  message: {
    content: { maxBytes: 1_000_000, maxUpdatesPerSecond: 30, maxBytesPerSecond: 500_000 },
    // a JSON array living in a text column: it is rewritten each time, not
    // extended, so it must opt out of the append default
    parts: {
      maxBytes: 1_000_000,
      maxUpdatesPerSecond: 15,
      maxBytesPerSecond: 500_000,
      mode: 'replace',
    },
  },
  sootTask: {
    description: {
      maxBytes: 100_000,
      maxUpdatesPerSecond: 30,
      maxBytesPerSecond: 200_000,
    },
  },
})

const contentSpec = streaming.message.content.spec
const partsSpec = streaming.message.parts.spec
const descriptionSpec = streaming.sootTask.description.spec

const settle = async () => {
  for (let index = 0; index < 4; index++) await new Promise((r) => setTimeout(r, 0))
}

describe('local realtime', () => {
  it('streams assistant text into the tab that is producing it', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(contentSpec, topic, () => {})

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    let content = ''
    for (const token of ['The ', 'agent ', 'is ', 'thinking']) {
      content += token
      session.set(content)
      await session.flush()
    }

    expect(realtime.store.read(contentSpec, topic, '').value).toBe(
      'The agent is thinking'
    )
  })

  it('streams a task description while the task is being written', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.sootTask.description({ id: 'task-1' })
    realtime.store.subscribe(descriptionSpec, topic, () => {})

    const session = await realtime.publisher.begin<string>('sootTask', 'description', {
      namespace: 'local',
      key: { id: 'task-1' },
    })
    let description = ''
    for (const token of ['Refactor ', 'the ', 'sync ', 'layer']) {
      description += token
      session.set(description)
      await session.flush()
    }

    // the card shows live text while the row's durable description is empty
    expect(realtime.store.read(descriptionSpec, topic, '')).toMatchObject({
      value: 'Refactor the sync layer',
      phase: 'streaming',
    })

    await session.finish('Refactor the sync layer', async () => {})

    // and hands off cleanly once the row is committed and Zero produces it
    expect(
      realtime.store.read(descriptionSpec, topic, 'Refactor the sync layer')
    ).toMatchObject({ phase: 'durable', streamID: null })
  })

  it('replaces a JSON-in-text column rather than appending to it', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.parts({ id: 'm1' })
    realtime.store.subscribe(partsSpec, topic, () => {})

    const session = await realtime.publisher.begin<string>('message', 'parts', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    // each write is the whole array re-serialized, exactly as soot's
    // syncLiveActiveParts does
    session.set(JSON.stringify([{ type: 'text', text: 'hi' }]))
    await session.flush()
    session.set(
      JSON.stringify([
        { type: 'text', text: 'hi' },
        { type: 'tool', name: 'read' },
      ])
    )
    await session.flush()

    expect(
      JSON.parse(realtime.store.read(partsSpec, topic, '[]').value as string)
    ).toEqual([
      { type: 'text', text: 'hi' },
      { type: 'tool', name: 'read' },
    ])
  })

  // the failure mode a JSON-in-text column hits if the manifest is wrong, and
  // the message that has to point at the fix
  it('refuses a replaced value on an append field and names the remedy', async () => {
    const appendByMistake = defineStreamingFields(schema, {
      message: {
        parts: { maxBytes: 1000, maxUpdatesPerSecond: 10, maxBytesPerSecond: 10_000 },
      },
    })
    expect(appendByMistake.message.parts.spec.mode).toBe('append')

    const realtime = createLocalRealtime({ manifest: appendByMistake.manifest })
    const session = await realtime.publisher.begin<string>('message', 'parts', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    session.set('[{"type":"text"}]')
    await settle()

    expect(() => session.set('[{"type":"tool"}]')).toThrow(/declare mode: 'replace'/)
  })

  // the property soot hand-rolls today with a scoped, deep-equal projection
  it('wakes only the subscriber whose row changed', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    let firstWakes = 0
    let secondWakes = 0
    realtime.store.subscribe(contentSpec, streaming.message.content({ id: 'm1' }), () => {
      firstWakes++
    })
    realtime.store.subscribe(contentSpec, streaming.message.content({ id: 'm2' }), () => {
      secondWakes++
    })

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    for (const token of ['a', 'ab', 'abc']) {
      session.set(token)
      await session.flush()
    }

    expect(firstWakes).toBeGreaterThan(0)
    // m2's subscriber never woke, which is the whole point: five agents
    // streaming at once must not re-render each other's rows
    expect(secondWakes).toBe(0)
  })

  it('runs several concurrent generations without crosstalk', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topics = ['m1', 'm2', 'm3'].map((id) => streaming.message.content({ id }))
    for (const topic of topics) realtime.store.subscribe(contentSpec, topic, () => {})

    const sessions = await Promise.all(
      ['m1', 'm2', 'm3'].map((id) =>
        realtime.publisher.begin<string>('message', 'content', {
          namespace: 'local',
          key: { id },
        })
      )
    )
    for (let step = 1; step <= 4; step++) {
      sessions.forEach((session, index) => session.set(`agent-${index}-`.repeat(step)))
      await Promise.all(sessions.map((session) => session.flush()))
    }

    topics.forEach((topic, index) => {
      expect(realtime.store.read(contentSpec, topic, '').value).toBe(
        `agent-${index}-`.repeat(4)
      )
    })
  })

  it('reveals the durable value when a generation is abandoned', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(contentSpec, topic, () => {})

    const session = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    session.set('half an answer')
    await session.flush()
    await session.abort()

    expect(realtime.store.read(contentSpec, topic, 'last committed')).toMatchObject({
      value: 'last committed',
      phase: 'durable',
    })
  })

  // soot ends a generation at a semantic checkpoint (a completed tool result)
  // and starts a new one from the committed value
  it('supports a checkpoint: commit, then start a new generation', async () => {
    const realtime = createLocalRealtime({ manifest: streaming.manifest })
    const topic = streaming.message.content({ id: 'm1' })
    realtime.store.subscribe(contentSpec, topic, () => {})

    const first = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    first.set('before the tool call')
    await first.flush()
    let durable = ''
    await first.finish('before the tool call', async () => {
      durable = 'before the tool call'
    })
    expect(realtime.store.read(contentSpec, topic, durable).phase).toBe('durable')

    const second = await realtime.publisher.begin<string>('message', 'content', {
      namespace: 'local',
      key: { id: 'm1' },
    })
    second.set(`${durable} and after it`)
    await second.flush()

    expect(realtime.store.read(contentSpec, topic, durable)).toMatchObject({
      value: 'before the tool call and after it',
      phase: 'streaming',
    })
  })
})
