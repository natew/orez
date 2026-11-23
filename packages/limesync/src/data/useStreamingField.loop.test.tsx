// @vitest-environment jsdom
//
// useSyncExternalStore's getSnapshot contract: the same reference must come
// back until something observable changes. React DEV enforces it by calling
// getSnapshot twice per render and warning "The result of getSnapshot should
// be cached to avoid an infinite loop"; in production the same instability is
// a forceStoreRerender loop that throws "Maximum update depth exceeded".
//
// The store only stabilizes SUBSCRIBED topics. An unsubscribed read — the
// render before the subscription effect lands, or every render after an error
// boundary keeps that effect from ever landing — built a fresh state object
// per call, and a factory workspace rendering a task card with an empty
// description looped React until the whole pane tree unmounted. The hooks now
// carry their own per-hook stabilization, which this file locks in.

import { act, Suspense, use } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { createLocalRealtime, defineStreamingFields } from '../realtime.js'
import { createSchema, string, table } from '../zero/zero.js'
import { createUseStreamingField, createUseStreamingFields } from './useStreamingField.js'

declare global {
  // eslint-disable-next-line no-var
  var IS_REACT_ACT_ENVIRONMENT: boolean | undefined
}
globalThis.IS_REACT_ACT_ENVIRONMENT = true

const sootTask = table('sootTask')
  .columns({ id: string(), title: string(), description: string() })
  .primaryKey('id')
const schema = createSchema({ tables: [sootTask] })

const streaming = defineStreamingFields(schema, {
  sootTask: {
    description: {
      maxBytes: 100_000,
      maxUpdatesPerSecond: 30,
      maxBytesPerSecond: 200_000,
    },
  },
})

let container: HTMLDivElement
let root: Root
let consoleError: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
  consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
})

afterEach(() => {
  act(() => root.unmount())
  container.remove()
  consoleError.mockRestore()
})

const uncachedSnapshotWarnings = () =>
  consoleError.mock.calls.filter((call) =>
    String(call[0]).includes('getSnapshot should be cached')
  )

test('single-field hook is snapshot-stable while its topic is unsubscribed', () => {
  const realtime = createLocalRealtime({ manifest: streaming.manifest })
  const useStreamingField = createUseStreamingField(() => realtime.store)

  function Card({ id, base }: { id: string; base: string }) {
    const state = useStreamingField(streaming.sootTask.description({ id }), base)
    return <div>{state.value}</div>
  }

  act(() => {
    // several consumers with DIFFERENT bases interleave in one render pass, so
    // a shared module-level cache cannot satisfy the contract — only per-hook
    // stabilization can
    root.render(
      <>
        <Card id="t1" base="" />
        <Card id="t2" base="a durable description" />
        <Card id="t3" base="" />
      </>
    )
  })

  expect(uncachedSnapshotWarnings()).toEqual([])
})

test('multi-field hook is snapshot-stable while topics are unsubscribed', () => {
  const realtime = createLocalRealtime({ manifest: streaming.manifest })
  const useStreamingFields = createUseStreamingFields(() => realtime.store)

  const requests = [
    { key: 't1', handle: streaming.sootTask.description({ id: 't1' }), base: '' },
    { key: 't2', handle: streaming.sootTask.description({ id: 't2' }), base: 'x' },
  ]

  function List() {
    const states = useStreamingFields(requests)
    return <div>{Object.keys(states).length}</div>
  }

  act(() => {
    root.render(<List />)
  })

  expect(uncachedSnapshotWarnings()).toEqual([])
})

test('a streamed replacement can suspend without hiding the committed description', async () => {
  const realtime = createLocalRealtime({ manifest: streaming.manifest })
  const useStreamingField = createUseStreamingField(() => realtime.store, 'transition')
  const topic = streaming.sootTask.description({ id: 't1' })
  const replacement = Promise.withResolvers<string>()

  function Description() {
    const { value } = useStreamingField(topic, 'Saved description')
    return <div>{value === 'Draft' ? use(replacement.promise) : value}</div>
  }

  try {
    await act(async () => {
      root.render(
        <Suspense fallback={<div>Loading</div>}>
          <Description />
        </Suspense>
      )
    })
    await act(async () => {
      const draft = await realtime.publisher.begin<string>('sootTask', 'description', {
        namespace: 'local',
        key: { id: 't1' },
      })
      draft.set('Draft')
      await draft.flush()
    })
    expect(realtime.store.read(topic, 'Saved description').value).toBe('Draft')
    expect(container.textContent).toBe('Saved description')

    await act(async () => {
      replacement.resolve('Parsed draft')
    })
    expect(container.textContent).toBe('Parsed draft')
  } finally {
    await act(async () => realtime.close())
  }
})
