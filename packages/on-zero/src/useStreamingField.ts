// React binding for a streaming field.
//
// The base value is passed in explicitly rather than the hook reaching into the
// Zero query result. Two reasons: Zero query results are not mutated behind the
// application's back, and the moment of durable handoff stays visible to UI
// that wants a "saving" affordance.
//
//   const [message] = useQuery(queries.message.byID({ id }))
//   const content = useStreamingField(streaming.message.content({ id }), message.content)
//   return <Markdown>{content.value}</Markdown>
//
// Mounting the hook is what subscribes. A component that renders the row
// without the hook receives no field traffic at all.

import { canonicalTopic } from 'orez-lite/realtime'
import { useCallback, useSyncExternalStore } from 'react'

import type {
  RealtimeStore,
  RealtimeTopic,
  StreamingFieldSpec,
  StreamingFieldState,
} from 'orez-lite/realtime'

// A topic paired with the spec it came from. `streaming.message.content({id})`
// returns the topic; the spec rides along so the hook needs no manifest lookup.
export type StreamingFieldHandle = {
  readonly topic: RealtimeTopic
  readonly spec: StreamingFieldSpec
}

export type UseStreamingField = <Value>(
  handle: StreamingFieldHandle,
  base: Value
) => StreamingFieldState<Value>

// The store lives on the transport, so the hook is created against whichever
// store the client installed rather than reaching for a module-level singleton.
export function createUseStreamingField(
  getStore: () => RealtimeStore | undefined
): UseStreamingField {
  return function useStreamingField<Value>(
    handle: StreamingFieldHandle,
    base: Value
  ): StreamingFieldState<Value> {
    const store = getStore()
    // the canonical topic, not object identity: a caller that rebuilds `{id}`
    // inline on every render must not churn its subscription
    const id = canonicalTopic(handle.spec.primaryKey, handle.topic)

    const subscribe = useCallback(
      (onChange: () => void) => {
        if (!store) return () => {}
        return store.subscribe(handle.spec, handle.topic, onChange)
      },
      [store, handle.spec, id]
    )

    // `store.read` returns a reference-stable state: useSyncExternalStore
    // re-invokes getSnapshot on every render and loops forever if the result is
    // a fresh object each time. `base` participates because a new durable value
    // from Zero is what ends the committing phase.
    const getSnapshot = useCallback(() => {
      if (!store) return durableOnly(base)
      return store.read(handle.spec, handle.topic, base)
    }, [store, handle.spec, id, base])

    return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  }
}

const DURABLE_CACHE = new WeakMap<object, StreamingFieldState<unknown>>()
let lastPrimitiveBase: unknown
let lastPrimitiveState: StreamingFieldState<unknown> | undefined

// Unsubscribed reads still have to be reference-stable for the same reason.
function durableOnly<Value>(base: Value): StreamingFieldState<Value> {
  if (base !== null && typeof base === 'object') {
    const cached = DURABLE_CACHE.get(base)
    if (cached) return cached as StreamingFieldState<Value>
    const state = { value: base, phase: 'durable', streamID: null } as const
    DURABLE_CACHE.set(base, state as StreamingFieldState<unknown>)
    return state as StreamingFieldState<Value>
  }
  if (lastPrimitiveState && Object.is(lastPrimitiveBase, base)) {
    return lastPrimitiveState as StreamingFieldState<Value>
  }
  lastPrimitiveBase = base
  lastPrimitiveState = { value: base, phase: 'durable', streamID: null }
  return lastPrimitiveState as StreamingFieldState<Value>
}
