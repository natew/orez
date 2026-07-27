// React bindings for streaming fields.
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
// Mounting a hook is what subscribes. A component that renders the row without
// one receives no field traffic at all.
//
// Both hooks are selector-isolated: a subscription covers exactly one row's one
// field, so a value arriving for another row cannot wake this component. That
// is the property an app hand-rolls otherwise, usually as a global emitter plus
// a deep-equal projection to undo the over-broadcasting.

import { canonicalTopic } from 'orez-lite/realtime'
import { useCallback, useMemo, useSyncExternalStore } from 'react'

import type {
  RealtimeStore,
  StreamingFieldHandle,
  StreamingFieldState,
} from 'orez-lite/realtime'

// `streaming.message.content({ id })` returns a StreamingFieldHandle: the row's
// topic plus the manifest spec for that column, so a hook needs no lookup and
// cannot be handed a field the manifest does not declare.
export type { StreamingFieldHandle }

// `handle` may be null so a component can subscribe conditionally while still
// calling the hook unconditionally, which is the common React shape: a row that
// is not streaming, or whose id is not known yet, has nothing to subscribe to.
export type UseStreamingField = <Value>(
  handle: StreamingFieldHandle | null | undefined,
  base: Value
) => StreamingFieldState<Value>

// One row's field, keyed by whatever the caller already uses to identify the
// row (a message id, usually), so results can be looked up without rebuilding
// a canonical topic at the call site.
export type StreamingFieldRequest<Value = unknown> = {
  readonly key: string
  readonly handle: StreamingFieldHandle
  readonly base: Value
}

export type UseStreamingFields = <Value>(
  requests: readonly StreamingFieldRequest<Value>[]
) => Readonly<Record<string, StreamingFieldState<Value>>>

// The store lives on the transport (or on a local realtime), so the hooks are
// created against whichever store the application installed rather than
// reaching for a module-level singleton.
export function createUseStreamingField(
  getStore: () => RealtimeStore | undefined
): UseStreamingField {
  return function useStreamingField<Value>(
    handle: StreamingFieldHandle | null | undefined,
    base: Value
  ): StreamingFieldState<Value> {
    const store = getStore()
    // the canonical topic, not object identity: a caller that rebuilds `{id}`
    // inline on every render must not churn its subscription
    const id = handle ? canonicalTopic(handle.spec.primaryKey, handle.topic) : ''

    const subscribe = useCallback(
      (onChange: () => void) => {
        if (!store || !handle) return () => {}
        return store.subscribe(handle, onChange)
      },
      [store, id]
    )

    // `store.read` returns a reference-stable state: useSyncExternalStore
    // re-invokes getSnapshot on every render and loops forever if the result is
    // a fresh object each time. `base` participates because a new durable value
    // from Zero is what ends the committing phase.
    const getSnapshot = useCallback(() => {
      if (!store || !handle) return durableOnly(base)
      return store.read(handle, base)
    }, [store, id, base])

    return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  }
}

// Many rows' fields at once, for a list that has to merge live values before it
// renders (grouping consecutive tool parts, interleaving by timestamp, and so
// on) and therefore cannot push the subscription down into each row.
//
// Scoping is structural: this component subscribes to exactly the topics in
// `requests`, so a value for a row outside the list cannot wake it. There is no
// projection to write and no deep-equal comparator to get right.
export function createUseStreamingFields(
  getStore: () => RealtimeStore | undefined
): UseStreamingFields {
  return function useStreamingFields<Value>(
    requests: readonly StreamingFieldRequest<Value>[]
  ): Readonly<Record<string, StreamingFieldState<Value>>> {
    const store = getStore()

    // identity of the subscription SET, so adding or removing a row
    // resubscribes but a changing base value does not
    const topicsKey = useMemo(
      () =>
        requests
          .map(
            (request) =>
              `${request.key}=${canonicalTopic(request.handle.spec.primaryKey, request.handle.topic)}`
          )
          .join('\n'),
      [requests]
    )

    const subscribe = useCallback(
      (onChange: () => void) => {
        if (!store) return () => {}
        const releases = requests.map((request) =>
          store.subscribe(request.handle, onChange)
        )
        return () => {
          for (const release of releases) release()
        }
      },
      [store, topicsKey]
    )

    // Reference-stable across renders where nothing observable changed, both
    // for useSyncExternalStore's contract and so a consuming useMemo (soot
    // merges these into its transcript) is not invalidated every render.
    const cache = useMemo(
      () => ({ value: {} as Record<string, StreamingFieldState<Value>> }),
      [store, topicsKey]
    )

    const getSnapshot = useCallback(() => {
      const next: Record<string, StreamingFieldState<Value>> = {}
      let changed = false
      for (const request of requests) {
        const state = store
          ? store.read(request.handle, request.base)
          : durableOnly(request.base)
        next[request.key] = state
        if (cache.value[request.key] !== state) changed = true
      }
      if (!changed && Object.keys(cache.value).length === Object.keys(next).length) {
        return cache.value
      }
      cache.value = next
      return next
    }, [store, topicsKey, cache, requests])

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
