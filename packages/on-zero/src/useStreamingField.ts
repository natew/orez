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

    // getSnapshot must hand back the SAME reference until something observable
    // changes: useSyncExternalStore re-invokes it on every render and rerenders
    // forever (throwing "Maximum update depth exceeded") if each call returns a
    // fresh object. The store only stabilizes SUBSCRIBED topics — an
    // unsubscribed read (before the subscription effect lands, after an error
    // boundary unmounts the tree so it never lands, or with a null handle) has
    // no entry to cache on. Each hook therefore carries its own last-state
    // slot, which also survives interleaved renders of many hooks in a way a
    // module-level cache cannot. `base` participates because a new durable
    // value from Zero is what ends the committing phase.
    const last = useRef<StreamingFieldState<Value> | undefined>(undefined)
    const getSnapshot = useCallback(() => {
      const next =
        !store || !handle ? durableState(base) : store.read(handle, base)
      const cached = last.current
      if (cached && sameState(cached, next)) return cached
      last.current = next
      return next
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
      const previous = cache.value
      const next: Record<string, StreamingFieldState<Value>> = {}
      let changed = false
      for (const request of requests) {
        const raw = store
          ? store.read(request.handle, request.base)
          : durableState(request.base)
        // stabilize per key here, not just per store entry: an unsubscribed
        // topic's read is a fresh object each call, and one unstable member
        // would otherwise force a fresh map from every getSnapshot — the same
        // rerender loop the single-field hook guards against.
        const cached = previous[request.key]
        const state = cached && sameState(cached, raw) ? cached : raw
        next[request.key] = state
        if (state !== cached) changed = true
      }
      if (!changed && Object.keys(previous).length === Object.keys(next).length) {
        return previous
      }
      cache.value = next
      return next
    }, [store, topicsKey, cache, requests])

    return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  }
}

function sameState(
  a: StreamingFieldState<unknown>,
  b: StreamingFieldState<unknown>
): boolean {
  // exact: `value` is either the caller's own base reference or the store
  // generation's accumulated value, both of which only change identity when
  // they actually change
  return Object.is(a.value, b.value) && a.phase === b.phase && a.streamID === b.streamID
}

function durableState<Value>(base: Value): StreamingFieldState<Value> {
  return { value: base, phase: 'durable', streamID: null }
}
