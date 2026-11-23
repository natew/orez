import {
  startTransition,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
} from 'react'

import { IS_SERVER_RUNTIME } from './helpers/platform.js'

export type SnapshotDelivery = 'sync' | 'transition'

const pending = new Set<() => void>()

function schedule(update: () => void) {
  if (pending.size === 0) {
    queueMicrotask(() => {
      const updates = [...pending]
      pending.clear()
      startTransition(() => {
        for (const notify of updates) notify()
      })
    })
  }
  pending.add(update)
}

type Subscribe = (listener: () => void) => () => void

function useTransitionSnapshot<T>(
  subscribe: Subscribe,
  getSnapshot: () => T,
  getServerSnapshot?: () => T
): T {
  const source = useMemo(
    () => ({
      initial:
        IS_SERVER_RUNTIME && getServerSnapshot ? getServerSnapshot() : getSnapshot(),
      getSnapshot,
      subscribe,
    }),
    [subscribe, getSnapshot, getServerSnapshot]
  )
  const [snapshot, setSnapshot] = useState(() => ({ source, value: source.initial }))
  if (snapshot.source !== source) setSnapshot({ source, value: source.initial })

  useEffect(() => {
    let active = true
    let value = source.initial
    const update = () => {
      if (!active) return
      const next = source.getSnapshot()
      if (Object.is(value, next)) return
      value = next
      setSnapshot({ source, value })
    }
    const notify = () => schedule(update)
    const unsubscribe = source.subscribe(notify)
    // the source could have published between the initial read and subscription
    notify()
    return () => {
      active = false
      pending.delete(update)
      unsubscribe()
    }
  }, [source])

  return snapshot.source === source ? snapshot.value : source.initial
}

// choose once for the lifetime of the hook, before rendering consumers
export function selectSnapshotHook(delivery: SnapshotDelivery = 'sync') {
  return delivery === 'transition' ? useTransitionSnapshot : useSyncExternalStore
}
