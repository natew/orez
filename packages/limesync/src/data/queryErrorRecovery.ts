import { useEffect, useMemo, useRef, useSyncExternalStore } from 'react'

// zero's ArrayView settles its result status once. A changed row can repair an
// errored query, but the existing view must be rematerialized to become complete.
export class QueryErrorRecovery {
  private data: string | undefined
  private errored = false

  changed(data: unknown, resultType: string): boolean {
    if (resultType !== 'error') {
      this.errored = false
      this.data = undefined
      return false
    }
    const current = JSON.stringify(data)
    const changed = this.errored && current !== this.data
    this.errored = true
    this.data = current
    return changed
  }
}

type QueryFault = { name: string; args: readonly unknown[]; message: string }

// The HTTP transport reports the complete set of failed queries for a pull.
// Zero's already-complete ArrayView cannot observe a later transform error.
export class QueryFaults {
  private faults = new Map<string, QueryFault>()
  private listeners = new Set<() => void>()

  constructor(readonly retry: () => void) {}

  update(faults: readonly QueryFault[]) {
    const next = new Map<string, QueryFault>()
    for (const fault of faults) {
      const key = JSON.stringify([fault.name, fault.args])
      const old = this.faults.get(key)
      next.set(key, old?.message === fault.message ? old : fault)
    }
    if (
      next.size === this.faults.size &&
      [...next].every(([key, fault]) => this.faults.get(key) === fault)
    )
      return
    this.faults = next
    for (const listener of this.listeners) listener()
  }

  get(key: string) {
    return this.faults.get(key)
  }

  subscribe = (listener: () => void) => {
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }
}

const noSubscribe = () => () => {}
const noSnapshot = () => undefined

export function useQueryFault(
  faults: QueryFaults | undefined,
  request: { query: { queryName: string }; args?: unknown },
  out: readonly [unknown, { type: string; retry?: () => void }]
) {
  const key = JSON.stringify([
    request.query.queryName,
    request.args === undefined ? [] : [request.args],
  ])
  const getSnapshot = useMemo(() => () => faults?.get(key), [faults, key])
  const fault = useSyncExternalStore(
    faults?.subscribe ?? noSubscribe,
    getSnapshot,
    noSnapshot
  )
  const previous = useRef(false)
  useEffect(() => {
    if (previous.current && !fault && out[1].type === 'error') out[1].retry?.()
    previous.current = !!fault
  }, [fault, out])
  const status = useMemo(
    () =>
      fault && faults
        ? {
            type: 'error' as const,
            error: { type: 'app' as const, message: fault.message },
            retry: faults.retry,
            refetch: faults.retry,
          }
        : undefined,
    [fault, faults]
  )
  return status ? ([out[0], status] as const) : out
}
