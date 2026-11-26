import {
  useContext,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
  type Context,
} from 'react'

import { addContextToQuery, asQueryInternals } from '../zero/bindings.js'
import { useQuery as zeroUseQuery, useZero } from '../zero/react.js'
import { IS_SERVER_RUNTIME } from './helpers/platform.js'
import { useZeroDebug } from './helpers/useZeroDebug.js'
import { classifyQueryAst, type LocalDataStore } from './localStore.js'
import {
  QueryErrorRecovery,
  useQueryFault,
  type QueryFaults,
} from './queryErrorRecovery.js'
import { serializeQueryParams } from './queryJson.js'
import { resolveQuery, type PlainQueryFn } from './resolveQuery.js'
import { selectSnapshotHook, type SnapshotDelivery } from './snapshot.js'

import type {
  AnyQueryRegistry,
  HumanReadable,
  Query,
  Schema as ZeroSchema,
  TTL,
} from '../zero/zero.js'

// false = enabled, 'empty' = disabled (return empty), 'last-value' = disabled (return cached)
export type QueryControlMode = false | 'empty' | 'last-value'

export type UseQueryOptions = {
  enabled?: boolean | undefined
  ttl?: TTL | undefined
}

type QueryResultDetails = ReturnType<typeof zeroUseQuery>[1]
export type QueryResult<TReturn> = readonly [HumanReadable<TReturn>, QueryResultDetails]

export type { PlainQueryFn }

export type UseQueryHook<Schema extends ZeroSchema> = {
  // overload 1: inline local query expression (e.g. `query.uiShell.where('id', 'main').one()`)
  <TTable extends keyof Schema['tables'] & string, TReturn>(
    queryExpr: Query<TTable, Schema, TReturn>,
    options?: UseQueryOptions | boolean
  ): QueryResult<TReturn>

  // overload 2: plain function with params
  <TArg, TTable extends keyof Schema['tables'] & string, TReturn>(
    fn: PlainQueryFn<TArg, Query<TTable, Schema, TReturn>>,
    params: TArg,
    options?: UseQueryOptions | boolean
  ): QueryResult<TReturn>

  // overload 3: plain function with no params
  <TTable extends keyof Schema['tables'] & string, TReturn>(
    fn: PlainQueryFn<void, Query<TTable, Schema, TReturn>>,
    options?: UseQueryOptions | boolean
  ): QueryResult<TReturn>
}

// shape the "empty" / loading / disabled response to match the typed contract:
// plural queries get [], singular queries get undefined. returning null for a
// plural query (the old `EMPTY_RESPONSE` constant) broke callers that do the
// obvious .filter / .find / .length on the result during the first render.
const EMPTY_PLURAL = Object.freeze([])
const RESULT_UNKNOWN = Object.freeze({ type: 'unknown' as const })
const RESULT_COMPLETE = Object.freeze({ type: 'complete' as const })
const EMPTY_RESPONSE_PLURAL = Object.freeze([EMPTY_PLURAL, RESULT_UNKNOWN]) as never
const EMPTY_RESPONSE_SINGULAR = Object.freeze([undefined, RESULT_UNKNOWN]) as never

function isInlineQueryObject(value: unknown): value is Query<any, any, any> {
  return Boolean(value && typeof value === 'object' && 'ast' in value)
}

function useLocalQuerySubscription(
  localStore: LocalDataStore | null,
  localQuery: Query<any, any, any> | null,
  enabled: boolean
): readonly [unknown, { type: 'unknown' | 'complete' }] | null {
  const subscriptionKey = useMemo(() => {
    if (!localQuery || !enabled) return null
    try {
      const internals = asQueryInternals(localQuery)
      return `${internals.hash()}|${JSON.stringify(internals.format)}`
    } catch {
      return null
    }
  }, [localQuery, enabled])

  const latestQueryRef = useRef(localQuery)
  latestQueryRef.current = localQuery

  const activeSubRef = useRef<{
    store: LocalDataStore
    key: string
    view: ReturnType<LocalDataStore['materialize']>
  } | null>(null)

  const snapshotCacheRef = useRef<{
    store: LocalDataStore
    key: string
    version: number
    ready: boolean
    value: readonly [unknown, { type: 'unknown' | 'complete' }]
  } | null>(null)

  const subscribe = useMemo(
    () => (onStoreChange: () => void) => {
      const currentQuery = latestQueryRef.current
      if (!localStore || !currentQuery || !subscriptionKey || !enabled) {
        return () => {}
      }
      const view = localStore.materialize(currentQuery as any)
      activeSubRef.current = { store: localStore, key: subscriptionKey, view }

      const updateSnapshot = (data: unknown): boolean => {
        const prev = snapshotCacheRef.current
        const ready = localStore.ready
        if (
          prev &&
          prev.store === localStore &&
          prev.key === subscriptionKey &&
          prev.ready === ready &&
          Object.is(prev.value[0], data)
        ) {
          prev.version = localStore.version
          return false
        }
        const nextTuple: readonly [unknown, { type: 'unknown' | 'complete' }] = [
          data,
          ready ? RESULT_COMPLETE : RESULT_UNKNOWN,
        ]
        snapshotCacheRef.current = {
          store: localStore,
          key: subscriptionKey,
          version: localStore.version,
          ready,
          value: nextTuple,
        }
        return true
      }

      if (updateSnapshot(view.data)) {
        onStoreChange()
      }

      let attaching = true
      const unsubView = view.addListener((data) => {
        const changed = updateSnapshot(data)
        if (!attaching && changed) {
          onStoreChange()
        }
      })
      attaching = false
      const unsubReady = localStore.subscribeReady(() => {
        if (updateSnapshot(view.data)) {
          onStoreChange()
        }
      })

      return () => {
        unsubView()
        unsubReady()
        view.destroy()
        if (activeSubRef.current?.view === view) {
          activeSubRef.current = null
        }
      }
    },
    [localStore, subscriptionKey, enabled]
  )

  const getSnapshot = useMemo(
    () => () => {
      const currentQuery = latestQueryRef.current
      if (!localStore || !currentQuery || !subscriptionKey || !enabled) return null
      const cached = snapshotCacheRef.current
      if (
        cached &&
        cached.store === localStore &&
        cached.key === subscriptionKey &&
        cached.version === localStore.version &&
        cached.ready === localStore.ready
      ) {
        return cached.value
      }
      const active = activeSubRef.current
      const data =
        active && active.store === localStore && active.key === subscriptionKey
          ? active.view.data
          : localStore.readSnapshot(currentQuery as any)
      if (
        cached &&
        cached.store === localStore &&
        cached.key === subscriptionKey &&
        cached.ready === localStore.ready &&
        Object.is(cached.value[0], data)
      ) {
        cached.version = localStore.version
        return cached.value
      }
      const value: readonly [unknown, { type: 'unknown' | 'complete' }] = [
        data,
        localStore.ready ? RESULT_COMPLETE : RESULT_UNKNOWN,
      ]
      snapshotCacheRef.current = {
        store: localStore,
        key: subscriptionKey,
        version: localStore.version,
        ready: localStore.ready,
        value,
      }
      return value
    },
    [localStore, subscriptionKey, enabled]
  )

  const getServerSnapshot = useMemo(
    () => () => {
      const currentQuery = latestQueryRef.current
      return currentQuery ? emptyResponseFor(currentQuery) : null
    },
    [subscriptionKey]
  )

  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot as any) as any
}

// inspect a resolved query (a Query OR a QueryRequest, i.e. what resolveQuery
// returns from a defineQueries-registered fn) to find its singular/plural
// format. QueryRequests carry a `.query.fn({args, ctx})` that materializes
// the underlying Query — singular vs plural is determined entirely by the
// user-authored query body (`.one()` vs not), so passing an empty `ctx` is
// safe here.
export function emptyResponseFor(
  queryRequest: unknown
): readonly [unknown, { type: string }] {
  try {
    const query = addContextToQuery(queryRequest as any, {} as never)
    const internals = asQueryInternals(query)
    return internals.format.singular ? EMPTY_RESPONSE_SINGULAR : EMPTY_RESPONSE_PLURAL
  } catch {
    // SSG factory / no-resolved-query — default to plural so callers' iteration
    // (.filter / .find / .length / for-of) is always safe.
    return EMPTY_RESPONSE_PLURAL
  }
}

// determine if useQuery-style args are (fn, params, options) or (fn, options)
export function parseUseQueryArgs(paramsOrOptions: any, optionsArg: any) {
  const hasParams =
    optionsArg !== undefined ||
    (paramsOrOptions &&
      typeof paramsOrOptions === 'object' &&
      !('enabled' in paramsOrOptions) &&
      !('ttl' in paramsOrOptions))

  return {
    params: hasParams ? paramsOrOptions : undefined,
    options: hasParams ? optionsArg : paramsOrOptions,
  }
}

export function createUseQuery<Schema extends ZeroSchema>({
  DisabledContext,
  LocalDataContext,
  customQueries,
  getQueryFaults,
  getQueryScope,
  snapshotDelivery,
}: {
  DisabledContext: Context<QueryControlMode>
  LocalDataContext?: Context<LocalDataStore | null>
  customQueries: AnyQueryRegistry
  getQueryFaults: (zero: object) => QueryFaults | undefined
  getQueryScope?: (zero: object) => number | undefined
  snapshotDelivery?: SnapshotDelivery
}): UseQueryHook<Schema> {
  const useSnapshot = selectSnapshotHook(snapshotDelivery)
  if (IS_SERVER_RUNTIME) {
    return ((fnOrQuery: any, paramsOrOptions: any, optionsArg: any) => {
      if (isInlineQueryObject(fnOrQuery)) {
        return emptyResponseFor(fnOrQuery)
      }
      const { params } = parseUseQueryArgs(paramsOrOptions, optionsArg)
      const queryRequest = resolveQuery({ customQueries, fn: fnOrQuery, params })
      return emptyResponseFor(queryRequest)
    }) as UseQueryHook<Schema>
  }

  function useQuery(...args: any[]): any {
    const disableMode = useContext(DisabledContext)
    const localStore = LocalDataContext ? useContext(LocalDataContext) : null
    const lastRef = useRef<any>(EMPTY_RESPONSE_PLURAL)
    const [fnOrQuery, paramsOrOptions, optionsArg] = args

    const isInline = isInlineQueryObject(fnOrQuery)
    const { params, options } = isInline
      ? { params: undefined, options: paramsOrOptions }
      : parseUseQueryArgs(paramsOrOptions, optionsArg)

    // extract option values as primitives — a fresh options object each render
    // shouldn't propagate as a new ref into zero-react.
    let optionEnabled: boolean | undefined
    let optionTTL: TTL | undefined
    if (typeof options === 'boolean') {
      optionEnabled = options
    } else if (options) {
      optionEnabled = options.enabled
      optionTTL = options.ttl
    }

    const paramsKey = isInline ? '' : serializeQueryParams(fnOrQuery, params)

    const resolved = useMemo(() => {
      if (isInline) {
        const internals = asQueryInternals(fnOrQuery)
        const tableNames = localStore?.tableNames ?? new Set<string>()
        const kind = classifyQueryAst(internals.ast, tableNames)
        if (kind !== 'local') {
          throw new Error(
            `[limesync] inline query expressions in useQuery are only supported for .local() tables. ` +
              `Use a named query function for synced table '${internals.ast.table}'.`
          )
        }
        return { kind: 'local' as const, localQuery: fnOrQuery, queryRequest: fnOrQuery }
      }
      // check if this plain function returns a local-only query (unregistered local helper)
      if (localStore && localStore.tableNames.size > 0) {
        try {
          const candidate = fnOrQuery(params)
          if (isInlineQueryObject(candidate)) {
            const internals = asQueryInternals(candidate)
            const kind = classifyQueryAst(internals.ast, localStore.tableNames)
            if (kind === 'local') {
              return {
                kind: 'local' as const,
                localQuery: candidate,
                queryRequest: candidate,
              }
            }
          }
        } catch (err) {
          if (err instanceof Error && err.message.includes('mixed local and synced')) {
            throw err
          }
          // fall through to synced registry resolution
        }
      }
      const queryRequest = resolveQuery({ customQueries, fn: fnOrQuery, params })
      return { kind: 'sync' as const, localQuery: null, queryRequest }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fnOrQuery, isInline, paramsKey, localStore])

    const isLocal = resolved.kind === 'local'
    const localResult = useLocalQuerySubscription(
      isLocal ? localStore : null,
      resolved.localQuery,
      isLocal && optionEnabled !== false
    )

    // when disabled or routed to local store, force enabled=false through to
    // zero's useQuery so its viewStore stays completely inert while preserving
    // unconditional hook order.
    const effectiveEnabled = isLocal || disableMode ? false : optionEnabled
    const effectiveOptions = useMemo(
      () => ({ enabled: effectiveEnabled, ttl: optionTTL }),
      [effectiveEnabled, optionTTL]
    )
    const zero = useZero()
    const scope = getQueryScope?.(zero)
    const reactQuery = useMemo(() => {
      if (isLocal || scope === undefined) return resolved.queryRequest
      const query = asQueryInternals(
        addContextToQuery(resolved.queryRequest, zero.context)
      )
      // zero-react keys its global views by clientID, which a native store retains across opens.
      return new Proxy(query, {
        get(target, property) {
          if (property === 'hash') return () => `${target.hash()}:external:${scope}`
          const value = Reflect.get(target, property, target)
          return typeof value === 'function' ? value.bind(target) : value
        },
      })
    }, [resolved.queryRequest, zero, scope, isLocal])
    const rawOut = zeroUseQuery(reactQuery, effectiveOptions)
    const faultOut = useQueryFault(
      isLocal || effectiveEnabled === false ? undefined : getQueryFaults(zero),
      resolved.queryRequest,
      rawOut,
      useSnapshot
    )
    const recovery = useMemo(() => new QueryErrorRecovery(), [resolved.queryRequest])
    useEffect(() => {
      if (
        !isLocal &&
        recovery.changed(rawOut[0], rawOut[1].type) &&
        rawOut[1].type === 'error'
      ) {
        rawOut[1].retry?.()
      }
    }, [rawOut, recovery, isLocal])

    // normalize the rare null-data first snapshot for plural queries to []:
    // zero-react's viewStore briefly hands us [null, {type:'unknown'}] when a
    // stub Zero is wired up (e.g. during the disable→active transition) before
    // the real materialize fires. useQuery is typed as T[] for plural so
    // callers must never see null here. cheap drop-in: only allocates a new
    // tuple in the (rare) null case; the steady-state pass-through is one
    // identity check per render.
    let out: any = isLocal ? localResult : faultOut
    if (!isLocal && faultOut?.[0] === null) {
      const fallback = emptyResponseFor(resolved.queryRequest)
      if (fallback === EMPTY_RESPONSE_PLURAL) {
        out = [EMPTY_PLURAL, faultOut[1] ?? RESULT_UNKNOWN]
      }
    }

    // call debug instrumentation unconditionally before any early return so
    // switching a mounted hook between local and sync keeps hook order stable.
    if (process.env.NODE_ENV === 'development') {
      if (process.env.DEBUG_ZERO_QUERIES === '1')
        // eslint-disable-next-line react-hooks/rules-of-hooks
        useZeroDebug(resolved.queryRequest, isLocal ? { enabled: false } : options, out)
    }

    if (isLocal) {
      if (optionEnabled === false) {
        return emptyResponseFor(resolved.localQuery)
      }
      if (!localStore) {
        throw new Error(
          `[limesync] local table query requires mounting <DataProvider local={{ storageKey }} ...> with localSchema.`
        )
      }
      return localResult ?? emptyResponseFor(resolved.localQuery)
    }

    if (!disableMode) {
      lastRef.current = out
      return out
    }

    if (disableMode === 'last-value') {
      // first render under last-value mode: lastRef is still the plural default —
      // reshape to match this query's actual format so a singular query gets
      // undefined instead of [].
      if (lastRef.current === EMPTY_RESPONSE_PLURAL) {
        return emptyResponseFor(resolved.queryRequest)
      }
      return lastRef.current
    }

    return emptyResponseFor(resolved.queryRequest)
  }

  return useQuery as UseQueryHook<Schema>
}
