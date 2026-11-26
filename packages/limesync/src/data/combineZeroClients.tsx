import { createEmitter } from './helpers/emitter.js'
import { combineMutationLifecycles } from './helpers/mutationLifecycle.js'
import { getInstanceForNamespace, getInstanceForQueryFn } from './instanceRegistry.js'
import { run } from './run.js'

import type {
  CombinedMutationLifecycle,
  MutationLifecycle,
} from './helpers/mutationLifecycle.js'
import type { LocalDataStore } from './localStore.js'
import type { ZeroEvent, ZeroEventsEmitter } from './types.js'
import type { ReactNode } from 'react'

// combines multiple createDataClient instances into one consumer surface:
// useQuery/run/preload/getQuery dispatch to the instance that claimed the
// query fn's namespace (or the local-schema owner for local AST queries),
// mutate.<namespace> dispatches by model/local table namespace, mutation
// acknowledgement (awaitMutation*/background queue) dispatches to the instance
// that issued the mutation, and everything unclaimed (plus non-mutate zero
// access like userID/clientID) goes to the FIRST client — the primary.
//
// PROVIDER NESTING CONTRACT: zero-react's useQuery resolves its instance from
// the NEAREST ZeroProvider context, so only ONE instance — the one whose
// provider is mounted INNERMOST — may use the upstream context path. by
// default that is the LAST client passed here (override via the `inner`
// option); the first client is primary and its provider is expected OUTER.
// the inner instance's queries ride zero-react's useQuery unchanged. every
// other instance must be created through `createDataClientWithDirectQueries`
// from `limesync/data/multi`, which opts into the context-free adapter for this
// nested-provider edge case. keep those non-inner instances bounded.

type ControlQueriesProps = {
  children: ReactNode
  action?: 'enable' | 'disable'
  whenDisabled?: 'empty' | 'last-value'
}

// the minimal structural surface the facade dispatches over — the const
// generic captures each client's real types, this only constrains shape
type CombinableDataClient = {
  instanceName: string
  useQuery: (...args: any[]) => any
  useQueryDirect: (...args: any[]) => any
  usePermission: (...args: any[]) => any
  usePermissionDirect: (...args: any[]) => any
  zero: any
  mutate?: any
  transaction?: (...args: any[]) => any
  useLocalStore?: () => LocalDataStore<any> | null
  readonly localTableNames?: ReadonlySet<string>
  readonly localStore?: LocalDataStore<any> | null
  preload: (...args: any[]) => any
  getQuery: (...args: any[]) => any
  mutationLifecycle: MutationLifecycle
  zeroEvents: ZeroEventsEmitter
  events?: ZeroEventsEmitter
  reloadPage: () => boolean
  retire: () => Promise<void>
  ControlQueries: (props: ControlQueriesProps) => ReactNode
}

export type CombineDataClientsOptions = {
  // instanceName of the client whose provider is mounted INNERMOST.
  // defaults to the last client passed.
  inner?: string
}

type UnionToIntersection<U> = (U extends any ? (x: U) => void : never) extends (
  x: infer I
) => void
  ? I
  : never

type WithoutMutate<Zero> = Zero extends unknown ? Omit<Zero, 'mutate'> : never
type MutationsOf<Zero> = Zero extends { mutate: infer Mutations } ? Mutations : never

type CombinedZero<Clients extends readonly CombinableDataClient[]> = UnionToIntersection<
  WithoutMutate<Clients[number]['zero']>
> & {
  mutate: UnionToIntersection<MutationsOf<Clients[number]['zero']>>
}

type LocalTransactionOf<Clients extends readonly CombinableDataClient[]> =
  Extract<Clients[number], { transaction: (...args: any[]) => any }> extends never
    ? never
    : UnionToIntersection<
        Extract<Clients[number], { transaction: (...args: any[]) => any }>['transaction']
      >

type LocalStoreOf<Clients extends readonly CombinableDataClient[]> =
  Extract<Clients[number], { localStore: LocalDataStore<any> | null }> extends never
    ? LocalDataStore<any> | null
    : Extract<Clients[number], { localStore: LocalDataStore<any> | null }>['localStore']

export type CombinedDataClients<Clients extends readonly CombinableDataClient[]> = {
  useQuery: UnionToIntersection<Clients[number]['useQuery']>
  usePermission: UnionToIntersection<Clients[number]['usePermission']>
  zero: CombinedZero<Clients>
  mutate: UnionToIntersection<MutationsOf<Clients[number]['zero']>>
  transaction: LocalTransactionOf<Clients>
  useLocalStore: () => LocalStoreOf<Clients>
  readonly localStore: LocalStoreOf<Clients>
  preload: UnionToIntersection<Clients[number]['preload']>
  getQuery: UnionToIntersection<Clients[number]['getQuery']>
  run: typeof run
  events: ZeroEventsEmitter
  zeroEvents: ZeroEventsEmitter
  reloadPage: () => boolean
  retire: () => Promise<void>
  ControlQueries: (props: ControlQueriesProps) => ReactNode
} & CombinedMutationLifecycle

export function combineDataClients<
  const Clients extends readonly [CombinableDataClient, ...CombinableDataClient[]],
>(
  ...clientsAndOptions: [...Clients] | [...Clients, CombineDataClientsOptions]
): CombinedDataClients<Clients> {
  const last = clientsAndOptions[clientsAndOptions.length - 1]
  const hasOptions = typeof (last as { instanceName?: unknown }).instanceName !== 'string'
  // boundary narrow: everything before a trailing options object is a client
  const clients = (hasOptions
    ? clientsAndOptions.slice(0, -1)
    : clientsAndOptions) as unknown as Clients
  const options: CombineDataClientsOptions = hasOptions
    ? (last as CombineDataClientsOptions)
    : {}

  const primary = clients[0]
  const innerName = options.inner ?? clients[clients.length - 1]!.instanceName
  const clientsByName = new Map(clients.map((client) => [client.instanceName, client]))

  if (!clientsByName.has(innerName)) {
    throw new Error(
      `[limesync] combineDataClients inner instance '${innerName}' is not one of the passed clients`
    )
  }

  const innerClient = clientsByName.get(innerName)!
  const localOwner = (): CombinableDataClient =>
    clients.find((client) => (client.localTableNames?.size ?? 0) > 0) ?? innerClient

  const ownerOfNamespace = (namespace: string): CombinableDataClient => {
    const localClient = clients.find((client) => client.localTableNames?.has(namespace))
    if (localClient) return localClient
    const owner = getInstanceForNamespace(namespace)
    return (owner && clientsByName.get(owner.name)) || primary
  }

  const ownerOfQueryInput = (input: unknown): CombinableDataClient => {
    if (typeof input !== 'function') {
      return localOwner()
    }
    const owner = getInstanceForQueryFn(input)
    return (owner && clientsByName.get(owner.name)) || primary
  }

  // hooks: a given call site always passes the same query fn / table, so the
  // dispatched hook target is stable across renders (no conditional hooks).
  // only the inner instance may use the upstream context path — its provider
  // is the nearest one, so useZero() resolves to it. everyone else goes
  // through the context-free direct hooks.
  function useQuery(...args: any[]) {
    const owner = ownerOfQueryInput(args[0])
    return owner.instanceName === innerName
      ? owner.useQuery(...args)
      : owner.useQueryDirect(...args)
  }

  function usePermission(...args: any[]) {
    const owner = ownerOfNamespace(String(args[0]))
    return owner.instanceName === innerName
      ? owner.usePermission(...args)
      : owner.usePermissionDirect(...args)
  }

  function useLocalStore() {
    return localOwner().useLocalStore?.() ?? null
  }

  function transaction(...args: any[]) {
    const owner = localOwner()
    if (!owner.transaction) {
      throw new Error('[limesync] no local tables are configured on these data clients')
    }
    return owner.transaction(...args)
  }

  const mutate = new Proxy({} as never, {
    get(_, key) {
      if (typeof key !== 'string') return undefined
      const owner = ownerOfNamespace(key)
      return (owner.mutate ?? owner.zero.mutate)[key]
    },
  })

  const zero = new Proxy({} as never, {
    get(_, key) {
      if (key === 'mutate') {
        return mutate
      }
      // non-mutate access (userID, clientID, close, …) forwards to primary
      return primary.zero[key]
    },
  })

  function preload(...args: any[]) {
    return ownerOfQueryInput(args[0]).preload(...args)
  }

  function getQuery(...args: any[]) {
    return ownerOfQueryInput(args[0]).getQuery(...args)
  }

  // one events stream relaying every instance's emitter
  const events = createEmitter<ZeroEvent | null>(
    `zero:combined(${clients.map((client) => client.instanceName).join('+')})`,
    null
  )
  const reconnecting = new Map<
    string,
    Extract<ZeroEvent, { type: 'reconnect'; status: 'trying' | 'waiting' }>
  >()
  for (const client of clients) {
    ;(client.events ?? client.zeroEvents).listen((event) => {
      if (event?.type !== 'reconnect') {
        events.emit(event)
        return
      }
      if (event.status === 'connected') {
        reconnecting.delete(client.instanceName)
        if (reconnecting.size === 0) {
          events.emit(event)
          return
        }
        const pending = [...reconnecting.values()]
        events.emit(
          pending.find((candidate) => candidate.status === 'waiting') ?? pending[0]!
        )
        return
      }
      reconnecting.set(client.instanceName, event)
      const pending = [...reconnecting.values()]
      events.emit(
        pending.find((candidate) => candidate.status === 'waiting') ?? event
      )
    })
  }

  // sign-out teardown across every instance at once. one client refusing to
  // tear down must not leave the others holding the signed-out account's local
  // store, so every retire runs and the first rejection surfaces after.
  async function retire(): Promise<void> {
    const outcomes = await Promise.allSettled(clients.map((client) => client.retire()))
    const failed = outcomes.find((outcome) => outcome.status === 'rejected')
    if (failed?.status === 'rejected') throw failed.reason
  }

  const ControlQueries = ({ children, ...props }: ControlQueriesProps) =>
    clients.reduceRight(
      (inner: ReactNode, client) => (
        <client.ControlQueries {...props}>{inner}</client.ControlQueries>
      ),
      children
    )

  // one queue across every instance, fenced per instance: a mutation is only
  // ever cancelled by the recovery of the client that issued it.
  const mutations = combineMutationLifecycles([
    primary.mutationLifecycle,
    ...clients.slice(1).map((client) => client.mutationLifecycle),
  ])

  const combined = {
    useQuery,
    usePermission,
    zero,
    mutate,
    transaction,
    useLocalStore,
    get localStore() {
      return localOwner().localStore ?? null
    },
    preload,
    getQuery,
    run,
    events,
    zeroEvents: events,
    reloadPage: primary.reloadPage,
    retire,
    ControlQueries,
    ...mutations,
  }

  // boundary assertion: the dispatching wrappers are untyped internally; the
  // combined type re-applies each client's real surface
  return combined as unknown as CombinedDataClients<Clients>
}
