import { addContextToQuery, asQueryInternals } from '../bindings.js'

import type { ConnectionState, CustomMutatorDefs, Schema, Zero } from '../zero.js'
import type { NativeZero } from './client.js'
import type { OrezTransport } from './orez.js'
import type { SyncState } from './sync.js'
import type { Mutators, Query, Transport, TTL } from './types.js'

type QueryInput = Parameters<typeof addContextToQuery>[0]

// a transport's fetch throws this on a 401 or 403; the client then reports
// needs-auth and stays offline until connect() brings a new token.
export class NativeZeroAuthError extends Error {
  constructor(
    readonly operation: 'query' | 'mutate',
    readonly status: 401 | 403
  ) {
    super(`Native Zero ${operation} requires authentication (${status})`)
  }
}

export type NativeZeroClientOptions = {
  userID: string
  auth?: string
  onAuthChange?: (auth: string) => void
  active?: boolean
}

function hasWake(transport: Transport | undefined): transport is OrezTransport {
  return transport !== undefined && 'subscribeWake' in transport
}

// the provider-facing client: the same surface the JavaScript Zero client gives
// a limesync provider's `zero` prop, answered by the native engine.
export async function nativeZeroClient<
  S extends Schema,
  M extends Mutators & CustomMutatorDefs,
>(
  client: NativeZero<M>,
  store: { name: string; schema: S; transport: Transport | undefined },
  options: NativeZeroClientOptions
): Promise<NativeZeroClient<S, M>> {
  const { schema, transport } = store
  let closed = false
  let online = true
  let needsAuth = false
  let auth = options.auth
  let current: ConnectionState = { name: 'connecting' }
  const listeners = new Set<(state: ConnectionState) => void>()
  const pendingQueries = new Set<() => void>()
  const publish = (state: ConnectionState) => {
    current = state
    for (const listener of [...listeners]) listener(state)
  }
  const identity = await client.state()
  const sync = client.startSync({
    active: options.active,
    subscribeWake: hasWake(transport)
      ? (request) => transport.subscribeWake(identity, request)
      : undefined,
    onStateChange(state: SyncState) {
      if (state.name === 'error') {
        if (state.error instanceof NativeZeroAuthError) {
          needsAuth = true
          sync.setOnline(false)
          publish({
            name: 'needs-auth',
            reason: { type: state.error.operation, status: state.error.status },
          })
        } else publish({ name: 'disconnected', reason: String(state.error) })
      } else if (state.name === 'closed') {
        publish({ name: 'closed', reason: 'Native Zero client closed' })
      } else if (state.name === 'disconnected') {
        if (!needsAuth)
          publish({ name: 'disconnected', reason: 'Native Zero sync paused' })
      } else publish({ name: state.name })
    },
  })
  const resolveQuery = (input: QueryInput) =>
    asQueryInternals(addContextToQuery(input, client.context)) as unknown as Query

  const watch = (query: Query, ttl?: TTL) => {
    const view = client.materialize(query, { ttl })
    let resolve!: () => void
    let reject!: (error: Error) => void
    let settled = false
    const complete = new Promise<void>((done, fail) => {
      resolve = done
      reject = fail
    })
    // preloads may be used only for their cleanup handle.
    void complete.catch(() => {})
    const cancel = () => {
      if (settled) return
      settled = true
      pendingQueries.delete(cancel)
      reject(new Error('Native Zero query cancelled'))
    }
    pendingQueries.add(cancel)
    const unsubscribe = view.addListener((_data, type, error) => {
      if (settled || type === 'unknown') return
      settled = true
      pendingQueries.delete(cancel)
      if (type === 'error') reject(new Error(error!.message))
      else resolve()
    })
    return {
      view,
      complete,
      cleanup() {
        cancel()
        unsubscribe()
        view.destroy()
      },
    }
  }

  const dispose = () => {
    if (closed) return
    closed = true
    sync.stop()
    for (const cancel of [...pendingQueries]) cancel()
    listeners.clear()
  }

  const provider = {
    clientID: client.clientID,
    clientGroupID: Promise.resolve(identity.clientGroupID),
    context: client.context,
    schema,
    userID: options.userID,
    mutate: client.mutate,
    get closed() {
      return closed
    },
    connection: {
      state: {
        get current() {
          return current
        },
        subscribe(listener: (state: ConnectionState) => void) {
          listeners.add(listener)
          return () => {
            listeners.delete(listener)
          }
        },
      },
      async connect(next?: { auth: string }) {
        if (closed) return
        if (next && auth !== next.auth) {
          sync.setOnline(false)
          options.onAuthChange?.(next.auth)
          auth = next.auth
        }
        needsAuth = false
        sync.setOnline(online)
        sync.request()
      },
    },
    setActive(active: boolean) {
      sync.setActive(active)
    },
    setOnline(available: boolean) {
      online = available
      sync.setOnline(online && !needsAuth)
    },
    materialize(input: QueryInput, settings?: { ttl?: TTL }) {
      return client.materialize(resolveQuery(input), settings)
    },
    async run<T = unknown>(
      input: QueryInput,
      settings?: { type: 'unknown' | 'complete'; ttl?: TTL }
    ): Promise<T> {
      const query = resolveQuery(input)
      const pending = watch(query, settings?.ttl)
      try {
        if (settings?.type === 'complete') {
          await pending.complete
          return pending.view.data as T
        }
        return await client.run<T>(query)
      } finally {
        pending.cleanup()
      }
    },
    preload(input: QueryInput, settings?: { ttl?: TTL }) {
      const { complete, cleanup } = watch(resolveQuery(input), settings?.ttl ?? 0)
      return { complete, cleanup }
    },
    close() {
      dispose()
      return client.close()
    },
    // the same report Zero.delete gives, for the one store this client owns
    async delete() {
      dispose()
      try {
        await client.delete()
        return { deleted: [store.name], errors: [], kept: [] }
      } catch (error) {
        return { deleted: [], errors: [error], kept: [store.name] }
      }
    },
  }
  // Zero's private fields make the class nominal. a provider reads only its
  // public surface, which this object answers, so this is the one place the
  // native client becomes the type a `zero` prop takes.
  return provider as unknown as NativeZeroClient<S, M>
}

export type NativeZeroClient<
  S extends Schema = Schema,
  M extends Mutators & CustomMutatorDefs = Mutators & CustomMutatorDefs,
> = Zero<S, M, unknown> & {
  setActive(active: boolean): void
  setOnline(available: boolean): void
}
