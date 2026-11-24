import { handleSyncExecutorPushRequest, type SyncExecutor } from '../index.js'
import { mintWakeCapability, verifyWakeCapability } from '../wake.js'
import {
  createZeroServerBindings,
  type CreateZeroServerBindingsOptions,
  type ZeroServerMutatorRegistry,
} from './server.js'

import type { AnyQueryRegistry, Schema } from '../zero/zero.js'
import type { AuthData, GenericModels } from './types.js'
import type { Check } from './validate.js'

// the public `limesync/data/server` entry: the routes plus every binding export.
export * from './server.js'

type Identity = AuthData & { id: string }

// the wake-token and wake-authorize routes must agree on this lifetime.
const WAKE_CAPABILITY_TTL_MS = 60_000

// the namespace every user shares: `app` deployed, `singleton` in local dev.
const SHARED_NAMESPACE = 'singleton'

// a keyed namespace, `<family>-<key>`, is its own database and sync stream.
// clients push to /<namespace>/api/zero/push, so it is the first path segment.
const KEYED_NAMESPACE = /^[a-z][a-z0-9]{0,15}-[A-Za-z0-9_-]{1,64}$/

function keyedNamespaceOf(request: Request): string | null {
  const first = new URL(request.url).pathname.split('/')[1] ?? ''
  return KEYED_NAMESPACE.test(first) ? first : null
}

function isSharedNamespace(namespace: string) {
  return namespace === 'app' || namespace === SHARED_NAMESPACE
}

export type SyncRoutesOptions<S extends Schema> = {
  authData(request: Request): Promise<Identity | null>
  // the executor that commits one namespace's pushes; asked once per namespace.
  executor(namespace: string): SyncExecutor<S>
  transformQueries(input: {
    authData: Identity | null
    request: Request
  }): object | Promise<object>
  wakeSecret(): string
  // who may open a keyed namespace. without it only the shared one opens.
  canOpenNamespace?(userId: string, namespace: string): boolean | Promise<boolean>
  // signed-out visitors sync as `anon` instead of being refused.
  anonymous?: boolean
}

// the app side of the sync host's callbacks, one handler per app/api/zero route.
export function createSyncRoutes<S extends Schema>(options: SyncRoutesOptions<S>) {
  const executors = new Map<string, SyncExecutor<S>>()

  // a response when the request may not open the namespace.
  async function open(
    request: Request,
    namespace: string
  ): Promise<{ authData: Identity | null } | Response> {
    const authData = await options.authData(request)
    // no placeholder identity for a signed-out push: a write committed as
    // `anon` while the query socket validates as the real user fails the
    // protocol's user check and leaves rows nobody owns.
    if (!authData && !options.anonymous) {
      return new Response('unauthorized', { status: 401 })
    }
    const allowed =
      isSharedNamespace(namespace) ||
      (authData !== null &&
        (await options.canOpenNamespace?.(authData.id, namespace)) === true)
    return allowed ? { authData } : new Response('forbidden', { status: 403 })
  }

  async function push(request: Request, namespace: string) {
    try {
      const opened = await open(request, namespace)
      if (opened instanceof Response) return opened
      let executor = executors.get(namespace)
      if (!executor) {
        executor = options.executor(namespace)
        executors.set(namespace, executor)
      }
      return await handleSyncExecutorPushRequest({
        executor,
        request,
        authData: opened.authData,
      })
    } catch (error) {
      console.error('[zero:push] error', error)
      return new Response('internal error', { status: 500 })
    }
  }

  async function wakeToken(request: Request, namespace: string) {
    const opened = await open(request, namespace)
    if (opened instanceof Response) return opened
    return Response.json(
      await mintWakeCapability(options.wakeSecret(), {
        namespace,
        identity: opened.authData?.id ?? 'anon',
        ttlMs: WAKE_CAPABILITY_TTL_MS,
      }),
      { headers: { 'cache-control': 'no-store' } }
    )
  }

  const notFound = () => new Response('not found', { status: 404 })

  return {
    push: (request: Request) => push(request, SHARED_NAMESPACE),

    keyedPush(request: Request) {
      const namespace = keyedNamespaceOf(request)
      return namespace ? push(request, namespace) : notFound()
    },

    async pull(request: Request) {
      try {
        const authData = await options.authData(request)
        const response = await options.transformQueries({ authData, request })
        const body = Array.isArray(response) ? { queries: response } : response
        return Response.json({ queryTransformVersion: 0, ...body })
      } catch (error) {
        console.error('[zero:pull] error', error)
        return new Response('internal error', { status: 500 })
      }
    },

    // the sync host asks here before it opens a namespace for a client. naming
    // the namespace in the reply grants it.
    async authenticate(request: Request) {
      const body: unknown = await request.json().catch(() => null)
      const namespace =
        body &&
        typeof body === 'object' &&
        'namespace' in body &&
        typeof body.namespace === 'string'
          ? body.namespace
          : 'app'
      const opened = await open(request, namespace)
      if (opened instanceof Response) return opened
      const { authData } = opened
      return Response.json({
        userID: authData?.id ?? 'anon',
        ...(authData ? { authData } : {}),
        namespace,
      })
    },

    // mints a wake capability so a local-dev wake socket authorizes against the
    // native sidecar, which calls back to authorizeWake with it.
    wakeToken: (request: Request) => wakeToken(request, SHARED_NAMESPACE),

    keyedWakeToken(request: Request) {
      const namespace = keyedNamespaceOf(request)
      return namespace ? wakeToken(request, namespace) : notFound()
    },

    // the native sidecar posts { namespace, token } and expects
    // { userID, tokenIssuedAt }.
    async authorizeWake(request: Request) {
      const body: unknown = await request.json().catch(() => null)
      if (
        !body ||
        typeof body !== 'object' ||
        !('namespace' in body) ||
        typeof body.namespace !== 'string' ||
        !('token' in body) ||
        typeof body.token !== 'string'
      ) {
        return new Response('unauthorized', { status: 401 })
      }
      const capability = await verifyWakeCapability(options.wakeSecret(), body.token, {
        namespace: body.namespace,
        ttlMs: WAKE_CAPABILITY_TTL_MS,
      })
      if (!capability) return new Response('unauthorized', { status: 401 })
      return Response.json({
        userID: capability.identity,
        tokenIssuedAt: capability.expiresAt - WAKE_CAPABILITY_TTL_MS,
      })
    },
  }
}

export type DataServerApp<S extends Schema, M extends GenericModels> = {
  schema: S
  models: M
  queries: AnyQueryRegistry
  mutationValidators: Record<string, Record<string, Check<unknown>>>
}

type DataServerOptions<
  S extends Schema,
  M extends GenericModels,
  Actions extends Record<string, unknown>,
> = Omit<SyncRoutesOptions<S>, 'executor' | 'transformQueries'> &
  Pick<
    CreateZeroServerBindingsOptions<S, M, Actions>,
    'validateQuery' | 'validateMutation' | 'defaultAllowAdminRole' | 'mapClaims'
  > & {
    createServerActions?: () => Actions
    executor(namespace: string, mutators: ZeroServerMutatorRegistry<S>): SyncExecutor<S>
  }

export function createDataServer<
  S extends Schema,
  M extends GenericModels,
  Actions extends Record<string, unknown>,
>(app: DataServerApp<S, M>, options: DataServerOptions<S, M, Actions>) {
  const {
    executor,
    createServerActions = () => ({}),
    validateQuery,
    validateMutation,
    defaultAllowAdminRole,
    mapClaims,
    ...routeOptions
  } = options
  const bindings = createZeroServerBindings({
    schema: app.schema,
    models: app.models,
    queries: app.queries,
    mutations: app.mutationValidators,
    createServerActions,
    validateQuery,
    validateMutation,
    defaultAllowAdminRole,
    mapClaims,
  })
  const routes = createSyncRoutes({
    ...routeOptions,
    executor: (namespace) => executor(namespace, bindings.mutators),
    transformQueries: bindings.transformQueryRequest,
  })
  return (request: Request) => {
    const match = /^(?:\/([^/]+))?\/api\/(?:data|zero)\/([^/]+)$/.exec(
      new URL(request.url).pathname
    )
    const namespace = match?.[1]
    switch (match?.[2]) {
      case 'push':
        return namespace ? routes.keyedPush(request) : routes.push(request)
      case 'wake-token':
        return namespace ? routes.keyedWakeToken(request) : routes.wakeToken(request)
      case 'pull':
        if (!namespace) return routes.pull(request)
        break
      case 'rust-auth':
        if (!namespace) return routes.authenticate(request)
        break
      case 'wake-authorize':
        if (!namespace) return routes.authorizeWake(request)
    }
    return new Response('not found', { status: 404 })
  }
}
