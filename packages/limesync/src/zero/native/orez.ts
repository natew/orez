import { subscribeOrezWake, type OrezWakeOptions } from './orez-wake.js'
import { diffPullQueries, pullQueryPatch, type PullState } from './pullQueries.js'

import type {
  ClientState,
  DesiredQuery,
  Mutation,
  MutationOutcome,
  JSONValue,
  PullResponse,
  Transport,
} from './types.js'

export type OrezTransportOptions = {
  server: string
  appID: string
  schemaVersion: string
  shardNum?: number
  headers?: () => Record<string, string> | Promise<Record<string, string>>
  fetch?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>
  wake?: OrezWakeOptions
  payloadCodec?: {
    encodePush(body: unknown): unknown | Promise<unknown>
    decodePull(body: unknown): unknown | Promise<unknown>
  }
}

export interface OrezTransport extends Transport {
  subscribeWake(
    identity: Pick<ClientState, 'clientID' | 'clientGroupID'>,
    request: () => void
  ): () => void
}

function jsonValue(value: unknown): value is JSONValue {
  if (value === null || typeof value === 'string' || typeof value === 'boolean')
    return true
  if (typeof value === 'number') return Number.isFinite(value)
  if (Array.isArray(value)) return value.every(jsonValue)
  return Boolean(
    value && typeof value === 'object' && Object.values(value).every(jsonValue)
  )
}

function pullResponse(value: unknown): value is PullResponse {
  if (
    !value ||
    typeof value !== 'object' ||
    !('cookie' in value) ||
    !jsonValue(value.cookie)
  )
    return false
  if (!('lastMutationIDChanges' in value) || value.lastMutationIDChanges === undefined)
    return true
  const changes = value.lastMutationIDChanges
  return (
    changes !== null &&
    typeof changes === 'object' &&
    !Array.isArray(changes) &&
    Object.values(changes).every((id) => typeof id === 'number')
  )
}

export function createOrezTransport(options: OrezTransportOptions): OrezTransport {
  const server = options.server.replace(/\/$/, '')
  const request = options.fetch ?? globalThis.fetch.bind(globalThis)

  const send = async (path: 'pull' | 'push', body: unknown, signal: AbortSignal) => {
    const url = new URL(`${server}/${path}`)
    if (path === 'push') {
      url.searchParams.set('appID', options.appID)
      url.searchParams.set('schema', `${options.appID}_${options.shardNum ?? 0}`)
    }
    const extraHeaders = await options.headers?.()
    const response = await request(url.toString(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...extraHeaders },
      body: JSON.stringify(body),
      signal,
    })
    if (!response.ok) throw new Error(`Orez ${path} failed: ${response.status}`)
    return response
  }
  const post = async (path: 'pull' | 'push', body: unknown, signal: AbortSignal) => {
    const result: unknown = await (await send(path, body, signal)).json()
    if (result && typeof result === 'object' && 'error' in result) {
      throw new Error(`Orez ${path} failed: ${JSON.stringify(result)}`)
    }
    return result
  }

  return {
    subscribeWake(identity, request) {
      return options.wake
        ? subscribeOrezWake(server, identity, options.wake, request)
        : () => {}
    },
    async pull(state: PullState, queries: readonly DesiredQuery[], signal: AbortSignal) {
      const queryPatch =
        state[pullQueryPatch] === undefined
          ? diffPullQueries({ queryVersion: state.queryVersion, queries })
          : state[pullQueryPatch]
      const body = {
        clientID: state.clientID,
        clientGroupID: state.clientGroupID,
        cookie: state.cookie,
        queries: queryPatch ?? undefined,
      }
      // the engine parses and validates the raw body off the JavaScript thread;
      // only a payload codec needs the body as values here.
      if (!options.payloadCodec) return (await send('pull', body, signal)).text()
      const response = await options.payloadCodec.decodePull(
        await post('pull', body, signal)
      )
      if (!pullResponse(response))
        throw new Error('Orez pull returned an invalid response')
      return response
    },
    async push(state: ClientState, mutations: readonly Mutation[], signal: AbortSignal) {
      const body = {
        clientGroupID: state.clientGroupID,
        pushVersion: 1,
        schemaVersion: options.schemaVersion,
        timestamp: Date.now(),
        mutations: mutations.map((mutation) => ({
          ...mutation,
          name: mutation.name.replaceAll('.', '|'),
          type: 'custom',
          args: mutation.args === undefined ? [] : [mutation.args],
        })),
      }
      const received = await post(
        'push',
        options.payloadCodec ? await options.payloadCodec.encodePush(body) : body,
        signal
      )
      if (!received || typeof received !== 'object')
        throw new Error('Orez push returned an invalid response')
      const response = 'pushResponse' in received ? received.pushResponse : received
      if (
        !response ||
        typeof response !== 'object' ||
        !('mutations' in response) ||
        !Array.isArray(response.mutations)
      )
        throw new Error('Orez push returned an invalid mutation response')
      const expected = new Set(mutations.map((mutation) => mutation.id))
      const outcomes: MutationOutcome[] = []
      for (const mutation of response.mutations) {
        if (
          !mutation ||
          typeof mutation !== 'object' ||
          !('id' in mutation) ||
          !mutation.id ||
          typeof mutation.id !== 'object' ||
          !('clientID' in mutation.id) ||
          mutation.id.clientID !== state.clientID ||
          !('id' in mutation.id) ||
          typeof mutation.id.id !== 'number' ||
          !expected.delete(mutation.id.id)
        )
          throw new Error('Orez push returned an unexpected mutation identity')
        const result: unknown = 'result' in mutation ? mutation.result : undefined
        if (!result || typeof result !== 'object' || Array.isArray(result))
          throw new Error('Orez push returned an invalid mutation result')
        const error = 'error' in result ? result.error : undefined
        const message = 'message' in result ? result.message : undefined
        const details = 'details' in result ? result.details : undefined
        if ('error' in result && typeof error !== 'string')
          throw new Error('Orez push returned an invalid mutation error')
        if (error === 'app' && typeof message !== 'string')
          throw new Error('Orez push returned an invalid application error')
        if (error === 'alreadyProcessed') continue
        if (details !== undefined && !jsonValue(details))
          throw new Error('Orez push returned invalid error details')
        outcomes.push({
          id: mutation.id.id,
          result: error
            ? {
                type: 'error',
                error: {
                  type: error === 'app' ? 'app' : 'zero',
                  message: typeof message === 'string' ? message : JSON.stringify(result),
                  details,
                },
              }
            : { type: 'success' },
        })
      }
      if (expected.size) throw new Error('Orez push omitted mutation results')
      return outcomes
    },
  }
}
