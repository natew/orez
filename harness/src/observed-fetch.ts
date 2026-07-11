export type HttpPullObservation = {
  at: number
  body: unknown
  rawBody?: string
  rawBody?: string
  response?: unknown
  rawResponseBody?: string
  status?: number
  error?: unknown
}

export type SyncHttpObservation = {
  request: number
  path: 'push' | 'pull'
  phase: 'invoke' | 'terminal'
  body: unknown
  response?: unknown
  status?: number
  error?: unknown
}

export function createOperationBoundDropFetch(
  consume: (token: string) => void | Promise<void>,
  fetchImpl: typeof fetch = globalThis.fetch
): { fetch: typeof fetch; arm(token: string): void } {
  let armed: string | undefined
  return {
    arm(token) {
      if (!token.trim()) throw new Error('drop token must be nonempty')
      if (armed) throw new Error('response drop is already armed')
      armed = token
    },
    fetch: async (input, init) => {
      const response = await fetchImpl(input, init)
      const url = new URL(
        typeof input === 'string' || input instanceof URL ? input : input.url
      )
      if (!url.pathname.endsWith('/push')) return response
      const received = response.headers.get('x-orez-drop-token')
      if (!armed) {
        if (received) throw new Error('received an unarmed or reused drop token')
        return response
      }
      const expected = armed
      armed = undefined
      if (!received) throw new Error('armed push response is missing its drop token')
      if (received !== expected)
        throw new Error('push response drop token does not match')
      await response.arrayBuffer()
      await consume(expected)
      throw new Error('operation-bound post-commit response loss')
    },
  }
}

export const singleAttemptHttpFetch: typeof fetch = (input, init) =>
  new Promise((resolve, reject) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? input : input.url
    )
    if (url.protocol !== 'http:') {
      reject(new Error('single-attempt fetch supports only http'))
      return
    }
    const requestHeaders = Object.fromEntries(new Headers(init?.headers).entries())
    const outgoing = request(
      url,
      { method: init?.method ?? 'GET', headers: requestHeaders },
      (incoming) => {
        const headers = new Headers()
        for (const [name, value] of Object.entries(incoming.headers)) {
          if (Array.isArray(value)) for (const item of value) headers.append(name, item)
          else if (value !== undefined) headers.set(name, value)
        }
        resolve(
          new Response(Readable.toWeb(incoming) as ReadableStream, {
            status: incoming.statusCode ?? 500,
            statusText: incoming.statusMessage,
            headers,
          })
        )
      }
    )
    outgoing.once('error', reject)
    if (init?.signal) {
      const abort = () => outgoing.destroy(new Error('request aborted'))
      if (init.signal.aborted) abort()
      else init.signal.addEventListener('abort', abort, { once: true })
    }
    if (typeof init?.body === 'string' || init?.body instanceof Uint8Array) {
      outgoing.write(init.body)
    } else if (init?.body !== undefined && init.body !== null) {
      outgoing.destroy(new Error('single-attempt fetch requires a string body'))
      return
    }
    outgoing.end()
  })

export function observedSyncFetch(
  onObservation: (observation: SyncHttpObservation) => void,
  fetchImpl: typeof fetch = globalThis.fetch
): typeof fetch {
  const bound = fetchImpl.bind(globalThis)
  let request = 0
  return async (input, init) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? input : input.url
    )
    const path = url.pathname.endsWith('/push')
      ? 'push'
      : url.pathname.endsWith('/pull')
        ? 'pull'
        : undefined
    if (!path) return bound(input, init)
    let body: unknown
    const rawBody = typeof init?.body === 'string' ? init.body : undefined
    try {
      body = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined
    } catch {
      body = undefined
    }
    const id = ++request
    onObservation({ request: id, path, phase: 'invoke', body, rawBody })
    let response: Response
    try {
      response = await bound(input, init)
    } catch (error) {
      onObservation({ request: id, path, phase: 'terminal', body, rawBody, error })
      throw error
    }
    let responseBody: unknown
    let rawResponseBody: string | undefined
    try {
      rawResponseBody = await response.clone().text()
    } catch (error) {
      onObservation({ request: id, path, phase: 'terminal', body, rawBody, error })
      throw error
    }
    try {
      responseBody = JSON.parse(rawResponseBody)
    } catch {
      responseBody = undefined
    }
    onObservation({
      request: id,
      path,
      phase: 'terminal',
      body,
      rawBody,
      response: responseBody,
      rawResponseBody,
      status: response.status,
    })
    return response
  }
}

// The production transport already accepts a fetch implementation. Wrap that
// seam instead of modifying the vendored transport so fault lanes can assert
// request/response cookie history and HTTP status behavior.
export function observedPullFetch(
  onPull: ((observation: HttpPullObservation) => void) | undefined,
  fetchImpl: typeof fetch = globalThis.fetch
): typeof fetch {
  return observedSyncFetch((observation) => {
    if (observation.path !== 'pull' || observation.phase !== 'terminal') return
    onPull?.({
      at: Date.now(),
      body: observation.body,
      rawBody: observation.rawBody,
      response: observation.response,
      status: observation.status,
      error: observation.error,
    })
  }, fetchImpl)
}
import { request } from 'node:http'
import { Readable } from 'node:stream'
