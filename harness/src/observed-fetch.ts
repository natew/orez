export type HttpPullObservation = {
  at: number
  body: unknown
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
  rawBody?: string
  response?: unknown
  rawResponseBody?: string
  status?: number
  error?: unknown
}

export class PullAbortedByQuiesceControllerError extends Error {
  override readonly name = 'PullAbortedByQuiesceControllerError'
  constructor(options?: ErrorOptions) {
    super('stock pull aborted by quiesce controller', options)
  }
}

export function createPullQuiescenceFetch(fetchImpl: typeof fetch = globalThis.fetch): {
  fetch: typeof fetch
  pendingPullCount(): number
  abortPendingPulls(): number
} {
  let request = 0
  let sealed = false
  const pending = new Map<
    number,
    { controller: AbortController; abortSource?: 'caller' | 'quiesce' }
  >()
  return {
    pendingPullCount: () => pending.size,
    abortPendingPulls: () => {
      sealed = true
      let count = 0
      for (const value of pending.values()) {
        if (value.abortSource !== undefined) continue
        count++
        value.abortSource = 'quiesce'
        value.controller.abort()
      }
      return count
    },
    fetch: async (input, init) => {
      const url = new URL(
        typeof input === 'string' || input instanceof URL ? input : input.url
      )
      if (!url.pathname.endsWith('/pull')) return fetchImpl(input, init)
      if (sealed) throw new Error('stock pull began after quiescence controller sealed')
      const id = ++request
      const controller = new AbortController()
      const state: {
        controller: AbortController
        abortSource?: 'caller' | 'quiesce'
      } = { controller }
      pending.set(id, state)
      const callerAbort = () => {
        if (state.abortSource !== undefined) return
        state.abortSource = 'caller'
        controller.abort(init?.signal?.reason)
      }
      if (init?.signal?.aborted) callerAbort()
      else init?.signal?.addEventListener('abort', callerAbort, { once: true })
      try {
        return await fetchImpl(input, { ...init, signal: controller.signal })
      } catch (error) {
        if (state.abortSource === 'quiesce')
          throw new PullAbortedByQuiesceControllerError({ cause: error })
        throw error
      } finally {
        init?.signal?.removeEventListener('abort', callerAbort)
        pending.delete(id)
      }
    },
  }
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
      rawResponseBody: observation.rawResponseBody,
      status: observation.status,
      error: observation.error,
    })
  }, fetchImpl)
}
