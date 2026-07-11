export type HttpPullObservation = {
  at: number
  body: unknown
  response?: unknown
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
    try {
      body = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined
    } catch {
      body = undefined
    }
    const id = ++request
    onObservation({ request: id, path, phase: 'invoke', body })
    try {
      const response = await bound(input, init)
      let responseBody: unknown
      try {
        responseBody = await response.clone().json()
      } catch {
        responseBody = undefined
      }
      onObservation({
        request: id,
        path,
        phase: 'terminal',
        body,
        response: responseBody,
        status: response.status,
      })
      return response
    } catch (error) {
      onObservation({ request: id, path, phase: 'terminal', body, error })
      throw error
    }
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
      response: observation.response,
      status: observation.status,
      error: observation.error,
    })
  }, fetchImpl)
}
