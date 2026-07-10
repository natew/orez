export type HttpPullObservation = {
  at: number
  body: unknown
  response?: unknown
  status?: number
  error?: unknown
}

// The production transport already accepts a fetch implementation. Wrap that
// seam instead of modifying the vendored transport so fault lanes can assert
// request/response cookie history and HTTP status behavior.
export function observedPullFetch(
  onPull: ((observation: HttpPullObservation) => void) | undefined,
  fetchImpl: typeof fetch = globalThis.fetch
): typeof fetch {
  const bound = fetchImpl.bind(globalThis)
  return async (input, init) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? input : input.url
    )
    if (!url.pathname.endsWith('/pull')) return bound(input, init)

    let body: unknown
    try {
      body = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined
    } catch {
      body = undefined
    }

    try {
      const response = await bound(input, init)
      let responseBody: unknown
      try {
        responseBody = await response.clone().json()
      } catch {
        responseBody = undefined
      }
      onPull?.({ at: Date.now(), body, response: responseBody, status: response.status })
      return response
    } catch (error) {
      onPull?.({ at: Date.now(), body, error })
      throw error
    }
  }
}
