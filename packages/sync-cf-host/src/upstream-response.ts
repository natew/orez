export const DEFAULT_UPSTREAM_REQUEST_TIMEOUT_MS = 30_000
export const DEFAULT_UPSTREAM_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
export const DEFAULT_UPSTREAM_MAX_REQUEST_BYTES = 1024 * 1024

export class UpstreamResponseLimitError extends Error {
  readonly status = 502

  constructor(readonly maxBytes: number) {
    super(`upstream response exceeded ${maxBytes} bytes`)
    this.name = 'UpstreamResponseLimitError'
  }
}

function abortError(signal: AbortSignal): Error {
  return signal.reason instanceof Error
    ? signal.reason
    : new Error('upstream response was aborted')
}

async function readWithAbort<T>(
  read: Promise<T>,
  signal: AbortSignal,
  cancel: () => Promise<unknown>
): Promise<T> {
  if (signal.aborted) throw abortError(signal)
  let remove = () => {}
  const aborted = new Promise<never>((_, reject) => {
    const onAbort = () => {
      void cancel().catch(() => {})
      reject(abortError(signal))
    }
    signal.addEventListener('abort', onAbort, { once: true })
    remove = () => signal.removeEventListener('abort', onAbort)
  })
  try {
    return await Promise.race([read, aborted])
  } finally {
    remove()
  }
}

export async function readBoundedJsonResponse(
  response: Response,
  maxBytes: number,
  signal: AbortSignal
): Promise<unknown> {
  const contentLength = Number(response.headers.get('content-length'))
  if (Number.isFinite(contentLength) && contentLength > maxBytes) {
    await response.body?.cancel().catch(() => {})
    throw new UpstreamResponseLimitError(maxBytes)
  }
  if (!response.body) throw new Error('upstream response has no body')
  const body = await readBoundedStream(response.body, maxBytes, signal)
  return JSON.parse(new TextDecoder().decode(body))
}

export async function readBoundedStream(
  stream: ReadableStream<Uint8Array> | null,
  maxBytes: number,
  signal: AbortSignal
): Promise<Uint8Array> {
  if (!stream) return new Uint8Array()
  const reader = stream.getReader()
  const chunks: Uint8Array[] = []
  let bytes = 0
  for (;;) {
    if (signal.aborted) {
      await reader.cancel().catch(() => {})
      throw abortError(signal)
    }
    const result = await readWithAbort(reader.read(), signal, () => reader.cancel())
    if (result.done) break
    bytes += result.value.byteLength
    if (bytes > maxBytes) {
      await reader.cancel().catch(() => {})
      throw new UpstreamResponseLimitError(maxBytes)
    }
    chunks.push(result.value)
  }
  const body = new Uint8Array(bytes)
  let offset = 0
  for (const chunk of chunks) {
    body.set(chunk, offset)
    offset += chunk.byteLength
  }
  return body
}

export async function fetchBoundedUpstreamJson(
  binding: { fetch(input: string | Request, init?: RequestInit): Promise<Response> },
  input: string,
  init: RequestInit,
  options: {
    timeoutMs: number
    maxBytes: number
    now?: () => number
  }
): Promise<{
  response: Response
  body: unknown | null
  sendTimeMs: number
  receiveTimeMs: number
}> {
  const now = options.now ?? Date.now
  const controller = new AbortController()
  const timer = setTimeout(() => {
    controller.abort(
      new Error(`upstream response missed ${options.timeoutMs}ms deadline`)
    )
  }, options.timeoutMs)
  const sendTimeMs = now()
  try {
    const response = await binding.fetch(input, { ...init, signal: controller.signal })
    if (!response.ok) {
      await response.body?.cancel().catch(() => {})
      return { response, body: null, sendTimeMs, receiveTimeMs: now() }
    }
    const body = await readBoundedJsonResponse(
      response,
      options.maxBytes,
      controller.signal
    )
    return { response, body, sendTimeMs, receiveTimeMs: now() }
  } catch (error) {
    if (controller.signal.aborted) throw abortError(controller.signal)
    throw error
  } finally {
    clearTimeout(timer)
  }
}
