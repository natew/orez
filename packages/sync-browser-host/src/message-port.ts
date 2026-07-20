import type { BrowserHostTestHooks, BrowserHostTestFaultPoint } from './host.js'
import type { BrowserSyncHost, BrowserSyncHostPortClient } from './types.js'
import type { Schema } from '@rocicorp/zero'
import type { SqlStatementMetadata } from 'orez-sync-executor'

type SerializedRequest = {
  url: string
  method: string
  headers: [string, string][]
  body: ArrayBuffer | null
}

type SerializedResponse = {
  status: number
  statusText: string
  headers: [string, string][]
  body: ArrayBuffer
}

type PortRequest =
  | { id: number; operation: 'fetch'; request: SerializedRequest }
  | {
      id: number
      operation: 'query'
      sql: string
      params: readonly unknown[]
    }
  | {
      id: number
      operation: 'exec'
      sql: string
      params: readonly unknown[]
      metadata?: SqlStatementMetadata
    }
  | { id: number; operation: 'disconnect' }

type PortRequestWithoutID = PortRequest extends infer Request
  ? Request extends { id: number }
    ? Omit<Request, 'id'>
    : never
  : never

type PortResponse =
  | { id: number; ok: true; value?: unknown; response?: SerializedResponse }
  | {
      id: number
      ok: false
      error: { name: string; message: string; stack?: string }
    }

type PortEvent = { event: 'data-changed' }

function serializeError(error: unknown): Extract<PortResponse, { ok: false }>['error'] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { name: 'Error', message: String(error) }
}

async function serializeRequest(request: Request): Promise<SerializedRequest> {
  return {
    url: request.url,
    method: request.method,
    headers: [...request.headers.entries()],
    body:
      request.method === 'GET' || request.method === 'HEAD'
        ? null
        : await request.arrayBuffer(),
  }
}

function deserializeRequest(request: SerializedRequest): Request {
  return new Request(request.url, {
    method: request.method,
    headers: request.headers,
    body: request.body,
  })
}

async function serializeResponse(response: Response): Promise<SerializedResponse> {
  return {
    status: response.status,
    statusText: response.statusText,
    headers: [...response.headers.entries()],
    body: await response.arrayBuffer(),
  }
}

function deserializeResponse(response: SerializedResponse): Response {
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  })
}

async function reachDuringDelivery(hooks?: BrowserHostTestHooks): Promise<void> {
  await hooks?.reach('during_response_delivery' satisfies BrowserHostTestFaultPoint)
}

export function serveBrowserSyncHostPortInternal<S extends Schema>(
  host: BrowserSyncHost<S>,
  port: MessagePort,
  hooks?: BrowserHostTestHooks
): () => void {
  let disconnected = false
  const unsubscribe = host.subscribe(() => {
    if (!disconnected) port.postMessage({ event: 'data-changed' } satisfies PortEvent)
  })

  const disconnect = () => {
    if (disconnected) return
    disconnected = true
    unsubscribe()
    port.close()
  }

  port.addEventListener('message', (event: MessageEvent<PortRequest>) => {
    const request = event.data
    if (!request || typeof request !== 'object' || typeof request.id !== 'number') return
    if (request.operation === 'disconnect') {
      disconnect()
      return
    }
    void (async () => {
      try {
        let response: PortResponse
        if (request.operation === 'fetch') {
          const result = await host.fetch(deserializeRequest(request.request))
          response = {
            id: request.id,
            ok: true,
            response: await serializeResponse(result),
          }
        } else if (request.operation === 'query') {
          response = {
            id: request.id,
            ok: true,
            value: await host.query(request.sql, request.params),
          }
        } else {
          response = {
            id: request.id,
            ok: true,
            value: await host.exec(request.sql, request.params, request.metadata),
          }
        }
        await reachDuringDelivery(hooks)
        if (!disconnected) {
          const transfer =
            response.ok && response.response ? [response.response.body] : undefined
          port.postMessage(response, transfer ?? [])
        }
      } catch (error) {
        if (!disconnected) {
          port.postMessage({
            id: request.id,
            ok: false,
            error: serializeError(error),
          } satisfies PortResponse)
        }
      }
    })()
  })
  port.start()
  return disconnect
}

export function serveBrowserSyncHostPort<S extends Schema>(
  host: BrowserSyncHost<S>,
  port: MessagePort
): () => void {
  return serveBrowserSyncHostPortInternal(host, port)
}

export function createBrowserSyncHostPortClient(
  port: MessagePort
): BrowserSyncHostPortClient {
  let nextID = 1
  let closed = false
  const pending = new Map<
    number,
    { resolve(value: PortResponse): void; reject(error: unknown): void }
  >()
  const listeners = new Set<() => void>()

  port.addEventListener('message', (event: MessageEvent<PortResponse | PortEvent>) => {
    const message = event.data
    if ('event' in message) {
      for (const listener of listeners) listener()
      return
    }
    const promise = pending.get(message.id)
    if (!promise) return
    pending.delete(message.id)
    if (message.ok) {
      promise.resolve(message)
    } else {
      const error = new Error(message.error.message)
      error.name = message.error.name
      error.stack = message.error.stack
      promise.reject(error)
    }
  })
  port.start()

  function send(request: PortRequestWithoutID): Promise<PortResponse> {
    if (closed) return Promise.reject(new Error('browser sync host port is closed'))
    const id = nextID++
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject })
      port.postMessage({ ...request, id })
    })
  }

  return {
    async fetch(input, init) {
      const request = input instanceof Request ? input : new Request(input, init)
      const serialized = await serializeRequest(request)
      const result = await send({ operation: 'fetch', request: serialized })
      if (!result.ok || !result.response) throw new Error('missing port response')
      return deserializeResponse(result.response)
    },
    async query(sql, params = []) {
      const result = await send({ operation: 'query', sql, params })
      if (!result.ok) throw new Error('unreachable')
      return result.value as never
    },
    async exec(sql, params = [], metadata) {
      const result = await send({ operation: 'exec', sql, params, metadata })
      if (!result.ok) throw new Error('unreachable')
      return result.value as never
    },
    subscribe(listener) {
      if (closed) throw new Error('browser sync host port is closed')
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    close() {
      if (closed) return
      closed = true
      port.postMessage({ id: nextID++, operation: 'disconnect' } satisfies PortRequest)
      port.close()
      listeners.clear()
      for (const promise of pending.values()) {
        promise.reject(new Error('browser sync host port closed before response'))
      }
      pending.clear()
    },
  }
}
