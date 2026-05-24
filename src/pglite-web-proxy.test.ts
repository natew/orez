import { describe, expect, test } from 'vitest'

import { PGliteWebProxy } from './pglite-web-proxy.js'

class FakeWorker {
  messages: unknown[] = []
  terminated = false
  private listeners = new Map<string, Set<(event: any) => void>>()

  addEventListener(type: string, handler: (event: any) => void) {
    let handlers = this.listeners.get(type)
    if (!handlers) {
      handlers = new Set()
      this.listeners.set(type, handlers)
    }
    handlers.add(handler)
  }

  removeEventListener(type: string, handler: (event: any) => void) {
    this.listeners.get(type)?.delete(handler)
  }

  postMessage(message: unknown) {
    this.messages.push(message)
  }

  terminate() {
    this.terminated = true
  }

  dispatch(type: string, event: any) {
    for (const handler of this.listeners.get(type) ?? []) {
      handler(event)
    }
  }
}

describe('PGliteWebProxy', () => {
  test('rejects pending and future requests when the worker errors', async () => {
    const worker = new FakeWorker()
    const proxy = new PGliteWebProxy(worker as unknown as Worker, 'postgres')

    worker.dispatch('message', { data: { type: 'ready' } })
    await proxy.waitReady

    const pending = proxy.query('SELECT 1')
    expect(worker.messages).toHaveLength(1)

    worker.dispatch('error', {
      error: new Error('pglite worker crashed'),
      message: 'pglite worker crashed',
    })

    await expect(pending).rejects.toThrow('pglite worker crashed')
    await expect(proxy.query('SELECT 2')).rejects.toThrow('pglite worker crashed')
  })
})
