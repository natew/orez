/**
 * PGlite Web Worker — browser equivalent of pglite-worker-thread.ts.
 *
 * runs a single PGlite instance in a Web Worker. receives commands via
 * postMessage, executes on PGlite, sends results back. ArrayBuffers
 * are transferred (not copied) for execProtocolRaw.
 *
 * message protocol (same as pglite-worker-thread.ts):
 *   init: { type: 'init', dataDir, name, withExtensions, pgliteOptions }
 *   → { type: 'ready' }
 *
 *   execProtocolRaw: { type: 'execProtocolRaw', id, data: ArrayBuffer, options }
 *   → { type: 'result', id, data: ArrayBuffer }
 *
 *   query: { type: 'query', id, sql, params }
 *   → { type: 'result', id, rows, affectedRows }
 *
 *   exec: { type: 'exec', id, sql }
 *   → { type: 'result', id, results: [{ affectedRows }] }
 *
 *   listen/unlisten/close: same as pglite-worker-thread.ts
 */

// NOTE: this file is meant to be bundled with PGlite as external
// the consumer provides PGlite via importScripts or ESM import

declare const self: DedicatedWorkerGlobalScope

const listeners = new Map<number, () => Promise<void>>()
let db: any // PGlite instance — type depends on how it's loaded

self.onmessage = async (ev: MessageEvent) => {
  const msg = ev.data
  if (!msg || typeof msg !== 'object') return
  const { type, id } = msg

  try {
    switch (type) {
      case 'init': {
        // dynamically import PGlite (external, provided by consumer's bundler)
        const { PGlite } = await import('@electric-sql/pglite')
        db = new PGlite({
          dataDir: msg.dataDir || 'idb://orez-pglite',
          relaxedDurability: true,
          ...(msg.pgliteOptions || {}),
          // extensions loaded by consumer if needed
        })
        await db.waitReady

        // tune for throughput
        await db.exec(`
          SET work_mem = '16MB';
          SET jit = off;
        `)

        self.postMessage({ type: 'ready' })
        break
      }

      case 'execProtocolRaw': {
        const input = new Uint8Array(msg.data as ArrayBuffer)
        const result = await db.execProtocolRaw(input, msg.options)
        const buf = new ArrayBuffer(result.byteLength)
        new Uint8Array(buf).set(result)
        self.postMessage({ type: 'result', id, data: buf }, [buf])
        break
      }

      case 'query': {
        const result = await db.query(msg.sql, msg.params)
        self.postMessage({
          type: 'result',
          id,
          rows: result.rows,
          affectedRows: result.affectedRows,
        })
        break
      }

      case 'exec': {
        const result = await db.exec(msg.sql)
        const results = result.map((r: any) => ({ affectedRows: r.affectedRows ?? 0 }))
        self.postMessage({ type: 'result', id, results })
        break
      }

      case 'listen': {
        const unsub = await db.listen(msg.channel, (payload: string) => {
          self.postMessage({ type: 'notification', channel: msg.channel, payload })
        })
        listeners.set(id, unsub)
        self.postMessage({ type: 'result', id })
        break
      }

      case 'unlisten': {
        const unsub = listeners.get(msg.listenId)
        if (unsub) {
          await unsub()
          listeners.delete(msg.listenId)
        }
        self.postMessage({ type: 'result', id })
        break
      }

      case 'close': {
        for (const unsub of listeners.values()) {
          await unsub().catch(() => {})
        }
        listeners.clear()
        await db.close()
        self.postMessage({ type: 'result', id })
        break
      }

      default:
        self.postMessage({
          type: 'error',
          id,
          message: `unknown message type: ${type}`,
        })
    }
  } catch (err: unknown) {
    const error = err as { message?: string; code?: string }
    self.postMessage({
      type: 'error',
      id,
      message: error?.message || String(err),
      code: error?.code,
    })
  }
}
