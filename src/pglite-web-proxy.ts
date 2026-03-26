/**
 * PGlite Web Worker proxy — browser equivalent of pglite-ipc.ts.
 *
 * runs in the zero-cache worker, proxies calls to a Web Worker
 * running the actual PGlite instance. mirrors PGliteWorkerProxy
 * from pglite-ipc.ts but uses Web Worker postMessage instead of
 * node worker_threads.
 *
 * ArrayBuffers are transferred (not copied) for execProtocolRaw
 * to keep IPC overhead near-zero for wire protocol data.
 */

import { signalReplicationChange } from './replication/handler.js'

interface PendingRequest {
  resolve: (value: any) => void
  reject: (error: Error) => void
}

const WRITE_PREFIXES = ['insert', 'update', 'delete', 'copy', 'truncate']
const SHARD_INTERNAL_TABLES = ['"replicas"', '"mutations"', '"replicationState"']

function isReplicatedWrite(sql: string): boolean {
  const q = sql.trimStart().toLowerCase()
  if (!WRITE_PREFIXES.some((p) => q.startsWith(p))) return false
  for (const t of SHARD_INTERNAL_TABLES) {
    if (q.includes(t.toLowerCase())) return false
  }
  return true
}

export class PGliteWebProxy {
  private worker: Worker
  private pending = new Map<number, PendingRequest>()
  private nextId = 1
  private notificationCallbacks = new Map<string, Set<(payload: string) => void>>()
  readonly name: string

  readonly waitReady: Promise<void>

  // PGlite compat flags
  closed = false
  ready = false

  constructor(worker: Worker, name: string) {
    this.name = name
    this.worker = worker

    let onReady: () => void
    this.waitReady = new Promise<void>((resolveReady, rejectReady) => {
      onReady = () => {
        this.ready = true
        resolveReady()
      }

      const onMessage = (ev: MessageEvent) => {
        const msg = ev.data
        if (msg?.type === 'ready') {
          this.worker.removeEventListener('message', onMessage)
          this.installMessageHandler()
          onReady()
        } else if (msg?.type === 'error' && msg.id === 0) {
          rejectReady(new Error(msg.message))
        }
      }

      this.worker.addEventListener('message', onMessage)
      this.worker.addEventListener('error', (ev) => {
        rejectReady(new Error(String(ev)))
      })
    })
  }

  private installMessageHandler() {
    this.worker.addEventListener('message', (ev: MessageEvent) => {
      const msg = ev.data
      if (!msg || typeof msg !== 'object') return

      if (msg.type === 'notification') {
        const callbacks = this.notificationCallbacks.get(msg.channel)
        if (callbacks) {
          for (const cb of callbacks) {
            try { cb(msg.payload) } catch {}
          }
        }
        return
      }

      const req = this.pending.get(msg.id)
      if (!req) return
      this.pending.delete(msg.id)

      if (msg.type === 'error') {
        const err = new Error(msg.message) as Error & { code?: string }
        if (msg.code) err.code = msg.code
        req.reject(err)
      } else {
        req.resolve(msg)
      }
    })
  }

  private send(msg: Record<string, unknown>, transfer?: Transferable[]): Promise<any> {
    const id = this.nextId++
    msg.id = id
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      if (transfer?.length) {
        this.worker.postMessage(msg, transfer)
      } else {
        this.worker.postMessage(msg)
      }
    })
  }

  async execProtocolRaw(
    data: Uint8Array,
    options?: { syncToFs?: boolean; throwOnError?: boolean }
  ): Promise<Uint8Array> {
    // copy to a transferable buffer then transfer
    const buf = new ArrayBuffer(data.byteLength)
    new Uint8Array(buf).set(data)
    const result = await this.send({ type: 'execProtocolRaw', data: buf, options }, [buf])
    return new Uint8Array(result.data)
  }

  async query<T = any>(
    sql: string,
    params?: any[]
  ): Promise<{ rows: T[]; affectedRows?: number }> {
    const result = await this.send({ type: 'query', sql, params })
    // signal replication after writes on postgres instance (like orez-node's PGliteWorkerProxy)
    if (this.name === 'postgres' && isReplicatedWrite(sql)) {
      signalReplicationChange()
    }
    return { rows: result.rows ?? [], affectedRows: result.affectedRows }
  }

  async exec(sql: string): Promise<{ affectedRows?: number }[]> {
    const result = await this.send({ type: 'exec', sql })
    if (this.name === 'postgres' && isReplicatedWrite(sql)) {
      signalReplicationChange()
    }
    return result.results ?? []
  }

  async listen(
    channel: string,
    callback: (payload: string) => void
  ): Promise<() => Promise<void>> {
    let callbacks = this.notificationCallbacks.get(channel)
    if (!callbacks) {
      callbacks = new Set()
      this.notificationCallbacks.set(channel, callbacks)
    }
    callbacks.add(callback)

    const result = await this.send({ type: 'listen', channel })
    const listenId = result.id

    return async () => {
      callbacks!.delete(callback)
      if (callbacks!.size === 0) {
        this.notificationCallbacks.delete(channel)
      }
      await this.send({ type: 'unlisten', listenId }).catch(() => {})
    }
  }

  async close(): Promise<void> {
    this.closed = true
    this.ready = false
    try {
      await this.send({ type: 'close' })
    } catch {}
    this.worker.terminate()
  }
}
