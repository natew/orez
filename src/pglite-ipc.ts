/**
 * PGlite worker proxy — runs in the main thread, proxies calls to a
 * worker thread running the actual PGlite instance.
 *
 * implements the PGlite interface surface used throughout orez:
 * execProtocolRaw, query, exec, listen, close.
 *
 * ArrayBuffers are transferred (not copied) for execProtocolRaw to
 * keep IPC overhead near-zero for wire protocol data.
 */

import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { Worker } from 'node:worker_threads'

import { log } from './log.js'
import { signalReplicationChange } from './replication/handler.js'

import type { WorkerInitConfig } from './pglite-worker-thread.js'

interface PendingRequest {
  resolve: (value: any) => void
  reject: (error: Error) => void
}

const WRITE_PREFIXES = ['insert', 'update', 'delete', 'copy', 'truncate']
function isWriteSQL(sql: string): boolean {
  const q = sql.trimStart().toLowerCase()
  return WRITE_PREFIXES.some((p) => q.startsWith(p))
}

// resolve worker file path — .ts in dev/test (vitest), .js when compiled
function resolveWorkerPath(): string {
  const dir = import.meta.dirname
  const tsPath = resolve(dir, 'pglite-worker-thread.ts')
  if (existsSync(tsPath)) return tsPath
  return resolve(dir, 'pglite-worker-thread.js')
}

export class PGliteWorkerProxy {
  private worker: Worker
  private pending = new Map<number, PendingRequest>()
  private nextId = 1
  private notificationCallbacks = new Map<string, Set<(payload: string) => void>>()
  readonly name: string

  /** resolves when the worker's PGlite instance is ready */
  readonly waitReady: Promise<void>

  constructor(config: WorkerInitConfig) {
    this.name = config.name
    const workerPath = resolveWorkerPath()

    this.worker = new Worker(workerPath, {
      workerData: config,
      name: `pglite-${config.name}`,
    })

    // set up waitReady promise, then install message handler once ready
    let onReady: () => void
    this.waitReady = new Promise<void>((resolveReady, rejectReady) => {
      onReady = () => {
        log.debug.pglite(`worker ${config.name} ready`)
        resolveReady()
      }

      const onMessage = (msg: { type: string; id?: number; message?: string }) => {
        if (msg.type === 'ready') {
          this.worker.off('message', onMessage)
          this.installMessageHandler()
          onReady()
        } else if (msg.type === 'error' && msg.id === 0) {
          rejectReady(new Error(msg.message))
        }
      }

      this.worker.on('message', onMessage)
      this.worker.once('error', rejectReady)
    })

    // handle unexpected worker crashes
    this.worker.on('error', (err) => {
      log.pglite(`worker ${config.name} error: ${err.message}`)
      for (const [, req] of this.pending) {
        req.reject(new Error(`worker crashed: ${err.message}`))
      }
      this.pending.clear()
    })

    this.worker.on('exit', (code) => {
      if (code !== 0) {
        log.pglite(`worker ${config.name} exited with code ${code}`)
        for (const [, req] of this.pending) {
          req.reject(new Error(`worker exited with code ${code}`))
        }
        this.pending.clear()
      }
    })
  }

  private installMessageHandler() {
    this.worker.on(
      'message',
      (msg: { type: string; id?: number; [key: string]: any }) => {
        if (msg.type === 'notification') {
          const callbacks = this.notificationCallbacks.get(msg.channel)
          if (callbacks) {
            for (const cb of callbacks) {
              try {
                cb(msg.payload)
              } catch {}
            }
          }
          return
        }

        const req = this.pending.get(msg.id!)
        if (!req) return
        this.pending.delete(msg.id!)

        if (msg.type === 'error') {
          const err = new Error(msg.message) as Error & { code?: string }
          if (msg.code) err.code = msg.code
          req.reject(err)
        } else {
          req.resolve(msg)
        }
      }
    )
  }

  private send(msg: Record<string, unknown>, transfer?: ArrayBuffer[]): Promise<any> {
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
    // copy to a transferable buffer then transfer (avoids copying in the worker)
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
    if (this.name === 'postgres' && isWriteSQL(sql)) {
      signalReplicationChange()
    }
    return { rows: result.rows ?? [], affectedRows: result.affectedRows }
  }

  async exec(sql: string): Promise<{ affectedRows?: number }[]> {
    const result = await this.send({ type: 'exec', sql })
    if (this.name === 'postgres' && isWriteSQL(sql)) {
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
    try {
      await this.send({ type: 'close' })
    } catch {
      // worker may already be gone
    }
    await this.worker.terminate()
  }
}
