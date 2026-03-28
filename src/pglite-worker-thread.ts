/**
 * worker thread that runs a single PGlite instance.
 *
 * receives commands via parentPort messages, executes them on the PGlite
 * instance, and sends results back. ArrayBuffers are transferred (not copied)
 * for execProtocolRaw to minimize overhead.
 */

import { parentPort, workerData } from 'node:worker_threads'

import { PGlite } from '@electric-sql/pglite'
import { btree_gin } from '@electric-sql/pglite/contrib/btree_gin'
import { btree_gist } from '@electric-sql/pglite/contrib/btree_gist'
import { citext } from '@electric-sql/pglite/contrib/citext'
import { cube } from '@electric-sql/pglite/contrib/cube'
import { earthdistance } from '@electric-sql/pglite/contrib/earthdistance'
import { fuzzystrmatch } from '@electric-sql/pglite/contrib/fuzzystrmatch'
import { hstore } from '@electric-sql/pglite/contrib/hstore'
import { ltree } from '@electric-sql/pglite/contrib/ltree'
import { pg_trgm } from '@electric-sql/pglite/contrib/pg_trgm'
import { pgcrypto } from '@electric-sql/pglite/contrib/pgcrypto'
import { uuid_ossp } from '@electric-sql/pglite/contrib/uuid_ossp'
import { vector } from '@electric-sql/pglite/vector'

export interface WorkerInitConfig {
  dataDir: string
  name: string
  withExtensions: boolean
  debug: number
  pgliteOptions?: Record<string, unknown>
}

const port = parentPort!
const config = workerData as WorkerInitConfig

// active listen subscriptions
const listeners = new Map<number, () => Promise<void>>()

let db: PGlite

const PGLITE_BASE_FLAGS = ['--single', '-F', '-O', '-j', '-c', 'search_path=public', '-c', 'exit_on_error=false', '-c', 'log_checkpoints=false']

const ZERO_START_PARAMS = [
  ...PGLITE_BASE_FLAGS,
  '-c', 'shared_buffers=128kB',
  '-c', 'wal_buffers=64kB',
  '-c', 'work_mem=64kB',
  '-c', 'maintenance_work_mem=1MB',
  '-c', 'temp_buffers=800kB',
]

async function init() {
  const { dataDir: _userDataDir, debug: _dbg, ...userOpts } = config.pgliteOptions || {}
  const isMain = config.withExtensions

  db = new PGlite({
    dataDir: config.dataDir,
    debug: config.debug,
    relaxedDurability: true,
    initialMemory: isMain ? 32 * 1024 * 1024 : 16 * 1024 * 1024,
    ...(isMain ? {} : { startParams: ZERO_START_PARAMS }),
    ...(isMain ? {
      startParams: [
        ...PGLITE_BASE_FLAGS,
        '-c', 'shared_buffers=4MB',
        '-c', 'wal_buffers=1MB',
      ],
      ...userOpts,
      extensions: userOpts.extensions || {
        vector,
        pg_trgm,
        pgcrypto,
        uuid_ossp,
        citext,
        hstore,
        ltree,
        fuzzystrmatch,
        btree_gin,
        btree_gist,
        cube,
        earthdistance,
      },
    } : { extensions: {} }),
  } as any)

  await db.waitReady

  if (isMain) {
    await db.exec(`
      SET work_mem = '4MB';
      SET maintenance_work_mem = '16MB';
      SET effective_cache_size = '64MB';
      SET random_page_cost = 1.1;
      SET jit = off;
    `)
  } else {
    await db.exec(`SET jit = off;`)
  }

  port.postMessage({ type: 'ready' })
}

port.on('message', async (msg: { type: string; id: number; [key: string]: unknown }) => {
  const { type, id } = msg

  try {
    switch (type) {
      case 'execProtocolRaw': {
        const input = new Uint8Array(msg.data as ArrayBuffer)
        const result = await db.execProtocolRaw(input, msg.options as any)
        // copy result to a transferable buffer (pglite may reuse wasm memory)
        const buf = new ArrayBuffer(result.byteLength)
        new Uint8Array(buf).set(result)
        port.postMessage({ type: 'result', id, data: buf }, [buf])
        break
      }

      case 'query': {
        const result = await db.query(msg.sql as string, msg.params as any[])
        port.postMessage({
          type: 'result',
          id,
          rows: result.rows,
          affectedRows: result.affectedRows,
        })
        break
      }

      case 'exec': {
        const result = await db.exec(msg.sql as string)
        // serialize exec results (array of { affectedRows })
        const results = result.map((r) => ({ affectedRows: r.affectedRows ?? 0 }))
        port.postMessage({ type: 'result', id, results })
        break
      }

      case 'listen': {
        const channel = msg.channel as string
        const unsub = await db.listen(channel, (payload) => {
          port.postMessage({ type: 'notification', channel, payload })
        })
        listeners.set(id, unsub)
        port.postMessage({ type: 'result', id })
        break
      }

      case 'unlisten': {
        const listenId = msg.listenId as number
        const unsub = listeners.get(listenId)
        if (unsub) {
          await unsub()
          listeners.delete(listenId)
        }
        port.postMessage({ type: 'result', id })
        break
      }

      case 'close': {
        for (const unsub of listeners.values()) {
          await unsub().catch(() => {})
        }
        listeners.clear()
        await db.close()
        port.postMessage({ type: 'result', id })
        break
      }

      default:
        port.postMessage({
          type: 'error',
          id,
          message: `unknown message type: ${type}`,
        })
    }
  } catch (err: unknown) {
    const error = err as { message?: string; code?: string }
    port.postMessage({
      type: 'error',
      id,
      message: error?.message || String(err),
      code: error?.code,
    })
  }
})

init().catch((err: unknown) => {
  const error = err as { message?: string }
  port.postMessage({
    type: 'error',
    id: 0,
    message: `worker init failed: ${error?.message || String(err)}`,
  })
  process.exit(1)
})
