/**
 * regression test for connection reuse after a multi-chunk COPY TO STDOUT.
 *
 * background: zero-cache's initial sync streams every published table over
 * `COPY (...) TO STDOUT` via porsager's `.readable()`. porsager pauses the
 * socket when `stream.push()` reports backpressure and only resumes it from
 * the readable's `read()`. on the MessagePort socket, trailing protocol
 * messages (CopyDone / CommandComplete / ReadyForQuery) that arrive while
 * the socket is paused can be stranded in the pause buffer after the copy
 * stream EOFs — the query never completes, and the NEXT query on that
 * connection hangs forever. observed as zero-cache initial sync hanging on
 * whichever table follows a large copy on the same pooled connection.
 */

import { PGlite } from '@electric-sql/pglite'
import postgres from 'postgres'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { createBrowserProxy } from './pg-proxy-browser.js'
import { createSocketFactory } from './worker/shims/postgres-socket.js'

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<T>((_, reject) => {
    timer = setTimeout(() => reject(new Error(label)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer)
  })
}

describe('connection reuse after COPY TO STDOUT', () => {
  let pg: PGlite

  beforeAll(async () => {
    pg = new PGlite()
    await pg.waitReady
    await pg.exec(`
      CREATE TABLE file (
        id text PRIMARY KEY,
        "projectId" text NOT NULL,
        path text NOT NULL,
        sha text NOT NULL,
        size integer NOT NULL
      )
    `)
    // enough rows that the COPY response spans multiple socket chunks and
    // porsager's backpressure pause/resume cycle actually engages
    await pg.exec(`
      INSERT INTO file (id, "projectId", path, sha, size)
      SELECT 'f' || i, 'proj' || (i % 28), 'src/screen' || i || '.tsx',
             md5(i::text) || md5((i * 31)::text), (i * 37) % 20000 + 50
      FROM generate_series(1, 2000) i
    `)
  }, 30_000)

  afterAll(async () => {
    await pg.close().catch(() => {})
  })

  test('a second query on the same connection completes after a large copy', async () => {
    const proxy = await createBrowserProxy(pg, { pgPassword: '', pgUser: 'u' })
    const sql = postgres({
      socket: createSocketFactory((port) => proxy.handleConnection(port)),
      database: 'postgres',
      username: 'u',
      password: '',
      host: '127.0.0.1',
      port: 0,
      ssl: false,
      max: 1,
      no_subscribe: true,
    } as any)

    try {
      // first: the large copy, fully consumed (mirrors zero's pipeline())
      const readable = await withTimeout(
        sql.unsafe(`COPY (SELECT * FROM file) TO STDOUT`).readable(),
        10_000,
        'first copy readable() timed out'
      )
      let bytes = 0
      await withTimeout(
        (async () => {
          for await (const chunk of readable) bytes += chunk.length
        })(),
        10_000,
        'first copy stream consumption timed out'
      )
      expect(bytes).toBeGreaterThan(100_000)

      // second: ANY query on the same (max: 1) connection — this is what
      // wedges when the trailing ReadyForQuery is stranded in the pause buffer
      const second = await withTimeout(
        sql.unsafe(`SELECT count(*) AS c FROM file`),
        10_000,
        'second query after large copy timed out (connection wedged)'
      )
      expect(Number(second[0].c)).toBe(2000)
    } finally {
      await sql.end({ timeout: 1 }).catch(() => {})
    }
  }, 40_000)
})
