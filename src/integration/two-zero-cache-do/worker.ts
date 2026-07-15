import { DurableObject } from 'cloudflare:workers'

import { ZeroDO } from '../../cf-do/worker.js'
import { DoBackend, releaseDoBackendInstanceCaches } from '../../pg-proxy-do-backend.js'
import { doSqliteStorage } from '../../worker/zero-cache-do-sqlite.js'
import {
  startZeroCacheEmbedCF,
  type ZeroCacheEmbedCF,
} from '../../worker/zero-cache-embed-cf.js'

type Namespace = DurableObjectNamespace

interface Env {
  SOURCE: Namespace
  ZERO_CACHE: Namespace
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.stack || error.message : String(error)
}

const allowAllPermissions = JSON.stringify({
  tables: {
    probe_source: {
      row: {
        select: [['allow', { type: 'and', conditions: [] }]],
      },
    },
  },
})

async function requireOk(response: Response, action: string): Promise<void> {
  if (response.ok) return
  throw new Error(`${action}: ${response.status} ${await response.text()}`)
}

export class SourceDO extends ZeroDO {
  private seeded = false

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const namespace = url.searchParams.get('namespace')

    if (url.pathname === '/write') {
      if (!namespace)
        return Response.json({ error: 'namespace required' }, { status: 400 })
      const value = url.searchParams.get('value')
      if (!value) return Response.json({ error: 'value required' }, { status: 400 })
      await requireOk(
        await super.fetch(
          new Request('https://source.local/exec', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              sql: 'UPDATE probe_source SET value = ? WHERE id = ?',
              params: [value, namespace],
            }),
          })
        ),
        'write live source value'
      )
      return Response.json({ ok: true })
    }

    if (url.pathname !== '/seed') return super.fetch(request)
    if (!namespace) return Response.json({ error: 'namespace required' }, { status: 400 })
    if (this.seeded) return Response.json({ ok: true })

    await requireOk(
      await super.fetch(
        new Request('https://source.local/exec', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            sql: `CREATE TABLE IF NOT EXISTS probe_source (
              id TEXT PRIMARY KEY,
              namespace TEXT NOT NULL,
              value TEXT NOT NULL
            )`,
          }),
        })
      ),
      'create source marker'
    )
    await requireOk(
      await super.fetch(
        new Request('https://source.local/exec', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            sql: `INSERT OR REPLACE INTO probe_source (id, namespace, value)
                  VALUES (?, ?, ?)`,
            params: [namespace, namespace, 'seed'],
          }),
        })
      ),
      'write source marker'
    )
    this.seeded = true
    return Response.json({ ok: true })
  }
}

export class ZeroCacheDO extends DurableObject<Env> {
  private embed: ZeroCacheEmbedCF | undefined
  private namespace = ''
  private starting: Promise<ZeroCacheEmbedCF> | undefined

  private start(namespace: string): Promise<ZeroCacheEmbedCF> {
    if (this.starting) return this.starting
    this.namespace = namespace
    const source = this.env.SOURCE.get(this.env.SOURCE.idFromName(namespace))
    this.starting = (async () => {
      await requireOk(
        await source.fetch(
          `https://source.local/seed?namespace=${encodeURIComponent(namespace)}`
        ),
        'seed source'
      )
      const seedBackend = new DoBackend('https://source.local', 'postgres', namespace, {
        fetch: (input, init) => source.fetch(new Request(input, init)),
        instanceId: namespace,
      })
      try {
        await seedBackend.waitReady
        await seedBackend.exec(
          'CREATE PUBLICATION orez_zero_public FOR TABLE probe_source'
        )
        await seedBackend.exec(
          `CREATE TABLE IF NOT EXISTS "zero"."permissions" (
            "permissions" JSONB,
            "hash" TEXT,
            "lock" BOOL PRIMARY KEY DEFAULT true
          )`
        )
        await seedBackend.query(
          `INSERT INTO "zero"."permissions" ("permissions", "hash", "lock")
           VALUES ($1, $2, true)`,
          [allowAllPermissions, `probe-${namespace}`]
        )
      } finally {
        await seedBackend.close()
        releaseDoBackendInstanceCaches(namespace)
      }
      const embed = await startZeroCacheEmbedCF({
        appId: 'zero',
        backendFetch: (input, init) => source.fetch(new Request(input, init)),
        backendNamespace: namespace,
        doSqlite: doSqliteStorage(this.ctx),
        env: {
          OREZ_PROBE_NAMESPACE: namespace,
          ZERO_ADMIN_PASSWORD: 'probe',
        },
        instanceId: namespace,
        readyTimeout: 15_000,
      })
      this.embed = embed
      return embed
    })()
    return this.starting
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const namespace = url.searchParams.get('namespace') || this.namespace
    if (!namespace) return Response.json({ error: 'namespace required' }, { status: 400 })

    try {
      const embed = await this.start(namespace)
      if (url.pathname === '/boot')
        return Response.json({ namespace, ready: embed.ready })
      if (url.pathname === '/replica') {
        const rows = Array.from(
          this.ctx.storage.sql.exec(
            'SELECT id, namespace, value FROM probe_source ORDER BY id'
          )
        )
        return Response.json({ namespace, rows })
      }
      if (url.pathname === '/ws') {
        const response = await embed.handleRequest(
          new Request(`https://embed.local/sync/v51/connect${url.search}`, {
            headers: request.headers,
          }),
          this.ctx
        )
        return response
      }
      return new Response('not found', { status: 404 })
    } catch (error) {
      return Response.json({ error: errorMessage(error) }, { status: 500 })
    }
  }
}

async function boot(stub: DurableObjectStub, namespace: string): Promise<Response> {
  return stub.fetch(`https://cache.local/boot?namespace=${encodeURIComponent(namespace)}`)
}

interface ProbeRow {
  id: string
  namespace: string
  value: string
}

interface ReplicaProbe {
  namespace: string
  rows: ProbeRow[]
}

interface ProofWebSocket {
  accept(): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, handler: (event: any) => void): void
  removeEventListener(type: string, handler: (event: any) => void): void
}

function encodeSecProtocols(namespace: string): string {
  const initConnectionMessage = [
    'initConnection',
    {
      desiredQueriesPatch: [
        {
          op: 'put',
          hash: `probe-${namespace}`,
          ast: { table: 'probe_source', orderBy: [['id', 'asc']] },
        },
      ],
      clientSchema: {
        tables: {
          probe_source: {
            columns: {
              id: { type: 'string' },
              namespace: { type: 'string' },
              value: { type: 'string' },
            },
            primaryKey: ['id'],
          },
        },
      },
    },
  ]
  const payload = JSON.stringify({ initConnectionMessage })
  const bytes = new TextEncoder().encode(payload)
  const binary = Array.from(bytes, (byte) => String.fromCharCode(byte)).join('')
  return encodeURIComponent(btoa(binary))
}

async function replicaProbe(
  stub: DurableObjectStub,
  namespace: string
): Promise<ReplicaProbe> {
  const response = await stub.fetch(
    `https://cache.local/replica?namespace=${encodeURIComponent(namespace)}`
  )
  if (!response.ok) {
    throw new Error(`replica probe failed: ${response.status} ${await response.text()}`)
  }
  return response.json<ReplicaProbe>()
}

function assertExactRow(probe: ReplicaProbe, namespace: string, value: string): ProbeRow {
  if (probe.namespace !== namespace) {
    throw new Error(
      `replica namespace cross-route: expected ${namespace}, received ${probe.namespace}`
    )
  }
  if (probe.rows.length !== 1) {
    throw new Error(
      `replica row count cross-route for ${namespace}: ${JSON.stringify(probe.rows)}`
    )
  }
  const row = probe.rows[0]
  if (row.id !== namespace || row.namespace !== namespace || row.value !== value) {
    throw new Error(`replica row cross-route for ${namespace}: ${JSON.stringify(row)}`)
  }
  return row
}

async function waitForReplicaValue(
  stub: DurableObjectStub,
  namespace: string,
  value: string
): Promise<ReplicaProbe> {
  const deadline = Date.now() + 15_000
  let latest: ReplicaProbe | undefined
  while (Date.now() < deadline) {
    latest = await replicaProbe(stub, namespace)
    if (latest.rows[0]?.value === value) {
      assertExactRow(latest, namespace, value)
      return latest
    }
    await scheduler.wait(25)
  }
  throw new Error(
    `replication timed out for ${namespace}=${value}: ${JSON.stringify(latest)}`
  )
}

function eventText(data: unknown): string {
  if (typeof data === 'string') return data
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data)
  if (ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(
      new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
    )
  }
  return String(data)
}

async function observeWebSocketRow(
  stub: DurableObjectStub,
  namespace: string,
  value: string
): Promise<void> {
  const secProtocol = encodeSecProtocols(namespace)
  const params = new URLSearchParams({
    baseCookie: '',
    clientGroupID: `probe-cg-${namespace}`,
    clientID: `probe-client-${namespace}`,
    lmid: '0',
    namespace,
    profileID: `probe-profile-${namespace}`,
    schemaVersion: '1',
    ts: String(Date.now()),
    wsid: `probe-ws-${namespace}`,
  })
  const response = await stub.fetch(`https://cache.local/ws?${params}`, {
    headers: {
      'sec-websocket-protocol': secProtocol,
      upgrade: 'websocket',
    },
  })
  const socket = (response as Response & { webSocket?: ProofWebSocket }).webSocket
  if (response.status !== 101 || !socket) {
    throw new Error(
      `websocket upgrade failed for ${namespace}: ${response.status} ${await response.text()}`
    )
  }

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup()
      reject(new Error(`websocket row timed out for ${namespace}`))
    }, 15_000)
    const cleanup = () => {
      clearTimeout(timeout)
      socket.removeEventListener('message', onMessage)
      socket.removeEventListener('close', onClose)
      socket.removeEventListener('error', onError)
    }
    const onMessage = (event: any) => {
      try {
        const message = JSON.parse(eventText(event.data))
        const rows = Array.isArray(message?.[1]?.rowsPatch) ? message[1].rowsPatch : []
        for (const patch of rows) {
          if (patch?.tableName !== 'probe_source' || patch?.op !== 'put') continue
          const row = patch.value as ProbeRow
          if (row.namespace !== namespace) {
            throw new Error(
              `websocket row cross-route for ${namespace}: ${JSON.stringify(row)}`
            )
          }
          if (row.id === namespace && row.value === value) {
            cleanup()
            resolve()
          }
        }
      } catch (error) {
        cleanup()
        reject(error)
      }
    }
    const onClose = (event: any) => {
      cleanup()
      reject(
        new Error(
          `websocket closed before row for ${namespace}: ${event.code} ${event.reason}`
        )
      )
    }
    const onError = (event: any) => {
      cleanup()
      reject(new Error(`websocket error for ${namespace}: ${errorMessage(event.error)}`))
    }

    socket.addEventListener('message', onMessage)
    socket.addEventListener('close', onClose)
    socket.addEventListener('error', onError)
    socket.accept()
  }).finally(() => socket.close(1000, 'proof complete'))
}

async function prove(env: Env): Promise<Response> {
  const namespaces = ['alpha', 'bravo'] as const
  const stubs = namespaces.map((namespace) =>
    env.ZERO_CACHE.get(env.ZERO_CACHE.idFromName(namespace))
  )

  const bootResponses = await Promise.all(
    stubs.map((stub, index) => boot(stub, namespaces[index]))
  )
  for (const [index, response] of bootResponses.entries()) {
    if (response.ok) continue
    return Response.json(
      {
        error: (await response.json<{ error?: string }>()).error,
        namespace: namespaces[index],
      },
      { status: response.status }
    )
  }

  const initialReplicas = await Promise.all(
    stubs.map((stub, index) => replicaProbe(stub, namespaces[index]))
  )
  for (const [index, replica] of initialReplicas.entries()) {
    assertExactRow(replica, namespaces[index], 'seed')
  }

  const liveValues = namespaces.map((namespace) => `live-${namespace}`)
  await Promise.all(
    namespaces.map(async (namespace, index) => {
      const source = env.SOURCE.get(env.SOURCE.idFromName(namespace))
      await requireOk(
        await source.fetch(
          `https://source.local/write?namespace=${namespace}&value=${liveValues[index]}`
        ),
        `write live ${namespace}`
      )
    })
  )

  const liveReplicas = await Promise.all(
    stubs.map((stub, index) =>
      waitForReplicaValue(stub, namespaces[index], liveValues[index])
    )
  )
  await Promise.all(
    stubs.map((stub, index) =>
      observeWebSocketRow(stub, namespaces[index], liveValues[index])
    )
  )

  return Response.json({
    ok: true,
    probes: liveReplicas.map((replica, index) => ({
      namespace: namespaces[index],
      replica: replica.rows[0],
      websocketObserved: true,
    })),
  })
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const pathname = new URL(request.url).pathname
    if (pathname === '/health') return Promise.resolve(new Response('ok'))
    if (pathname === '/prove') return prove(env)
    return Promise.resolve(new Response('not found', { status: 404 }))
  },
}
