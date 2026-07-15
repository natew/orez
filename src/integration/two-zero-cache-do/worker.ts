import { DurableObject } from 'cloudflare:workers'

import { ZeroDO } from '../../cf-do/worker.js'
import { doSqliteStorage } from '../../worker/zero-cache-do-sqlite.js'
import {
  startZeroCacheEmbedCF,
  type ZeroCacheEmbedCF,
} from '../../worker/zero-cache-embed-cf.js'
import { hasFirstProbeRunnerStarted } from './probe-run-worker.js'

type Namespace = DurableObjectNamespace

interface Env {
  SOURCE: Namespace
  ZERO_CACHE: Namespace
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

async function requireOk(response: Response, action: string): Promise<void> {
  if (response.ok) return
  throw new Error(`${action}: ${response.status} ${await response.text()}`)
}

export class SourceDO extends ZeroDO {
  private seeded = false

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname !== '/seed') return super.fetch(request)
    if (this.seeded) return Response.json({ ok: true })

    const namespace = url.searchParams.get('namespace')
    if (!namespace) return Response.json({ error: 'namespace required' }, { status: 400 })

    await requireOk(
      await super.fetch(
        new Request('https://source.local/exec', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({
            sql: 'CREATE TABLE IF NOT EXISTS probe_source (namespace TEXT PRIMARY KEY)',
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
            sql: 'INSERT OR REPLACE INTO probe_source (namespace) VALUES (?)',
            params: [namespace],
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
      const embed = await startZeroCacheEmbedCF({
        appId: 'zero',
        backendFetch: (input, init) => source.fetch(new Request(input, init)),
        backendNamespace: namespace,
        doSqlite: doSqliteStorage(this.ctx),
        env: {
          OREZ_PROBE_NAMESPACE: namespace,
          ZERO_ADMIN_PASSWORD: 'probe',
        },
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
      if (url.pathname === '/probe') {
        const response = await embed.handleRequest(
          new Request('https://embed.local/probe')
        )
        return new Response(response.body, response)
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

async function prove(env: Env): Promise<Response> {
  const namespaces = ['alpha', 'bravo'] as const
  const stubs = namespaces.map((namespace) =>
    env.ZERO_CACHE.get(env.ZERO_CACHE.idFromName(namespace))
  )

  const firstBoot = boot(stubs[0], namespaces[0])
  const runnerDeadline = Date.now() + 15_000
  while (!hasFirstProbeRunnerStarted()) {
    if (Date.now() >= runnerDeadline) {
      return Response.json({ error: 'first probe runner did not start' }, { status: 500 })
    }
    await scheduler.wait(1)
  }
  const secondResponse = await boot(stubs[1], namespaces[1])
  if (!secondResponse.ok) {
    return Response.json(
      {
        error: (await secondResponse.json<{ error?: string }>()).error,
        namespace: namespaces[1],
      },
      { status: secondResponse.status }
    )
  }
  const firstResponse = await firstBoot
  if (!firstResponse.ok) return new Response(firstResponse.body, firstResponse)

  const probes = await Promise.all(
    stubs.map(async (stub, index) => {
      const response = await stub.fetch(
        `https://cache.local/probe?namespace=${namespaces[index]}`
      )
      return {
        body: await response.json<Record<string, unknown>>(),
        status: response.status,
      }
    })
  )
  for (const [index, probe] of probes.entries()) {
    const expected = namespaces[index]
    if (probe.status !== 200) {
      return Response.json({ error: 'probe failed', expected, probe }, { status: 500 })
    }
    for (const field of [
      'namespace',
      'proxyNamespace',
      'replicationNamespace',
      'sqliteNamespace',
    ]) {
      if (probe.body[field] !== expected) {
        return Response.json(
          { error: 'namespace cross-route', expected, field, probe: probe.body },
          { status: 500 }
        )
      }
    }
    const expectedReplication =
      expected === 'alpha'
        ? { replicationConfirmed: false, replicationWriteSignaled: true }
        : { replicationConfirmed: true, replicationWriteSignaled: false }
    for (const [field, value] of Object.entries(expectedReplication)) {
      if (probe.body[field] !== value) {
        return Response.json(
          { error: 'replication state cross-route', expected, field, probe: probe.body },
          { status: 500 }
        )
      }
    }
  }

  return Response.json({ ok: true, probes: probes.map((probe) => probe.body) })
}

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    const pathname = new URL(request.url).pathname
    if (pathname === '/health') return Promise.resolve(new Response('ok'))
    if (pathname === '/prove') return prove(env)
    return Promise.resolve(new Response('not found', { status: 404 }))
  },
}
