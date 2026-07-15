import {
  getReplicationHealth,
  markReplicationProgress,
  signalReplicationChange,
} from '../../replication/handler.js'
import Fastify from '../../worker/shims/fastify.js'
import postgres from '../../worker/shims/postgres-browser.js'
import { Database } from '../../worker/shims/sqlite.js'

type Parent = {
  once(event: string, listener: () => void): void
  send(message: unknown): boolean
}

let probeRunnerCount = 0
let replicationProbeCount = 0

export function hasFirstProbeRunnerStarted(): boolean {
  return probeRunnerCount > 0
}

async function waitForBothInstances(): Promise<void> {
  while (probeRunnerCount < 2) await scheduler.wait(1)
}

export async function runWorker(
  parent: Parent,
  env: Record<string, string>
): Promise<void> {
  const namespace = env.OREZ_PROBE_NAMESPACE
  if (!namespace) throw new Error('OREZ_PROBE_NAMESPACE is required')
  probeRunnerCount++

  // force both DO instances to be live before touching any process-wide shim.
  // a fix that only deletes activeGeneration will cross-route at this barrier.
  await waitForBothInstances()

  const replica = new Database(':do-sqlite:')
  replica.exec(`
    CREATE TABLE probe_sqlite (namespace TEXT PRIMARY KEY);
    CREATE TABLE "_zero.replicationState" (namespace TEXT PRIMARY KEY);
  `)
  replica
    .prepare('INSERT OR REPLACE INTO probe_sqlite (namespace) VALUES (?)')
    .run(namespace)
  replica
    .prepare('INSERT OR REPLACE INTO "_zero.replicationState" (namespace) VALUES (?)')
    .run(namespace)
  const sqliteNamespace = String(
    replica.prepare<{ namespace: string }>('SELECT namespace FROM probe_sqlite').get()
      ?.namespace ?? ''
  )
  const replicationNamespace = String(
    replica
      .prepare<{ namespace: string }>('SELECT namespace FROM "_zero.replicationState"')
      .get()?.namespace ?? ''
  )

  if (namespace === 'alpha') signalReplicationChange()
  else markReplicationProgress()
  replicationProbeCount++
  while (replicationProbeCount < 2) await scheduler.wait(1)
  const replicationHealth = getReplicationHealth()

  const upstream = postgres(env.ZERO_UPSTREAM_DB, { max: 1 })
  const sourceRows = await upstream.unsafe<{ namespace: string }[]>(
    'SELECT namespace FROM probe_source'
  )
  const proxyNamespace = String(sourceRows[0]?.namespace ?? '')

  const fastify = Fastify()
  fastify.get('/probe', (_request, reply) =>
    reply.send({
      namespace,
      proxyNamespace,
      replicationConfirmed: replicationHealth.lastConfirmProgressAt > 0,
      replicationNamespace,
      replicationWriteSignaled: replicationHealth.lastWriteSignalAt > 0,
      sqliteNamespace,
    })
  )
  await fastify.listen({ host: '::', port: 0 })
  parent.send(['ready'])

  await new Promise<void>((resolve) => parent.once('SIGTERM', resolve))
  await upstream.end({ timeout: 1 }).catch(() => {})
  await fastify.close()
  replica.close()
}
