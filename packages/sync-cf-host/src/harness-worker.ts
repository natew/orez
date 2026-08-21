import { harnessConfig } from './harness-config.js'
import { createSyncDurableObject, createSyncWorker } from './index.js'

import type { SyncHostEnv } from './index.js'

interface Env extends SyncHostEnv {
  SYNC_DO: DurableObjectNamespace
}

const config = harnessConfig<Env>()
const syncWorker = createSyncWorker(config)
const SyncDurableObjectBase = createSyncDurableObject(config)

export class SyncDurableObject extends SyncDurableObjectBase {
  readonly #storage: DurableObjectStorage

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    this.#storage = ctx.storage
  }

  async fetch(request: Request): Promise<Response> {
    if (new URL(request.url).pathname.endsWith('/extension-schema')) {
      const [{ count }] = this.#storage.sql
        .exec<{ count: number }>('SELECT COUNT(*) AS count FROM project')
        .toArray()
      return Response.json({ count })
    }
    return super.fetch(request)
  }
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const [namespace, route] = new URL(request.url).pathname.split('/').filter(Boolean)
    if (namespace && route === 'extension-schema') {
      return env.SYNC_DO.get(env.SYNC_DO.idFromName(namespace)).fetch(request)
    }
    const headers = new Headers(request.headers)
    headers.set('x-harness-request-gate', '1')
    const gatedRequest = new Request(request, { headers }) as typeof request
    return syncWorker.fetch!(gatedRequest, env, ctx)
  },
} satisfies ExportedHandler<Env>
