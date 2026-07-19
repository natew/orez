import { harnessConfig, mintHarnessWakeToken } from './harness-config.js'
import { createSyncDurableObject, createSyncWorker } from './index.js'

import type { SyncHostEnv } from './index.js'

interface Env extends SyncHostEnv {
  SYNC_DO: DurableObjectNamespace
}

const config = harnessConfig<Env>()
const syncWorker = createSyncWorker(config)

export const SyncDurableObject = createSyncDurableObject(config)
export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url)
    const namespace = config.namespace(request)
    const route = url.pathname.split('/').slice(2).join('/')
    if (namespace && route === 'auth/wake-token') {
      const claims = await config.authenticate(request, env)
      if (!claims) {
        return Response.json({ error: 'missing authentication' }, { status: 401 })
      }
      if (!env.ADMIN_KEY) {
        return Response.json({ error: 'wake token minting unavailable' }, { status: 503 })
      }
      return Response.json(
        await mintHarnessWakeToken(namespace, claims.userID, env.ADMIN_KEY)
      )
    }
    const headers = new Headers(request.headers)
    headers.set('x-harness-request-gate', '1')
    const gatedRequest = new Request(request, { headers }) as typeof request
    return syncWorker.fetch!(gatedRequest, env, ctx)
  },
} satisfies ExportedHandler<Env>
