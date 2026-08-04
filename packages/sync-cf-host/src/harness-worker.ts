import { harnessConfig } from './harness-config.js'
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
    const headers = new Headers(request.headers)
    headers.set('x-harness-request-gate', '1')
    const gatedRequest = new Request(request, { headers }) as typeof request
    return syncWorker.fetch!(gatedRequest, env, ctx)
  },
} satisfies ExportedHandler<Env>
