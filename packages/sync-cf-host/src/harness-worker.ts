import { createSyncDurableObject, createSyncWorker } from './index.js'
import { harnessConfig } from './harness-config.js'

import type { SyncHostEnv } from './index.js'

interface Env extends SyncHostEnv {
  SYNC_DO: DurableObjectNamespace
}

const config = harnessConfig<Env>()

export const SyncDurableObject = createSyncDurableObject(config)
export default createSyncWorker(config)
