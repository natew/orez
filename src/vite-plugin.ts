import { startZeroLite } from './index.js'

import type { ZeroLiteConfig } from './config.js'
import type { Plugin } from 'vite'

export interface OrezPluginOptions extends Partial<ZeroLiteConfig> {}

export default function orez(options?: OrezPluginOptions): Plugin {
  let stop: (() => Promise<void>) | null = null

  return {
    name: 'orez',

    async configureServer(server) {
      const result = await startZeroLite(options)
      stop = result.stop

      server.httpServer?.on('close', async () => {
        if (stop) {
          await stop()
          stop = null
        }
      })
    },
  }
}
