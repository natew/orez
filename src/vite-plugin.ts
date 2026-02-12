import { startZeroLite } from './index.js'

import type { Hook, ZeroLiteConfig } from './config.js'
import type { Server } from 'node:http'
import type { Plugin } from 'vite'

export interface OrezPluginOptions extends Partial<
  Omit<ZeroLiteConfig, 'onDbReady' | 'onHealthy'>
> {
  s3?: boolean
  s3Port?: number
  // lifecycle hooks - callback functions (preferred for vite) or shell commands
  onDbReady?: Hook
  onHealthy?: Hook
}

export function orezPlugin(options?: OrezPluginOptions): Plugin {
  let stop: (() => Promise<void>) | null = null
  let s3Server: Server | null = null

  return {
    name: 'orez',

    async configureServer(server) {
      const result = await startZeroLite(options)
      stop = result.stop

      if (options?.s3) {
        const { startS3Local } = await import('./s3-local.js')
        s3Server = await startS3Local({
          port: options.s3Port || 9200,
          dataDir: result.config.dataDir,
        })
      }

      server.httpServer?.on('close', async () => {
        s3Server?.close()
        if (stop) {
          await stop()
          stop = null
        }
      })
    },
  }
}
