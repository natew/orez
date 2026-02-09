import { startZeroLite } from './index.js'

import type { ZeroLiteConfig } from './config.js'
import type { Server } from 'node:http'
import type { Plugin } from 'vite'

export interface OrezPluginOptions extends Partial<ZeroLiteConfig> {
  s3?: boolean
  s3Port?: number
  bunny?: boolean
  bunnyPort?: number
}

export default function orez(options?: OrezPluginOptions): Plugin {
  let stop: (() => Promise<void>) | null = null
  let s3Server: Server | null = null
  let bunnyServer: Server | null = null

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

      if (options?.bunny) {
        const { startBunnyLocal } = await import('./bunny-local.js')
        bunnyServer = await startBunnyLocal({
          port: options.bunnyPort || 3533,
          dataDir: result.config.dataDir,
        })
      }

      server.httpServer?.on('close', async () => {
        bunnyServer?.close()
        s3Server?.close()
        if (stop) {
          await stop()
          stop = null
        }
      })
    },
  }
}
