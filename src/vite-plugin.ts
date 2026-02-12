import { startZeroLite } from './index.js'

import type { ZeroLiteConfig } from './config.js'
import type { Server } from 'node:http'
import type { Plugin } from 'vite'

export interface OrezPluginOptions extends Partial<ZeroLiteConfig> {
  s3?: boolean
  s3Port?: number
  admin?: boolean
  adminPort?: number
  adminLogs?: boolean
}

export default function orez(options?: OrezPluginOptions): Plugin {
  let stop: (() => Promise<void>) | null = null
  let s3Server: Server | null = null
  let adminServer: Server | null = null

  return {
    name: 'orez',

    async configureServer(server) {
      const startTime = Date.now()
      const result = await startZeroLite(options)
      stop = result.stop

      if (options?.s3) {
        const { startS3Local } = await import('./s3-local.js')
        s3Server = await startS3Local({
          port: options.s3Port || 9200,
          dataDir: result.config.dataDir,
        })
      }

      if (options?.admin && result.logStore) {
        const { findPort } = await import('./port.js')
        const { log } = await import('./log.js')
        const adminPort = options.adminPort || (result.config.zeroPort + 2)
        const resolvedPort = await findPort(adminPort)
        const { startAdminServer } = await import('./admin/server.js')
        adminServer = await startAdminServer({
          port: resolvedPort,
          logStore: result.logStore,
          config: result.config,
          zeroEnv: result.zeroEnv,
          actions: result.actions,
          startTime,
          httpLog: result.httpLogStore || undefined,
        })
        log.orez(`admin: http://127.0.0.1:${resolvedPort}`)
        if (result.config.adminLogs) {
          const { resolve } = await import('node:path')
          log.orez(`logs: ${resolve(result.config.dataDir, 'logs', 'orez.log')}`)
        }
      }

      server.httpServer?.on('close', async () => {
        adminServer?.close()
        s3Server?.close()
        if (stop) {
          await stop()
          stop = null
        }
      })
    },
  }
}
