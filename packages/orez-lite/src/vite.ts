import { resolve } from 'node:path'

import { loadLocalConfig, startLocalSyncHost, type LocalSyncHostConfig } from './local.js'

import type { Plugin } from 'vite'

export interface OrezLitePluginOptions {
  config?: string
  proxyPath?: string
}

export function orez(options: OrezLitePluginOptions = {}): Plugin {
  const proxyPath = options.proxyPath ?? '/zero-http'
  if (!proxyPath.startsWith('/')) {
    throw new TypeError('proxyPath must start with /')
  }

  let root = process.cwd()
  let localConfig: LocalSyncHostConfig | undefined

  return {
    name: 'orez-lite',
    apply: 'serve',

    async config(config) {
      root = resolve(config.root ?? process.cwd())
      localConfig = await loadLocalConfig(
        resolve(root, options.config ?? 'orez-lite.config.ts')
      )
      const namespace = localConfig.namespace
      return {
        server: {
          proxy: {
            [proxyPath]: {
              target: `http://${localConfig.host ?? '127.0.0.1'}:${localConfig.port}`,
              changeOrigin: false,
              ws: true,
              rewrite(path: string) {
                return `/${namespace}${path.slice(proxyPath.length)}`
              },
            },
          },
        },
      }
    },

    configResolved(config) {
      root = config.root
    },

    async configureServer(server) {
      if (!localConfig) {
        throw new Error('orez-lite local configuration was not loaded')
      }
      const host = await startLocalSyncHost({
        ...localConfig,
        dataDir: resolve(root, localConfig.dataDir),
      })
      let closing = false
      const close = async () => {
        if (closing) return
        closing = true
        await host.close()
      }

      server.httpServer?.once('close', close)
      server.watcher.once('close', close)
      void host.exited.then((exit) => {
        if (exit.expected) return
        const reason = exit.signal ? `signal ${exit.signal}` : `code ${exit.code}`
        server.config.logger.error(`[orez-lite] native sync host exited (${reason})`)
        void server.close()
      })
    },
  }
}

export default orez
