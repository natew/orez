import { resolve } from 'node:path'

import type { LocalSyncHost, LocalSyncHostConfig } from './local.js'
import type { Plugin } from 'vite'

// vite restarts by creating the new server, which runs configureServer, before it
// closes the old one. the old server's host still holds the port at that point, so
// the new instance stops it first instead of failing to bind.
const hostsByPort = new Map<number, LocalSyncHost>()

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
      const { loadLocalConfig } = await import('./local.js')
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
      const { port } = localConfig
      const previous = hostsByPort.get(port)
      if (previous) {
        hostsByPort.delete(port)
        await previous.close()
      }
      const { startLocalSyncHost } = await import('./local.js')
      const host = await startLocalSyncHost({
        ...localConfig,
        dataDir: resolve(root, localConfig.dataDir),
      })
      hostsByPort.set(port, host)
      let closing = false
      const close = async () => {
        if (closing) return
        closing = true
        if (hostsByPort.get(port) === host) hostsByPort.delete(port)
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
