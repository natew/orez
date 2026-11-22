import { readdirSync, statSync } from 'node:fs'
import { resolve, sep } from 'node:path'

import {
  generateAppModules,
  renderAppModule,
  type DataAppOptions,
} from './app-modules.js'

import type { BunPlugin } from 'bun'

export function limesync(options: DataAppOptions = {}): BunPlugin {
  return {
    name: 'limesync:data',
    setup({ config, onResolve, onLoad }) {
      const root = config.root ?? process.cwd()
      const dir = resolve(root, options.dir ?? 'data')
      let snapshot: Awaited<ReturnType<typeof loadModules>> | undefined
      let pending: ReturnType<typeof loadModules> | undefined

      async function loadModules() {
        const started = Date.now()
        const {
          files,
          watchFiles: inputs,
          sourceRoots,
        } = await generateAppModules(root, options)
        const watchDirectories = dataDirectories(sourceRoots)
        const watchFiles = [...new Set(inputs)].filter(
          (path): path is string =>
            !!path &&
            !path.includes(`${sep}node_modules${sep}`) &&
            !path.startsWith(resolve(dir, 'generated') + sep) &&
            statSync(path, { throwIfNoEntry: false })?.isFile() === true
        )
        const stamps = new Map(
          [...watchFiles, ...watchDirectories].map((path) => [path, timestamp(path)])
        )
        const next = { files, watchFiles, watchDirectories, stamps }
        if ([...stamps.values()].every((stamp) => stamp < started)) snapshot = next
        return next
      }

      const generate = () => {
        if (
          snapshot &&
          [...snapshot.stamps].every(([path, stamp]) => timestamp(path) === stamp)
        ) {
          return snapshot
        }
        pending ??= loadModules().finally(() => {
          pending = undefined
        })
        return pending
      }

      onResolve(
        { filter: /^limesync:(?:app(?:\/server)?|internal\/.+)$/ },
        ({ path }) => ({
          path: path.slice('limesync:'.length),
          namespace: 'limesync',
        })
      )

      onLoad({ filter: /.*/, namespace: 'limesync' }, async ({ path }) => {
        const { files, watchFiles, watchDirectories } = await generate()
        const file =
          path === 'app'
            ? 'app.ts'
            : path === 'app/server'
              ? 'app-server.ts'
              : path.slice('internal/'.length)
        const contents = await renderAppModule(
          file,
          dir,
          files,
          (sibling) => 'limesync:internal/' + sibling
        )
        return {
          contents,
          loader: 'ts',
          watchFiles,
          watchDirectories,
        }
      })
    },
  }
}

function timestamp(path: string) {
  const stat = statSync(path, { throwIfNoEntry: false })
  return stat ? Math.max(stat.mtimeMs, stat.ctimeMs) : -1
}

function dataDirectories(roots: readonly string[]) {
  const paths = new Set<string>()
  function walk(path: string) {
    if (paths.has(path)) return
    paths.add(path)
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      if (
        entry.isDirectory() &&
        entry.name !== 'generated' &&
        entry.name !== 'node_modules'
      ) {
        walk(resolve(path, entry.name))
      }
    }
  }
  for (const root of roots) walk(root)
  return [...paths]
}
