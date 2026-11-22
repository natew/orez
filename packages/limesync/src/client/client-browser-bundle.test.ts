import { build } from 'esbuild'
import { expect, test } from 'vitest'

import { installHttpPullTransport } from './transport.js'

test('limesync/client stays a browser-only leaf package', async () => {
  const bundle = await build({
    entryPoints: ['packages/limesync/src/client/transport.ts'],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    metafile: true,
    write: false,
  })
  const inputs = Object.keys(bundle.metafile.inputs).sort()

  expect(inputs).toEqual([
    'packages/limesync/src/client/transport.ts',
    'packages/sync-executor/dist/zero/http/payload-codec.js',
    'packages/sync-executor/dist/zero/http/transport.js',
  ])
})

test('separately bundled Zero opens the installed HTTP transport', async () => {
  const bundle = await build({
    entryPoints: ['packages/sync-executor/dist/zero/http/transport.js'],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    write: false,
  })
  const moduleURL = `data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].contents).toString('base64')}`
  const { openHttpConnection } = await import(/* @vite-ignore */ moduleURL)
  const origin = 'https://separate-client.example.test'
  const transport = installHttpPullTransport({ origin, fetch: globalThis.fetch })
  try {
    const connection = openHttpConnection(origin, {
      clientGroupID: 'group',
      clientID: 'client',
      connectionID: 'connection',
      baseCookie: null,
    })
    try {
      expect(transport.connections).toBe(1)
    } finally {
      connection.close()
    }
  } finally {
    transport.uninstall()
  }
})
