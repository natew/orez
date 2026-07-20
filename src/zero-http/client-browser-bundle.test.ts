import { readFile } from 'node:fs/promises'

import { build } from 'esbuild'
import { expect, test } from 'vitest'

test('orez/client stays a browser-only leaf package', async () => {
  const packageJSON = JSON.parse(await readFile('package.json', 'utf8'))
  expect(packageJSON.exports['./client']).toEqual(packageJSON.exports['./zero-http'])

  const bundle = await build({
    entryPoints: ['src/zero-http/transport.ts'],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    metafile: true,
    write: false,
  })
  const inputs = Object.keys(bundle.metafile.inputs).sort()

  expect(inputs).toEqual(['src/zero-http/payload-codec.ts', 'src/zero-http/transport.ts'])
  expect(bundle.outputFiles[0]?.contents.byteLength).toBeLessThan(40_000)
})
