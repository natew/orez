import { build } from 'esbuild'
import { expect, test } from 'vitest'

test('orez-lite/client stays a browser-only leaf package', async () => {
  const bundle = await build({
    entryPoints: ['packages/orez-lite/src/client/transport.ts'],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    metafile: true,
    write: false,
  })
  const inputs = Object.keys(bundle.metafile.inputs).sort()

  expect(inputs).toEqual([
    'packages/orez-lite/src/client/payload-codec.ts',
    'packages/orez-lite/src/client/transport.ts',
  ])
  expect(bundle.outputFiles[0]?.contents.byteLength).toBeLessThan(40_000)
})
