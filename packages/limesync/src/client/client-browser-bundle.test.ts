import { build } from 'esbuild'
import { expect, test } from 'vitest'

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
    'packages/limesync/src/client/payload-codec.ts',
    'packages/limesync/src/client/transport.ts',
  ])
})
