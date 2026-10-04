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
  // Byte-bounded request/response streaming and the cross-socket query ack
  // cost ~1.5 KiB in the browser leaf, and superseding a stale pull ~1.1 KiB.
  // Keep that resilience budget explicit while still catching a server
  // dependency graph entering this package.
  expect(bundle.outputFiles[0]?.contents.byteLength).toBeLessThan(44_000)
})
