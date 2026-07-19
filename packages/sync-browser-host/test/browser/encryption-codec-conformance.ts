import { join } from 'node:path'

import { build } from 'esbuild'
import { chromium } from 'playwright'

const repositoryRoot = join(import.meta.dir, '../../../..')
const result = await build({
  entryPoints: [join(repositoryRoot, 'dist/zero-http/encrypted-column-conformance.js')],
  bundle: true,
  format: 'iife',
  globalName: 'OrezEncryptionConformance',
  platform: 'browser',
  target: 'es2022',
  write: false,
})
const bundle = result.outputFiles[0].text
const browser = await chromium.launch({ channel: 'chromium', headless: true })

try {
  const page = await browser.newPage()
  const errors: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'error') errors.push(message.text())
  })
  page.on('pageerror', (error) => errors.push(error.stack ?? error.message))
  await page.addInitScript(() => {
    Object.defineProperty(globalThis, 'crypto', {
      value: undefined,
      configurable: true,
    })
  })
  await page.goto('about:blank')
  await page.addScriptTag({ content: bundle })
  const conformance = await page.evaluate(() =>
    (
      globalThis as unknown as {
        OrezEncryptionConformance: {
          runEncryptionConformance(): Promise<Record<string, string>>
        }
      }
    ).OrezEncryptionConformance.runEncryptionConformance()
  )
  if (errors.length > 0) {
    throw new Error(`browser console errors:\n${errors.join('\n')}`)
  }
  console.log(
    `OREZ_ENCRYPTION_CONFORMANCE_PASS runtime=chromium ${JSON.stringify(conformance)}`
  )
} finally {
  await browser.close()
}
