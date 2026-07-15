import { mkdir, rm } from 'node:fs/promises'
import { join } from 'node:path'

import { build } from 'esbuild'
import { chromium } from 'playwright'

const packageRoot = join(import.meta.dir, '../..')
const repositoryRoot = join(packageRoot, '../..')
const outputDirectory = join(packageRoot, 'dist/browser-test')

await rm(outputDirectory, { recursive: true, force: true })
await mkdir(outputDirectory, { recursive: true })
await mkdir(join(outputDirectory, 'generated'), { recursive: true })

await build({
  alias: {
    'orez/sync-browser-host': join(repositoryRoot, 'dist/sync-browser-host/index.js'),
  },
  entryPoints: {
    main: join(import.meta.dir, 'main.ts'),
    worker: join(import.meta.dir, 'worker.ts'),
  },
  outdir: outputDirectory,
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  sourcemap: true,
})

await Bun.write(
  join(outputDirectory, 'generated/sqlite3-browser.wasm'),
  Bun.file(join(repositoryRoot, 'sqlite-wasm/dist/sqlite3-browser.wasm'))
)
await Bun.write(
  join(outputDirectory, 'generated/sync_wasm_bg.wasm'),
  Bun.file(join(packageRoot, 'src/generated/sync_wasm_bg.wasm'))
)

const contentTypes: Record<string, string> = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.map': 'application/json',
  '.wasm': 'application/wasm',
}

const server = Bun.serve({
  port: 0,
  async fetch(request) {
    const pathname = new URL(request.url).pathname
    const path =
      pathname === '/'
        ? join(import.meta.dir, 'index.html')
        : join(outputDirectory, pathname.slice(1))
    const file = Bun.file(path)
    if (!(await file.exists())) return new Response('not found', { status: 404 })
    const extension = path.slice(path.lastIndexOf('.'))
    return new Response(file, {
      headers: { 'content-type': contentTypes[extension] ?? 'application/octet-stream' },
    })
  },
})

const browser = await chromium.launch({ headless: true })
const page = await browser.newPage()
page.setDefaultTimeout(60_000)
const consoleErrors: string[] = []
page.on('console', (message) => {
  if (message.type() === 'error') consoleErrors.push(message.text())
})
page.on('pageerror', (error) => consoleErrors.push(error.stack ?? error.message))

try {
  await page.goto(server.url.toString(), { waitUntil: 'networkidle' })
  await page.waitForFunction(() => 'runBrowserHostSpike' in globalThis)
  const result = await page.evaluate(() =>
    (
      globalThis as unknown as {
        runBrowserHostSpike(): Promise<Record<string, unknown>>
      }
    ).runBrowserHostSpike()
  )
  if (consoleErrors.length > 0) {
    throw new Error(`browser console errors:\n${consoleErrors.join('\n')}`)
  }
  console.log(JSON.stringify(result, null, 2))
} finally {
  await browser.close()
  server.stop(true)
}
