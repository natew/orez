import { startZeroLite } from '../../../src/index'
import { spawn } from 'node:child_process'
import { join } from 'node:path'

const PG_PORT = 6436
const WEB_PORT = 3457

async function main() {
  console.info('starting zerolite...')
  const lite = await startZeroLite({
    dataDir: join(import.meta.dir, '../../../.zero-lite-test'),
    pgPort: PG_PORT,
    zeroPort: 4850,
    s3Port: 10203,
    webPort: WEB_PORT,
    migrationsDir: join(import.meta.dir, '../../src/database/migrations'),
    seedFile: '',
    skipZeroCache: true,
  })

  console.info('starting web server...')
  const server = spawn('bun', ['run', '--hot', join(import.meta.dir, '../../src/server.ts')], {
    env: { ...process.env, PORT: String(WEB_PORT), PG_PORT: String(PG_PORT) },
    stdio: 'inherit',
  })

  // wait for web server
  for (let i = 0; i < 30; i++) {
    try {
      await fetch(`http://localhost:${WEB_PORT}/`)
      break
    } catch {
      await new Promise((r) => setTimeout(r, 500))
    }
  }

  console.info('running playwright tests...')
  const pw = spawn(
    join(import.meta.dir, '../../node_modules/.bin/playwright'),
    ['test', '--config', join(import.meta.dir, '../../playwright.config.ts')],
    {
      env: { ...process.env, BASE_URL: `http://localhost:${WEB_PORT}` },
      stdio: 'inherit',
      cwd: join(import.meta.dir, '../..'),
    }
  )

  const code = await new Promise<number>((resolve) => {
    pw.on('close', (c) => resolve(c ?? 1))
  })

  server.kill()
  await lite.stop()
  process.exit(code)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
