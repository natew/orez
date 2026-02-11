#!/usr/bin/env bun
/**
 * integration test runner: clones ~/chat into test-chat/, links local orez,
 * and runs chat's playwright e2e tests against the local orez backend.
 *
 * usage: bun scripts/test-chat-integration.ts [--skip-clone] [--filter=pattern] [--smoke]
 *
 * automatically finds free ports so it can run alongside existing
 * chat/docker instances. patches test files to use the dynamic web port.
 */

import { spawn, execSync, type ChildProcess } from 'node:child_process'
import { existsSync, rmSync, readFileSync, writeFileSync, readdirSync } from 'node:fs'
import { createConnection, createServer } from 'node:net'
import { resolve } from 'node:path'

const OREZ_ROOT = resolve(import.meta.dirname, '..')
const TEST_DIR = resolve(OREZ_ROOT, 'test-chat')
const CHAT_SOURCE = resolve(process.env.HOME!, 'chat')

const args = process.argv.slice(2)
const skipClone = args.includes('--skip-clone')
const smokeOnly = args.includes('--smoke')
const filterArg = args.find((a) => a.startsWith('--filter='))
const filter = filterArg?.split('=')[1]

const children: ChildProcess[] = []
let exitCode = 0

// check if a port is in use via tcp connect
async function isPortInUse(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = createConnection({ port, host: '127.0.0.1' })
    sock.on('connect', () => { sock.destroy(); resolve(true) })
    sock.on('error', () => resolve(false))
    sock.setTimeout(300, () => { sock.destroy(); resolve(false) })
  })
}

// find a free port, starting from the preferred one
async function findFreePort(preferred: number): Promise<number> {
  for (let port = preferred; port < preferred + 100; port++) {
    if (!(await isPortInUse(port))) return port
  }
  throw new Error(`no free port found near ${preferred}`)
}

async function main() {
  // find free ports (start from standard, fall back to higher range)
  log('finding free ports...')
  const PORTS = {
    pg: await findFreePort(5632),
    zero: await findFreePort(5048),
    web: await findFreePort(8081),
    s3: await findFreePort(9290),
    bunny: await findFreePort(3533),
  }
  log(`ports: pg=${PORTS.pg} zero=${PORTS.zero} web=${PORTS.web} s3=${PORTS.s3} bunny=${PORTS.bunny}`)

  try {
    // step 1: build orez
    log('building orez')
    execSync('bun run build', { cwd: OREZ_ROOT, stdio: 'inherit' })

    // step 2: clone chat repo (or reuse)
    if (!skipClone || !existsSync(TEST_DIR)) {
      log('cloning ~/chat')
      if (existsSync(TEST_DIR)) {
        rmSync(TEST_DIR, { recursive: true, force: true })
      }
      execSync(`git clone --depth 1 "${CHAT_SOURCE}" "${TEST_DIR}"`, {
        stdio: 'inherit',
      })
    } else {
      log('reusing existing test-chat (--skip-clone)')
    }

    // step 3: install deps (idempotent — fast if already done)
    log('installing dependencies')
    execSync('bun install --ignore-scripts', {
      cwd: TEST_DIR,
      stdio: 'inherit',
      timeout: 300_000,
    })
    // run essential postinstall parts (skip tko CLI which isn't available in shallow clone)
    try {
      execSync('bun run one patch', { cwd: TEST_DIR, stdio: 'inherit', timeout: 30_000 })
    } catch {}
    try {
      execSync('bun tko run generate-env', { cwd: TEST_DIR, stdio: 'inherit', timeout: 30_000 })
    } catch {}

    // step 4: copy local orez build into node_modules
    log('installing local orez build')
    const orezInModules = resolve(TEST_DIR, 'node_modules', 'orez')
    if (existsSync(orezInModules)) {
      rmSync(orezInModules, { recursive: true, force: true })
    }
    const { mkdirSync: mkdir, cpSync } = await import('node:fs')
    mkdir(orezInModules, { recursive: true })
    cpSync(resolve(OREZ_ROOT, 'dist'), resolve(orezInModules, 'dist'), { recursive: true })
    cpSync(resolve(OREZ_ROOT, 'package.json'), resolve(orezInModules, 'package.json'))
    if (existsSync(resolve(OREZ_ROOT, 'src'))) {
      cpSync(resolve(OREZ_ROOT, 'src'), resolve(orezInModules, 'src'), { recursive: true })
    }
    log(`orez installed from local build`)

    // step 5: install playwright
    log('installing playwright chromium')
    execSync('bunx playwright install chromium', {
      cwd: TEST_DIR,
      stdio: 'inherit',
      timeout: 120_000,
    })

    // step 6: env setup — merge secrets from .env into .env.development, remove .env
    // .env has production values (VITE_ZERO_HOSTNAME etc) that conflict with local dev.
    // we keep only .env.development with dynamic ports + secrets merged in.
    const sourceEnv = resolve(CHAT_SOURCE, '.env')
    const envDevPath = resolve(TEST_DIR, '.env.development')
    if (existsSync(sourceEnv) && existsSync(envDevPath)) {
      const secrets = readFileSync(sourceEnv, 'utf-8')
      let envDev = readFileSync(envDevPath, 'utf-8')
      // merge secret keys not already in .env.development (skip production-only vars)
      const devKeys = new Set(envDev.match(/^[A-Za-z_][A-Za-z0-9_]*/gm) || [])
      const skipKeys = new Set(['VITE_ZERO_HOSTNAME', 'ZERO_DOMAIN', 'VITE_ZERO_URL'])
      for (const line of secrets.split('\n')) {
        const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/)
        if (match && !devKeys.has(match[1]) && !skipKeys.has(match[1])) {
          envDev += `\n${line}`
        }
      }
      // update dynamic ports
      envDev = envDev
        .replace(/VITE_PORT_WEB=\d+/, `VITE_PORT_WEB=${PORTS.web}`)
        .replace(/VITE_PORT_ZERO=\d+/, `VITE_PORT_ZERO=${PORTS.zero}`)
        .replace(/VITE_PORT_POSTGRES=\d+/, `VITE_PORT_POSTGRES=${PORTS.pg}`)
        .replace(/VITE_PORT_MINIO=\d+/, `VITE_PORT_MINIO=${PORTS.s3}`)
        .replace(/(ZERO_UPSTREAM_DB=.*127\.0\.0\.1:)\d+/g, `$1${PORTS.pg}`)
        .replace(/(ZERO_CVR_DB=.*127\.0\.0\.1:)\d+/g, `$1${PORTS.pg}`)
        .replace(/(ZERO_CHANGE_DB=.*127\.0\.0\.1:)\d+/g, `$1${PORTS.pg}`)
      writeFileSync(envDevPath, envDev)
      // remove .env entirely so nothing overrides .env.development
      const testDotEnv = resolve(TEST_DIR, '.env')
      if (existsSync(testDotEnv)) rmSync(testDotEnv)
      log('merged secrets into .env.development, removed .env')
    }

    // step 7: patch hardcoded ports everywhere (source + test files)
    log(`patching ports: 8081→${PORTS.web}, 5048→${PORTS.zero}`)
    execSync(
      `find src playwright.config.ts -type f \\( -name "*.ts" -o -name "*.tsx" \\) -exec sed -i '' ` +
      `-e 's/localhost:8081/localhost:${PORTS.web}/g' ` +
      `-e 's/localhost:5048/localhost:${PORTS.zero}/g' ` +
      `-e "s/'5048'/'${PORTS.zero}'/g" {} +`,
      { cwd: TEST_DIR, stdio: 'inherit' }
    )

    // step 8: clean all caches (stale compiled modules with old ports)
    for (const cache of ['node_modules/.vite', 'node_modules/.vxrn', 'node_modules/.cache']) {
      const cachePath = resolve(TEST_DIR, cache)
      if (existsSync(cachePath)) {
        rmSync(cachePath, { recursive: true, force: true })
      }
    }
    log('cleared caches')

    // step 9: build database schemas
    log('building database schemas')
    execSync('bun db:build', {
      cwd: TEST_DIR,
      stdio: 'inherit',
      timeout: 120_000,
    })

    // step 10: clean .orez data dir
    const orezDataDir = resolve(TEST_DIR, '.orez')
    if (existsSync(orezDataDir)) {
      log('cleaning .orez data dir')
      rmSync(orezDataDir, { recursive: true, force: true })
    }

    // step 11: start orez lite backend
    log('starting orez lite backend')
    const backendProc = spawn(
      'bun',
      [
        'run:dev',
        'orez',
        `--pg-port=${PORTS.pg}`,
        `--zero-port=${PORTS.zero}`,
        '--s3',
        `--s3-port=${PORTS.s3}`,
        '--bunny',
        `--bunny-port=${PORTS.bunny}`,
        `--on-db-ready=bun db:migrate`,
        '--migrations=./no',
      ],
      {
        cwd: TEST_DIR,
        stdio: 'inherit',
        env: {
          ...process.env,
          ZERO_MUTATE_URL: `http://localhost:${PORTS.web}/api/zero/push`,
          ZERO_QUERY_URL: `http://localhost:${PORTS.web}/api/zero/pull`,
        },
      },
    )
    children.push(backendProc)

    backendProc.on('exit', (code) => {
      if (code && code !== 0) {
        console.error(`backend exited with code ${code}`)
      }
    })

    // wait for pg + zero to be ready
    log('waiting for backend (pg + zero-cache)...')
    await waitForPort(PORTS.zero, 120_000, 'zero-cache')
    log('backend ready')

    // step 12: start web frontend (dev mode with --clean)
    log('starting web frontend')
    const webProc = spawn('bun', ['run:dev', 'one', 'dev', '--clean', '--port', String(PORTS.web)], {
      cwd: TEST_DIR,
      stdio: 'inherit',
      env: {
        ...process.env,
        ALLOW_MISSING_ENV: '1',
        VITE_PORT_WEB: String(PORTS.web),
        VITE_PORT_ZERO: String(PORTS.zero),
        VITE_PUBLIC_ZERO_SERVER: `http://localhost:${PORTS.zero}`,
        ONE_SERVER_URL: `http://localhost:${PORTS.web}`,
        ZERO_MUTATE_URL: `http://localhost:${PORTS.web}/api/zero/push`,
        ZERO_QUERY_URL: `http://localhost:${PORTS.web}/api/zero/pull`,
      },
    })
    children.push(webProc)

    webProc.on('exit', (code) => {
      if (code && code !== 0) {
        console.error(`web server exited with code ${code}`)
      }
    })

    log('waiting for web server...')
    await waitForPort(PORTS.web, 120_000, 'web')
    log('web server ready')

    // step 13: run playwright tests
    log('running playwright tests')
    const testArgs = ['playwright', 'test']
    if (filter) {
      testArgs.push(filter)
    } else if (smokeOnly) {
      testArgs.push('src/integration/e2e/orez-smoke.test.ts')
    }
    testArgs.push('--project=chromium')

    // load .env.development vars for playwright context
    const dotenvVars: Record<string, string> = {}
    for (const envFile of ['.env', '.env.development']) {
      const envPath = resolve(TEST_DIR, envFile)
      if (!existsSync(envPath)) continue
      const content = readFileSync(envPath, 'utf-8')
      for (const line of content.split('\n')) {
        const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/)
        if (match) {
          let val = match[2].trim()
          if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
            val = val.slice(1, -1)
          }
          dotenvVars[match[1]] = val
        }
      }
    }

    try {
      execSync(`bunx ${testArgs.join(' ')}`, {
        cwd: TEST_DIR,
        stdio: 'inherit',
        timeout: 600_000,
        env: {
          ...dotenvVars,
          ...process.env,
          CI: 'true',
          NODE_ENV: 'test',
          ALLOW_MISSING_ENV: '1',
          ZERO_MUTATE_URL: `http://localhost:${PORTS.web}/api/zero/push`,
          ZERO_QUERY_URL: `http://localhost:${PORTS.web}/api/zero/pull`,
          ZERO_UPSTREAM_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/postgres`,
          ZERO_CVR_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cvr`,
          ZERO_CHANGE_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cdb`,
          VITE_PORT_WEB: String(PORTS.web),
          VITE_PORT_ZERO: String(PORTS.zero),
          VITE_PORT_POSTGRES: String(PORTS.pg),
          VITE_PUBLIC_ZERO_SERVER: `http://localhost:${PORTS.zero}`,
          ONE_SERVER_URL: `http://localhost:${PORTS.web}`,
          DATABASE_URL: `postgresql://user:password@127.0.0.1:${PORTS.pg}/postgres`,
        },
      })
      log('TESTS PASSED')
    } catch (err: any) {
      log('TESTS FAILED')
      exitCode = err.status || 1
    }
  } catch (err: any) {
    console.error(`\nerror: ${err.message || err}`)
    exitCode = 1
  } finally {
    await cleanup()
    process.exit(exitCode)
  }
}

function log(msg: string) {
  console.log(`\n\x1b[1m\x1b[36m[test-chat]\x1b[0m ${msg}`)
}

async function cleanup() {
  log('cleaning up')
  for (const child of children) {
    if (!child.killed) {
      child.kill('SIGTERM')
    }
  }
  await new Promise((r) => setTimeout(r, 2000))
  for (const child of children) {
    if (!child.killed) {
      child.kill('SIGKILL')
    }
  }
}

async function waitForPort(port: number, timeoutMs: number, name: string): Promise<void> {
  const start = Date.now()
  const deadline = start + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/`)
      if (res.ok || res.status === 404 || res.status === 401 || res.status === 302) {
        return
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 1000))
  }
  throw new Error(`${name} (port ${port}) not ready after ${Math.round(timeoutMs / 1000)}s`)
}

process.on('SIGINT', async () => {
  console.log('\ninterrupted')
  await cleanup()
  process.exit(130)
})

process.on('SIGTERM', async () => {
  await cleanup()
  process.exit(143)
})

main()
