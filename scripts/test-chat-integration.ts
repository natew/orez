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
  // ensure correct node version for chat (needs crypto.hash from node 22+)
  const chatNodeVersion = (() => {
    try {
      const pkg = JSON.parse(readFileSync(resolve(CHAT_SOURCE, 'package.json'), 'utf-8'))
      return pkg.engines?.node
    } catch { return null }
  })()
  if (chatNodeVersion) {
    try {
      // find fnm dir (may contain spaces, e.g. "Application Support")
      const fnmDirMatch = execSync('fnm env --shell bash', { encoding: 'utf-8' })
        .match(/FNM_DIR="([^"]+)"/)
      const fnmBase = fnmDirMatch?.[1] || resolve(process.env.HOME!, 'Library', 'Application Support', 'fnm')
      const versionDir = resolve(fnmBase, 'node-versions', `v${chatNodeVersion}`, 'installation', 'bin')
      if (existsSync(versionDir)) {
        process.env.PATH = `${versionDir}:${process.env.PATH}`
        log(`using node ${chatNodeVersion} from ${versionDir}`)
      } else {
        execSync(`fnm install ${chatNodeVersion}`, { stdio: 'inherit' })
        if (existsSync(versionDir)) {
          process.env.PATH = `${versionDir}:${process.env.PATH}`
          log(`installed and using node ${chatNodeVersion}`)
        }
      }
    } catch (e: any) {
      log(`warning: could not switch to node ${chatNodeVersion}: ${e.message}`)
    }
  }

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
      // sync critical source files from ~/chat so schema/model changes are picked up
      log('syncing schema + models from ~/chat')
      const syncDirs = ['src/database', 'src/data', 'src/server', 'src/apps']
      for (const dir of syncDirs) {
        const src = resolve(CHAT_SOURCE, dir)
        const dst = resolve(TEST_DIR, dir)
        if (existsSync(src)) {
          execSync(`rsync -a --delete "${src}/" "${dst}/"`, { stdio: 'inherit' })
        }
      }
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
    // ensure bin link points to the right cli entry
    const binDir = resolve(TEST_DIR, 'node_modules', '.bin')
    const orezBinLink = resolve(binDir, 'orez')
    if (existsSync(orezBinLink)) rmSync(orezBinLink)
    const { symlinkSync } = await import('node:fs')
    symlinkSync(resolve(orezInModules, 'dist', 'cli-entry.js'), orezBinLink)
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
        .replace(/(CLOUDFLARE_R2_ENDPOINT=.*localhost:)\d+/g, `$1${PORTS.s3}`)
        .replace(/(CLOUDFLARE_R2_PUBLIC_URL=.*localhost:)\d+/g, `$1${PORTS.s3}`)
        .replace(/VITE_ZERO_HOSTNAME=localhost:\d+/, `VITE_ZERO_HOSTNAME=localhost:${PORTS.zero}`)
        .replace(/VITE_WEB_HOSTNAME=localhost:\d+/, `VITE_WEB_HOSTNAME=localhost:${PORTS.web}`)
        .replace(/(BETTER_AUTH_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
        .replace(/(ONE_SERVER_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
        // replace docker host.docker.internal refs with localhost
        .replace(/host\.docker\.internal/g, 'localhost')
        .replace(/(ZERO_MUTATE_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
        .replace(/(ZERO_QUERY_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
      writeFileSync(envDevPath, envDev)
      // remove .env entirely so nothing overrides .env.development
      const testDotEnv = resolve(TEST_DIR, '.env')
      if (existsSync(testDotEnv)) rmSync(testDotEnv)
      log('merged secrets into .env.development, removed .env')
    }

    // step 7: patch hardcoded ports everywhere (source + test files)
    // use broad regex ranges so --skip-clone works even when a previous run
    // patched ports to different values (e.g. 8081→8082 then 8082→8083)
    log(`patching ports: web→${PORTS.web}, zero→${PORTS.zero}, bunny→${PORTS.bunny}, s3→${PORTS.s3}`)
    execSync(
      `find src playwright.config.ts -type f \\( -name "*.ts" -o -name "*.tsx" \\) -exec sed -i '' -E ` +
      `-e 's/localhost:8[0-1][0-9][0-9]/localhost:${PORTS.web}/g' ` +
      `-e 's/localhost:50[0-9][0-9]/localhost:${PORTS.zero}/g' ` +
      `-e 's/localhost:35[0-9][0-9]/localhost:${PORTS.bunny}/g' ` +
      `-e 's/localhost:92[0-9][0-9]/localhost:${PORTS.s3}/g' ` +
      `-e "s/'50[0-9][0-9]'/'${PORTS.zero}'/g" {} +`,
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

    // step 9: build database migration scripts
    log('building database migrations')
    execSync('bun migrate build', {
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

    // step 10b: replace @rocicorp/zero-sqlite3 with bedrock-sqlite wasm shim
    // zero-cache uses CJS require() for sqlite3. on Node 20, ESM loader hooks
    // don't intercept CJS require(). so we replace the actual package in
    // node_modules with a shim that redirects to bedrock-sqlite wasm.
    log('shimming @rocicorp/zero-sqlite3 → bedrock-sqlite wasm')
    const zeroSqlitePkg = resolve(TEST_DIR, 'node_modules', '@rocicorp', 'zero-sqlite3')
    const bedrockPath = resolve(TEST_DIR, 'node_modules', 'bedrock-sqlite', 'dist', 'sqlite3.js')
    if (existsSync(zeroSqlitePkg)) {
      rmSync(zeroSqlitePkg, { recursive: true, force: true })
    }
    const { mkdirSync: mkdirShim } = await import('node:fs')
    mkdirShim(resolve(zeroSqlitePkg, 'lib'), { recursive: true })
    writeFileSync(resolve(zeroSqlitePkg, 'package.json'), JSON.stringify({
      name: '@rocicorp/zero-sqlite3', version: '0.0.0-shim', main: './lib/index.js'
    }))
    writeFileSync(resolve(zeroSqlitePkg, 'lib', 'index.js'), `'use strict';
var mod = require('${bedrockPath}');
var OrigDatabase = mod.Database;
var SqliteError = mod.SqliteError;
function Database() {
  var db = new OrigDatabase(...arguments);
  try { db.pragma('busy_timeout = 30000'); db.pragma('synchronous = normal'); } catch(e) {}
  return db;
}
Database.prototype = OrigDatabase.prototype;
Database.prototype.constructor = Database;
Object.keys(OrigDatabase).forEach(function(k) { Database[k] = OrigDatabase[k]; });
Database.prototype.unsafeMode = function() { return this; };
// wrap pragma to swallow SQLITE_CORRUPT on optimize (bedrock-sqlite wasm issue)
var origPragma = OrigDatabase.prototype.pragma;
Database.prototype.pragma = function(str, opts) {
  try { return origPragma.call(this, str, opts); }
  catch(e) { if (e && e.code === 'SQLITE_CORRUPT') return []; throw e; }
};
if (!Database.prototype.defaultSafeIntegers) Database.prototype.defaultSafeIntegers = function() { return this; };
if (!Database.prototype.serialize) Database.prototype.serialize = function() { throw new Error('not supported in wasm'); };
if (!Database.prototype.backup) Database.prototype.backup = function() { throw new Error('not supported in wasm'); };
var tmpDb = new OrigDatabase(':memory:');
var tmpStmt = tmpDb.prepare('SELECT 1');
var SP = Object.getPrototypeOf(tmpStmt);
if (!SP.safeIntegers) SP.safeIntegers = function() { return this; };
SP.scanStatus = function() { return undefined; };
SP.scanStatusV2 = function() { return []; };
SP.scanStatusReset = function() {};
tmpDb.close();
Database.SQLITE_SCANSTAT_NLOOP = 0;
Database.SQLITE_SCANSTAT_NVISIT = 1;
Database.SQLITE_SCANSTAT_EST = 2;
Database.SQLITE_SCANSTAT_NAME = 3;
Database.SQLITE_SCANSTAT_EXPLAIN = 4;
Database.SQLITE_SCANSTAT_SELECTID = 5;
Database.SQLITE_SCANSTAT_PARENTID = 6;
Database.SQLITE_SCANSTAT_NCYCLE = 7;
Database.SQLITE_SCANSTAT_COMPLEX = 8;
module.exports = Database;
module.exports.SqliteError = SqliteError;
`)
    log('sqlite wasm shim ready')

    // step 10c: write migration runner script
    const { mkdirSync: mkdirOrez } = await import('node:fs')
    mkdirOrez(resolve(TEST_DIR, '.orez'), { recursive: true })
    const migrateScript = resolve(TEST_DIR, '.orez', 'run-migrations.sh')
    writeFileSync(migrateScript, `#!/bin/bash
set -e
echo "[on-db-ready] running migrations..."
echo "[on-db-ready] DATABASE_URL=$DATABASE_URL"
echo "[on-db-ready] ZERO_UPSTREAM_DB=$ZERO_UPSTREAM_DB"
export RUN=1
export ALLOW_MISSING_ENV=1
cd "${TEST_DIR}"
node src/database/dist/migrate.js
echo "[on-db-ready] migrations complete"
`)
    execSync(`chmod +x "${migrateScript}"`)

    // step 11a: start bunny-mock server
    // bunny-mock hardcodes PORT and STORAGE_DIR, so we create a patched copy
    log('starting bunny-mock server')
    const bunnyDataDir = resolve(TEST_DIR, '.orez', 'bunny-data')
    const { mkdirSync: mkdirBunny } = await import('node:fs')
    mkdirBunny(bunnyDataDir, { recursive: true })
    const bunnyServerSrc = readFileSync(resolve(TEST_DIR, 'src', 'bunny-mock', 'server.js'), 'utf-8')
    const bunnyServerPatched = bunnyServerSrc
      .replace(/const PORT = \d+/, `const PORT = ${PORTS.bunny}`)
      .replace(/const STORAGE_DIR = '\/data'/, `const STORAGE_DIR = '${bunnyDataDir}'`)
    const bunnyScriptPath = resolve(TEST_DIR, '.orez', 'bunny-mock-patched.mjs')
    writeFileSync(bunnyScriptPath, bunnyServerPatched)
    const bunnyProc = spawn('node', [bunnyScriptPath], {
      cwd: TEST_DIR,
      stdio: 'inherit',
    })
    children.push(bunnyProc)
    // wait for bunny to be ready
    await waitForPort(PORTS.bunny, 15_000, 'bunny-mock')
    log(`bunny-mock ready on port ${PORTS.bunny}`)

    // step 11b: start orez lite backend
    // invoke orez binary directly (bypassing bun run:dev → dotenvx chain
    // which mangles --on-db-ready argument through multiple shell layers)
    log('starting orez lite backend')

    // load .env.development vars so orez gets all the config it needs
    const envDevForOrez: Record<string, string> = {}
    const envDevOrezPath = resolve(TEST_DIR, '.env.development')
    if (existsSync(envDevOrezPath)) {
      for (const line of readFileSync(envDevOrezPath, 'utf-8').split('\n')) {
        const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/)
        if (m) {
          let val = m[2].trim()
          if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'")))
            val = val.slice(1, -1)
          envDevForOrez[m[1]] = val
        }
      }
    }

    const orezBin = resolve(TEST_DIR, 'node_modules', '.bin', 'orez')
    const backendProc = spawn(
      'node',
      [
        orezBin,
        `--pg-port=${PORTS.pg}`,
        `--zero-port=${PORTS.zero}`,
        '--s3',
        `--s3-port=${PORTS.s3}`,
        `--on-db-ready=${migrateScript}`,
        '--migrations=./no',
      ],
      {
        cwd: TEST_DIR,
        stdio: 'inherit',
        env: {
          ...process.env,
          ...envDevForOrez,
          NODE_ENV: 'development',
          ALLOW_MISSING_ENV: '1',
          ZERO_UPSTREAM_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/postgres`,
          ZERO_CVR_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cvr`,
          ZERO_CHANGE_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cdb`,
          ZERO_MUTATE_URL: `http://localhost:${PORTS.web}/api/zero/push`,
          ZERO_QUERY_URL: `http://localhost:${PORTS.web}/api/zero/pull`,
          VITE_PORT_WEB: String(PORTS.web),
          VITE_PORT_ZERO: String(PORTS.zero),
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
        DEBUG: '1',
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
