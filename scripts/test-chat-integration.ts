#!/usr/bin/env bun
/**
 * integration test runner: clones ~/chat into test-chat/, links local orez,
 * and runs chat's playwright e2e tests against the local orez backend.
 *
 * usage: bun scripts/test-chat-integration.ts [--skip-clone] [--filter=pattern] [--smoke]
 *
 * uses fixed ports (pg=5499 zero=4888 web=8099 s3=9399 bunny=3599)
 * well away from default chat dev ports. patches all port refs + env files.
 */

import { spawn, execSync, type ChildProcess } from 'node:child_process'
import { existsSync, rmSync, readFileSync, writeFileSync } from 'node:fs'
import { createConnection } from 'node:net'
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
    sock.on('connect', () => {
      sock.destroy()
      resolve(true)
    })
    sock.on('error', () => resolve(false))
    sock.setTimeout(300, () => {
      sock.destroy()
      resolve(false)
    })
  })
}

async function main() {
  // ensure correct node version for chat (needs crypto.hash from node 22+)
  const chatNodeVersion = (() => {
    try {
      const pkg = JSON.parse(readFileSync(resolve(CHAT_SOURCE, 'package.json'), 'utf-8'))
      return pkg.engines?.node
    } catch {
      return null
    }
  })()
  if (chatNodeVersion) {
    try {
      // find fnm dir (may contain spaces, e.g. "Application Support")
      const fnmDirMatch = execSync('fnm env --shell bash', { encoding: 'utf-8' }).match(
        /FNM_DIR="([^"]+)"/
      )
      const fnmBase =
        fnmDirMatch?.[1] ||
        resolve(process.env.HOME!, 'Library', 'Application Support', 'fnm')
      const versionDir = resolve(
        fnmBase,
        'node-versions',
        `v${chatNodeVersion}`,
        'installation',
        'bin'
      )
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

  // fixed ports — well away from default chat dev ports (8081, 5432, 5048, etc.)
  const PORTS = {
    pg: 5499,
    zero: 4888,
    web: 8099,
    s3: 9399,
    bunny: 3599,
  }
  log(
    `ports: pg=${PORTS.pg} zero=${PORTS.zero} web=${PORTS.web} s3=${PORTS.s3} bunny=${PORTS.bunny}`
  )
  // bail if any port is already in use
  for (const [name, port] of Object.entries(PORTS)) {
    if (await isPortInUse(port)) {
      throw new Error(
        `port ${port} (${name}) is already in use — kill the process and retry`
      )
    }
  }

  try {
    // unlock env files from previous runs (they're made read-only to prevent vite restarts)
    for (const f of ['.env', '.env.development']) {
      const p = resolve(TEST_DIR, f)
      if (existsSync(p))
        try {
          execSync(`chmod 644 "${p}"`)
        } catch {}
    }

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
      for (const dir of [
        'src/database',
        'src/data',
        'src/server',
        'src/apps',
        'src/constants',
      ]) {
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
      execSync('bun tko run generate-env', {
        cwd: TEST_DIR,
        stdio: 'inherit',
        timeout: 30_000,
      })
    } catch {}

    // step 4: copy local orez build into node_modules
    log('installing local orez build')
    const orezInModules = resolve(TEST_DIR, 'node_modules', 'orez')
    if (existsSync(orezInModules)) {
      rmSync(orezInModules, { recursive: true, force: true })
    }
    const { mkdirSync: mkdir, cpSync } = await import('node:fs')
    mkdir(orezInModules, { recursive: true })
    cpSync(resolve(OREZ_ROOT, 'dist'), resolve(orezInModules, 'dist'), {
      recursive: true,
    })
    cpSync(resolve(OREZ_ROOT, 'package.json'), resolve(orezInModules, 'package.json'))
    if (existsSync(resolve(OREZ_ROOT, 'src'))) {
      cpSync(resolve(OREZ_ROOT, 'src'), resolve(orezInModules, 'src'), {
        recursive: true,
      })
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

    // step 6: env setup — patch BOTH .env and .env.development with test ports
    // dotenvx loads: .env first, then .env.development with --overload
    // both files must exist and have correct port values
    const sourceEnv = resolve(CHAT_SOURCE, '.env')
    const envDevPath = resolve(TEST_DIR, '.env.development')
    const envPath = resolve(TEST_DIR, '.env')

    // helper: patch all port references in an env file string
    function patchEnvPorts(content: string): string {
      return content
        .replace(/VITE_PORT_WEB=\d+/, `VITE_PORT_WEB=${PORTS.web}`)
        .replace(/VITE_PORT_ZERO=\d+/, `VITE_PORT_ZERO=${PORTS.zero}`)
        .replace(/VITE_PORT_POSTGRES=\d+/, `VITE_PORT_POSTGRES=${PORTS.pg}`)
        .replace(/VITE_PORT_MINIO=\d+/, `VITE_PORT_MINIO=${PORTS.s3}`)
        .replace(/(ZERO_UPSTREAM_DB=.*(?:127\.0\.0\.1|localhost):)\d+/g, `$1${PORTS.pg}`)
        .replace(/(ZERO_CVR_DB=.*(?:127\.0\.0\.1|localhost):)\d+/g, `$1${PORTS.pg}`)
        .replace(/(ZERO_CHANGE_DB=.*(?:127\.0\.0\.1|localhost):)\d+/g, `$1${PORTS.pg}`)
        .replace(/(CLOUDFLARE_R2_ENDPOINT=.*localhost:)\d+/g, `$1${PORTS.s3}`)
        .replace(/(CLOUDFLARE_R2_PUBLIC_URL=.*localhost:)\d+/g, `$1${PORTS.s3}`)
        .replace(/VITE_ZERO_HOSTNAME=\S+/, `VITE_ZERO_HOSTNAME=localhost:${PORTS.zero}`)
        .replace(/VITE_WEB_HOSTNAME=\S+/, `VITE_WEB_HOSTNAME=localhost:${PORTS.web}`)
        .replace(/(BETTER_AUTH_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
        .replace(/(ONE_SERVER_URL=.*localhost:)\d+/g, `$1${PORTS.web}`)
        .replace(/host\.docker\.internal/g, 'localhost')
        .replace(
          /ZERO_MUTATE_URL=.+/,
          `ZERO_MUTATE_URL=http://localhost:${PORTS.web}/api/zero/push`
        )
        .replace(
          /ZERO_QUERY_URL=.+/,
          `ZERO_QUERY_URL=http://localhost:${PORTS.web}/api/zero/pull`
        )
    }

    if (existsSync(sourceEnv) && existsSync(envDevPath)) {
      // patch .env — replace production hostnames with localhost:port
      let envContent = readFileSync(envPath, 'utf-8')
      envContent = envContent
        .replace(/VITE_ZERO_HOSTNAME=\S+/, `VITE_ZERO_HOSTNAME=localhost:${PORTS.zero}`)
        .replace(/VITE_WEB_HOSTNAME=\S+/, `VITE_WEB_HOSTNAME=localhost:${PORTS.web}`)
        .replace(/VITE_PRODUCTION_HOSTNAME=\S+/, `VITE_PRODUCTION_HOSTNAME=localhost`)
      envContent = patchEnvPorts(envContent)
      writeFileSync(envPath, envContent)

      // patch .env.development — merge secrets from .env, update ports
      let envDev = readFileSync(envDevPath, 'utf-8')
      const devKeys = new Set(envDev.match(/^[A-Za-z_][A-Za-z0-9_]*/gm) || [])
      const secrets = readFileSync(sourceEnv, 'utf-8')
      for (const line of secrets.split('\n')) {
        const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/)
        if (match && !devKeys.has(match[1])) {
          envDev += `\n${line}`
        }
      }
      envDev = patchEnvPorts(envDev)
      writeFileSync(envDevPath, envDev)

      log('patched .env + .env.development with test ports')
      // make both read-only so nothing modifies at runtime (vite restarts on .env changes)
      execSync(`chmod 444 "${envPath}" "${envDevPath}"`)
    }

    // step 7: patch all ~/chat default ports → our fixed test ports
    // covers: URL literals, string fallbacks, env defaults, PORT constants
    log('patching ports to test values')
    execSync(
      `find src playwright.config.ts -type f \\( -name "*.ts" -o -name "*.tsx" -o -name "*.js" -o -name "*.json" \\) ` +
        `-not -path '*/node_modules/*' -not -path '*/.orez/*' -not -path '*/uncloud/*' -not -path '*/tauri/*' -exec sed -i '' -E ` +
        `-e 's/localhost:8081/localhost:${PORTS.web}/g' ` +
        `-e 's/localhost:5048/localhost:${PORTS.zero}/g' ` +
        `-e 's/localhost:5632/localhost:${PORTS.pg}/g' ` +
        `-e 's/localhost:3533/localhost:${PORTS.bunny}/g' ` +
        `-e 's/localhost:9290/localhost:${PORTS.s3}/g' ` +
        `-e 's/127\\.0\\.0\\.1:5632/127.0.0.1:${PORTS.pg}/g' ` +
        `-e "s/'5048'/'${PORTS.zero}'/g" ` +
        `-e "s/'8081'/'${PORTS.web}'/g" ` +
        `-e "s/'5632'/'${PORTS.pg}'/g" ` +
        `-e 's/const PORT = 8081/const PORT = ${PORTS.web}/g' ` +
        `{} +`,
      { cwd: TEST_DIR, stdio: 'inherit' }
    )

    // remove skipped test files with TDZ errors that crash playwright collection
    const brokenTests = ['src/integration/e2e/scroll-animation-audit.spec.ts']
    for (const f of brokenTests) {
      const p = resolve(TEST_DIR, f)
      if (existsSync(p)) rmSync(p)
    }

    // step 8: clean all caches (stale compiled modules with old ports)
    for (const cache of [
      'node_modules/.vite',
      'node_modules/.vxrn',
      'node_modules/.cache',
    ]) {
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
    const bedrockPath = resolve(
      TEST_DIR,
      'node_modules',
      'bedrock-sqlite',
      'dist',
      'sqlite3.js'
    )
    if (existsSync(zeroSqlitePkg)) {
      rmSync(zeroSqlitePkg, { recursive: true, force: true })
    }
    const { mkdirSync: mkdirShim } = await import('node:fs')
    mkdirShim(resolve(zeroSqlitePkg, 'lib'), { recursive: true })
    writeFileSync(
      resolve(zeroSqlitePkg, 'package.json'),
      JSON.stringify({
        name: '@rocicorp/zero-sqlite3',
        version: '0.0.0-shim',
        main: './lib/index.js',
      })
    )
    writeFileSync(
      resolve(zeroSqlitePkg, 'lib', 'index.js'),
      `'use strict';
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
// wrap pragma to skip optimize (corrupts wasm vfs) and swallow sqlite errors
var origPragma = OrigDatabase.prototype.pragma;
Database.prototype.pragma = function(str, opts) {
  if (str && str.trim().toLowerCase().startsWith('optimize')) return [];
  try { return origPragma.call(this, str, opts); }
  catch(e) { if (e && (e.code === 'SQLITE_CORRUPT' || e.code === 'SQLITE_IOERR')) return []; throw e; }
};
// wrap close to swallow wasm errors during shutdown
var origClose = OrigDatabase.prototype.close;
Database.prototype.close = function() {
  try { return origClose.call(this); }
  catch(e) { console.error('[orez-shim] close error (swallowed):', e && e.message || e); }
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
// wrap statement methods to handle SQLITE_IOERR from wasm vfs
var origAll = SP.all;
SP.all = function() {
  try { return origAll.apply(this, arguments); }
  catch(e) {
    if (e && e.code === 'SQLITE_IOERR') {
      console.error('[orez-shim] SQLITE_IOERR in .all() (swallowed):', (this.source || '').slice(0, 80));
      return [];
    }
    throw e;
  }
};
var origGet = SP.get;
SP.get = function() {
  try { return origGet.apply(this, arguments); }
  catch(e) {
    if (e && e.code === 'SQLITE_IOERR') {
      console.error('[orez-shim] SQLITE_IOERR in .get() (swallowed):', (this.source || '').slice(0, 80));
      return undefined;
    }
    throw e;
  }
};
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
`
    )
    log('sqlite wasm shim ready')

    // step 10c: write migration runner script
    const { mkdirSync: mkdirOrez } = await import('node:fs')
    mkdirOrez(resolve(TEST_DIR, '.orez'), { recursive: true })
    const migrateScript = resolve(TEST_DIR, '.orez', 'run-migrations.sh')
    writeFileSync(
      migrateScript,
      `#!/bin/bash
set -e
echo "[on-db-ready] running migrations..."
echo "[on-db-ready] DATABASE_URL=$DATABASE_URL"
echo "[on-db-ready] ZERO_UPSTREAM_DB=$ZERO_UPSTREAM_DB"
export RUN=1
export ALLOW_MISSING_ENV=1
cd "${TEST_DIR}"
node src/database/dist/migrate.js
echo "[on-db-ready] migrations complete"
`
    )
    execSync(`chmod +x "${migrateScript}"`)

    // step 11a: start bunny-mock server
    // bunny-mock hardcodes PORT and STORAGE_DIR, so we create a patched copy
    log('starting bunny-mock server')
    const bunnyDataDir = resolve(TEST_DIR, '.orez', 'bunny-data')
    const { mkdirSync: mkdirBunny } = await import('node:fs')
    mkdirBunny(bunnyDataDir, { recursive: true })
    const bunnyServerSrc = readFileSync(
      resolve(TEST_DIR, 'src', 'bunny-mock', 'server.js'),
      'utf-8'
    )
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
          if (
            (val.startsWith('"') && val.endsWith('"')) ||
            (val.startsWith("'") && val.endsWith("'"))
          )
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
      }
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
    const webProc = spawn(
      'bun',
      ['run:dev', 'one', 'dev', '--clean', '--port', String(PORTS.web)],
      {
        cwd: TEST_DIR,
        stdio: 'inherit',
        env: {
          ...process.env,
          ...envDevForOrez,
          NODE_ENV: 'development',
          ALLOW_MISSING_ENV: '1',
          DEBUG: '1',
          VITE_PORT_WEB: String(PORTS.web),
          VITE_PORT_ZERO: String(PORTS.zero),
          VITE_PORT_POSTGRES: String(PORTS.pg),
          VITE_PORT_MINIO: String(PORTS.s3),
          VITE_ZERO_HOSTNAME: `localhost:${PORTS.zero}`,
          VITE_WEB_HOSTNAME: `localhost:${PORTS.web}`,
          VITE_PUBLIC_ZERO_SERVER: `http://localhost:${PORTS.zero}`,
          BETTER_AUTH_URL: `http://localhost:${PORTS.web}`,
          ONE_SERVER_URL: `http://localhost:${PORTS.web}`,
          ZERO_UPSTREAM_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/postgres`,
          ZERO_CVR_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cvr`,
          ZERO_CHANGE_DB: `postgresql://user:password@127.0.0.1:${PORTS.pg}/zero_cdb`,
          ZERO_MUTATE_URL: `http://localhost:${PORTS.web}/api/zero/push`,
          ZERO_QUERY_URL: `http://localhost:${PORTS.web}/api/zero/pull`,
          DATABASE_URL: `postgresql://user:password@127.0.0.1:${PORTS.pg}/postgres`,
        },
      }
    )
    children.push(webProc)

    webProc.on('exit', (code) => {
      if (code && code !== 0) {
        console.error(`web server exited with code ${code}`)
      }
    })

    log('waiting for web server...')
    await waitForPort(PORTS.web, 120_000, 'web')
    log('web server ready')

    // step 12b: smoke-test the zero push endpoint
    log('testing zero push endpoint...')
    try {
      const pushUrl = `http://localhost:${PORTS.web}/api/zero/push`
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), 10_000)
      const pushRes = await fetch(pushUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pushVersion: 1,
          schemaVersion: 1,
          clientGroupID: 'smoke-test',
          mutations: [],
          timestamp: Date.now(),
        }),
        signal: controller.signal,
      })
      clearTimeout(timeout)
      const pushBody = await pushRes.text()
      log(`push endpoint responded: ${pushRes.status} ${pushBody.slice(0, 200)}`)
    } catch (err: any) {
      log(`push endpoint FAILED: ${err.message}`)
    }

    // step 12c: warm up vite SSR modules and let HMR settle
    // vite regenerates env files and tamagui config after startup, causing HMR
    // cascades that disconnect zero clients. pre-trigger all module loading
    // by hitting key endpoints, then wait for vite to stabilize.
    log('warming up vite SSR modules...')
    const warmupUrls = [
      `http://localhost:${PORTS.web}/`,
      `http://localhost:${PORTS.web}/auth/login`,
      `http://localhost:${PORTS.web}/api/auth/get-session`,
    ]
    for (const url of warmupUrls) {
      try {
        await fetch(url, { signal: AbortSignal.timeout(15_000) })
      } catch {}
    }
    // wait for HMR to settle (tamagui config build + env regeneration)
    log('waiting for vite to settle...')
    await new Promise((r) => setTimeout(r, 15_000))
    // verify server still responds after settling
    try {
      const check = await fetch(`http://localhost:${PORTS.web}/`, {
        signal: AbortSignal.timeout(10_000),
      })
      log(`post-warmup check: ${check.status}`)
    } catch (err: any) {
      log(`post-warmup check failed: ${err.message} (continuing anyway)`)
    }

    // step 13: run playwright tests
    log('running playwright tests')
    const testArgs = ['playwright', 'test']
    if (filter) {
      testArgs.push(filter)
      testArgs.push('--project=chromium')
    } else if (smokeOnly) {
      // smoke test has its own login flow, skip global setup dependency
      testArgs.push('src/integration/e2e/orez-smoke.test.ts')
      testArgs.push('--project=chromium', '--no-deps')
    } else {
      testArgs.push('--project=chromium')
    }

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
          if (
            (val.startsWith('"') && val.endsWith('"')) ||
            (val.startsWith("'") && val.endsWith("'"))
          ) {
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
  // restore env files to writable for future runs
  try {
    for (const f of ['.env', '.env.development']) {
      const p = resolve(TEST_DIR, f)
      if (existsSync(p)) execSync(`chmod 644 "${p}"`)
    }
  } catch {}
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
  throw new Error(
    `${name} (port ${port}) not ready after ${Math.round(timeoutMs / 1000)}s`
  )
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
