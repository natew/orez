#!/usr/bin/env bun
/**
 * run ~/chat e2e tests against local orez build.
 *
 * copies ~/chat to a temp dir (isolated from live edits), installs local
 * orez + bedrock-sqlite dist, then runs chat's playwright integration
 * tests in lite mode.
 *
 * usage:
 *   bun scripts/test-chat-e2e.ts                     # full build + test
 *   RETRY=1 bun scripts/test-chat-e2e.ts             # skip orez build + web rebuild
 *   bun scripts/test-chat-e2e.ts --filter=messaging   # filter tests
 *   bun scripts/test-chat-e2e.ts --single-db          # run lite backend with --single-db
 *
 * env:
 *   RETRY=1         skip orez build + web rebuild (iterative mode)
 *   FILTER=pattern  filter playwright tests
 *   SINGLE_DB=1     run lite backend with --single-db
 *   PORT_OFFSET=N   override port offset (default: 400)
 */

import { execFileSync, execSync } from 'node:child_process'
import {
  existsSync,
  cpSync,
  rmSync,
  mkdirSync,
  writeFileSync,
  readFileSync,
  symlinkSync,
} from 'node:fs'
import { resolve } from 'node:path'

const OREZ_ROOT = resolve(import.meta.dirname, '..')
const CHAT_SOURCE = resolve(process.env.HOME!, 'chat')
const SQLITE_WASM_DIR = resolve(OREZ_ROOT, 'sqlite-wasm')
const TEST_DIR = resolve(OREZ_ROOT, 'test-chat')

const retry = process.env.RETRY === '1'
const filterArg = process.argv.find((a) => a.startsWith('--filter='))
const filter = filterArg?.split('=')[1] || process.env.FILTER || ''
const singleDb = process.env.SINGLE_DB === '1' || process.argv.includes('--single-db')
// use 400 to avoid colliding with chat dev (0) or chat's own test offset (300)
const portOffset = process.env.PORT_OFFSET || '400'
const doBackendUrl = process.env.DO_BACKEND_URL || 'http://127.0.0.1:8799'

function log(msg: string) {
  console.log(`\n\x1b[1m\x1b[36m[chat-e2e]\x1b[0m ${msg}`)
}

function resolveNodeBinary() {
  const nativePackageDir = resolve(TEST_DIR, 'node_modules', '@rocicorp', 'zero-sqlite3')
  const cwd = existsSync(nativePackageDir) ? nativePackageDir : TEST_DIR
  const candidate = execSync('command -v node', { cwd, encoding: 'utf8' }).trim()
  if (candidate && existsSync(candidate)) return candidate
  const explicit = process.env.NODE
  if (explicit && existsSync(explicit)) return explicit
  throw new Error('could not resolve node binary for zero-cache')
}

function syncChatWorkingTree() {
  log('syncing working tree from ~/chat')
  for (const path of ['app', 'scripts', 'src']) {
    const src = resolve(CHAT_SOURCE, path)
    const dst = resolve(TEST_DIR, path)
    if (existsSync(src)) {
      execSync(`rsync -a --delete "${src}/" "${dst}/"`, { stdio: 'inherit' })
    }
  }
  // also re-sync root config files that affect dependency resolution and files
  // we later mutate. without this, a past RETRY=1 run's patches — e.g. the old
  // `retries: 0` override — survive across reruns and silently turn flaky
  // tests into hard fails in the next run.
  for (const file of ['package.json', 'bun.lock', 'playwright.config.ts']) {
    const src = resolve(CHAT_SOURCE, file)
    const dst = resolve(TEST_DIR, file)
    if (existsSync(src)) {
      cpSync(src, dst)
    }
  }
}

function enableSingleDbBackendScript() {
  if (!singleDb) return

  const packageJsonPath = resolve(TEST_DIR, 'package.json')
  const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf-8')) as {
    scripts?: Record<string, string>
  }
  const script = packageJson.scripts?.['lite:backend']
  if (typeof script !== 'string') {
    throw new Error('~/chat package.json is missing scripts.lite:backend')
  }
  if (/\borez\s+--single-db\b/.test(script) || /\s--single-db(\s|$)/.test(script)) {
    return
  }
  const nextScript = script.replace(/\borez\b/, 'orez --single-db')
  if (nextScript === script) {
    throw new Error('could not insert --single-db into scripts.lite:backend')
  }
  packageJson.scripts!['lite:backend'] = nextScript
  writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`)
  log('patched chat lite backend script with --single-db')
}

function allowColdDoBackendStartup() {
  const e2ePath = resolve(TEST_DIR, 'scripts', 'test', 'e2e.ts')
  if (!existsSync(e2ePath)) return
  const source = readFileSync(e2ePath, 'utf8')
  const coldLiteWaits = [
    'await waitForPort(ports.postgres, { timeoutMs: 60_000 })',
    'await waitForPort(ports.zero, { timeoutMs: 60_000 })',
  ]
  let next = source
  for (const wait of coldLiteWaits) {
    // Replace only the first occurrence: these are the lite-mode waits. Chat's
    // non-lite backend already uses a 120s Zero budget.
    next = next.replace(wait, wait.replace('60_000', '120_000'))
  }
  if (next === source) return
  writeFileSync(e2ePath, next)
  log('extended cold lite backend readiness budget to 120s')
}

// chat-native pins orez's sync-core/sync-native to a git rev. the whole point
// of this harness is exercising the LOCAL orez working tree, so patch the git
// source to the local crates (cargo rebuilds on the next backend start).
function useLocalOrezCrates() {
  const cargoPath = resolve(TEST_DIR, 'rust-sync', 'chat-native', 'Cargo.toml')
  if (!existsSync(cargoPath)) return
  const source = readFileSync(cargoPath, 'utf8')
  if (source.includes('[patch."https://github.com/natew/orez.git"]')) return
  const patch = [
    '',
    '[patch."https://github.com/natew/orez.git"]',
    `sync-core = { path = "${resolve(OREZ_ROOT, 'crates', 'sync-core')}" }`,
    `sync-native = { path = "${resolve(OREZ_ROOT, 'crates', 'sync-native')}" }`,
    '',
  ].join('\n')
  writeFileSync(cargoPath, source + patch)
  log('patched chat-native to build against local orez crates')
}

function enableAlwaysOnTrace() {
  if (process.env.TRACE !== '1') return
  const configPath = resolve(TEST_DIR, 'playwright.config.ts')
  const source = readFileSync(configPath, 'utf8')
  const next = source.replace("trace: 'on-first-retry'", "trace: 'on'")
  if (next === source) {
    throw new Error('could not patch playwright trace mode (config changed upstream?)')
  }
  writeFileSync(configPath, next)
  log('patched playwright config: trace always on (TRACE=1)')
}

function main() {
  // step 0: validate ~/chat exists
  if (!existsSync(CHAT_SOURCE)) {
    console.error('~/chat does not exist, skipping')
    process.exit(0)
  }

  if (!existsSync(resolve(CHAT_SOURCE, 'package.json'))) {
    console.error('~/chat/package.json not found')
    process.exit(1)
  }

  // step 1: build orez (unless retry)
  if (!retry) {
    log('building orez...')
    execSync('bun run build', { cwd: OREZ_ROOT, stdio: 'inherit' })
  } else {
    log('RETRY mode: skipping orez build')
    if (!existsSync(resolve(OREZ_ROOT, 'dist'))) {
      console.error('dist/ not found - run without RETRY first')
      process.exit(1)
    }
  }

  // step 2: clone/sync ~/chat into test-chat/ (isolated from live edits)
  if (!retry || !existsSync(TEST_DIR)) {
    const chatRef = process.env.CHAT_REF || ''
    log(`cloning ~/chat → test-chat/${chatRef ? ` (ref: ${chatRef})` : ''}`)
    if (existsSync(TEST_DIR)) {
      rmSync(TEST_DIR, { recursive: true, force: true })
    }
    if (chatRef) {
      // full clone to allow checking out a specific commit
      execSync(`git clone "${CHAT_SOURCE}" "${TEST_DIR}"`, {
        stdio: 'inherit',
      })
      execSync(`git checkout ${chatRef}`, {
        cwd: TEST_DIR,
        stdio: 'inherit',
      })
    } else {
      execSync(`git clone --depth 1 "${CHAT_SOURCE}" "${TEST_DIR}"`, {
        stdio: 'inherit',
      })
    }

    // copy .env (secrets only - .env.development is auto-generated by env.ts)
    const envSrc = resolve(CHAT_SOURCE, '.env')
    if (existsSync(envSrc)) {
      cpSync(envSrc, resolve(TEST_DIR, '.env'))
    }

    // local chat package changes must be present before install; local clones
    // copy HEAD, not uncommitted dependency upgrades from the working tree.
    syncChatWorkingTree()

    // install deps
    log('installing dependencies...')
    execSync('bun install --ignore-scripts', {
      cwd: TEST_DIR,
      stdio: 'inherit',
      timeout: 300_000,
    })
    // run essential postinstall
    try {
      execSync('bun one patch', { cwd: TEST_DIR, stdio: 'inherit', timeout: 30_000 })
    } catch {}
    // build native @rocicorp/zero-sqlite3 (prebuild-install downloads prebuilt binary)
    log('building native @rocicorp/zero-sqlite3...')
    const nodeBinary = resolveNodeBinary()
    execSync('npm run install', {
      cwd: resolve(TEST_DIR, 'node_modules', '@rocicorp', 'zero-sqlite3'),
      stdio: 'inherit',
      timeout: 120_000,
      env: {
        ...process.env,
        NODE: nodeBinary,
        OREZ_NODE: nodeBinary,
      },
    })
  } else {
    log('RETRY mode: reusing test-chat/')
  }

  // overlay the current ~/chat working tree so local uncommitted fixes and
  // instrumentation are present in test-chat on both fresh and retry runs.
  syncChatWorkingTree()
  enableSingleDbBackendScript()
  allowColdDoBackendStartup()
  useLocalOrezCrates()
  enableAlwaysOnTrace()

  // write .env.development with offset ports
  // vite needs these to override production hostnames in .env
  const offset = Number(portOffset)
  const webPort = 8081 + offset
  const zeroPort = 5048 + offset
  const pgPort = 5632 + offset
  const minioPort = 9290 + offset
  const bunnyPort = 3533 + offset
  const nodeBinary = resolveNodeBinary()
  const envDev = [
    `# generated by test-chat-e2e.ts (PORT_OFFSET=${offset})`,
    `NODE="${nodeBinary}"`,
    `OREZ_NODE="${nodeBinary}"`,
    `VITE_PROTOCOL="http"`,
    `VITE_PORT_WEB="${webPort}"`,
    `VITE_PORT_ZERO="${zeroPort}"`,
    `VITE_PORT_POSTGRES="${pgPort}"`,
    `VITE_PORT_MINIO="${minioPort}"`,
    `VITE_PORT_BUNNY="${bunnyPort}"`,
    `VITE_WEB_HOSTNAME="localhost:${webPort}"`,
    `VITE_ZERO_HOSTNAME="localhost:${zeroPort}"`,
    `BETTER_AUTH_SECRET="asjdbafbiasdjsadjasahjYVGHSVHFB"`,
    `BETTER_AUTH_URL="http://localhost:${webPort}"`,
    `ONE_SERVER_URL="http://localhost:${webPort}"`,
    `ZERO_UPSTREAM_DB="postgresql://user:password@127.0.0.1:${pgPort}/postgres"`,
    `ZERO_CVR_DB="postgresql://user:password@127.0.0.1:${pgPort}/zero_cvr"`,
    `ZERO_CHANGE_DB="postgresql://user:password@127.0.0.1:${pgPort}/zero_cdb"`,
    `ZERO_MUTATE_URL="http://127.0.0.1:${webPort}/api/zero/push"`,
    `ZERO_QUERY_URL="http://127.0.0.1:${webPort}/api/zero/pull"`,
    `DO_BACKEND_URL="${doBackendUrl}"`,
    `CLOUDFLARE_R2_ACCESS_KEY="minio"`,
    `CLOUDFLARE_R2_ENDPOINT="http://localhost:${minioPort}/chat"`,
    `CLOUDFLARE_R2_PUBLIC_URL="http://localhost:${minioPort}/chat"`,
    `CLOUDFLARE_R2_SECRET_KEY="minio_password"`,
    `ALLOW_MISSING_ENV="1"`,
    `DO_NOT_TRACK="1"`,
    `ZERO_NUM_SYNC_WORKERS="2"`,
  ].join('\n')
  log(`writing .env.development (PORT_OFFSET=${offset})`)
  writeFileSync(resolve(TEST_DIR, '.env.development'), envDev + '\n')

  // step 3: copy orez dist into test-chat/node_modules/orez
  log('installing local orez build')
  const orezDst = resolve(TEST_DIR, 'node_modules', 'orez')
  if (existsSync(orezDst)) {
    const orezDistDst = resolve(orezDst, 'dist')
    const orezSrcDst = resolve(orezDst, 'src')
    if (existsSync(orezDistDst)) rmSync(orezDistDst, { recursive: true, force: true })
    if (existsSync(orezSrcDst)) rmSync(orezSrcDst, { recursive: true, force: true })
  } else {
    mkdirSync(orezDst, { recursive: true })
  }
  cpSync(resolve(OREZ_ROOT, 'dist'), resolve(orezDst, 'dist'), { recursive: true })
  cpSync(resolve(OREZ_ROOT, 'package.json'), resolve(orezDst, 'package.json'))
  if (existsSync(resolve(OREZ_ROOT, 'src'))) {
    cpSync(resolve(OREZ_ROOT, 'src'), resolve(orezDst, 'src'), { recursive: true })
  }

  // step 4: copy bedrock-sqlite dist (skip if wasm dist not built — wasm is disabled anyway)
  const sqliteWasmDist = resolve(SQLITE_WASM_DIR, 'dist')
  if (existsSync(sqliteWasmDist)) {
    log('installing local bedrock-sqlite build')
    const sqliteDst = resolve(TEST_DIR, 'node_modules', 'bedrock-sqlite')
    if (existsSync(sqliteDst)) {
      const sqliteDistDst = resolve(sqliteDst, 'dist')
      if (existsSync(sqliteDistDst))
        rmSync(sqliteDistDst, { recursive: true, force: true })
    } else {
      mkdirSync(sqliteDst, { recursive: true })
    }
    cpSync(sqliteWasmDist, resolve(sqliteDst, 'dist'), { recursive: true })
    cpSync(resolve(SQLITE_WASM_DIR, 'package.json'), resolve(sqliteDst, 'package.json'))
    if (existsSync(resolve(SQLITE_WASM_DIR, 'bedrock-sqlite.d.ts'))) {
      cpSync(
        resolve(SQLITE_WASM_DIR, 'bedrock-sqlite.d.ts'),
        resolve(sqliteDst, 'bedrock-sqlite.d.ts')
      )
    }
  } else {
    log('skipping bedrock-sqlite (no wasm dist built, wasm is disabled)')
  }

  // ensure node_modules/.bin/orez points at our freshly copied dist. `bun install`
  // only writes this symlink on install; copying just the dist/ after install
  // leaves a stale or missing link that breaks `bun run:dev orez` in lite:backend.
  const binDir = resolve(TEST_DIR, 'node_modules', '.bin')
  const orezBinLink = resolve(binDir, 'orez')
  if (existsSync(orezBinLink)) rmSync(orezBinLink)
  symlinkSync(resolve(orezDst, 'dist', 'cli-entry.js'), orezBinLink)

  // step 5: wipe .orez data so the boot uses a clean pglite dataset.
  // with PORT_OFFSET, @take-out/env sets OREZ_DATA_DIR=/tmp/orez-{offset}, so
  // clear both candidate locations.
  for (const dir of [resolve(TEST_DIR, '.orez'), `/tmp/orez-${portOffset}`]) {
    if (existsSync(dir)) {
      log(`cleaning ${dir}`)
      rmSync(dir, { recursive: true, force: true })
    }
  }

  // NOTE on what we intentionally do NOT touch unless explicitly requested:
  //   * package.json scripts — the default harness keeps chat's `lite:backend`
  //     identical to ~/chat. `--single-db`/SINGLE_DB=1 is the one explicit
  //     exception, used to validate the orez CLI single-db backend path.
  //   * playwright.config.ts — maxFailures/retries etc. stay at whatever chat
  //     ships. past overrides here (retries: 0, maxFailures: 0) masked real
  //     chat-vs-test-chat divergences as "flakes" or ate the wall-clock budget.
  //   * test-case timeouts — chat wraps every `test.setTimeout(N)` in `t(N)`,
  //     which already scales in lite mode via E2E_LITE=1. the raw-number regex
  //     we used to run didn't match `t(...)` calls anyway, so it was a no-op
  //     the whole time. The cold backend readiness budget above is deliberately
  //     separate: a fresh DO migration plus zero-cache bootstrap can exceed 60s.

  log('local packages installed')

  // verify we're using local orez, not global
  const localOrezVersion = JSON.parse(
    readFileSync(resolve(orezDst, 'package.json'), 'utf-8')
  ).version
  log(`orez version: ${localOrezVersion} (local build)`)

  // kill any stale processes from previous runs.
  // chat's `clear-ports` script authoritative-ly enumerates web/zero/postgres/
  // minio/agent-gateway from its env config, so delegate to it instead of
  // hardcoding a list here that drifts when chat adds/renames ports.
  // then also kill orez's zero internal port (zeroPort + 1000 — used as the
  // change-streamer listener when admin is enabled). it's not in chat's `ports`
  // map and was the source of past EADDRINUSE hangs on RETRY=1 reruns.
  try {
    execSync('bun run clear-ports', {
      cwd: TEST_DIR,
      stdio: 'ignore',
      env: { ...process.env, PORT_OFFSET: portOffset },
    })
  } catch {}
  const zeroInternalPort = 5048 + offset + 1000
  try {
    execSync(`lsof -ti:${zeroInternalPort} | xargs kill -9 2>/dev/null`, {
      stdio: 'ignore',
    })
  } catch {}

  // step 8: run chat e2e tests (chat removed its lite mode; the plain
  // integration run is the mode)
  const testCmd = ['bun', 'run', 'test', 'e2e', '--integration']
  if (retry) testCmd.push('--retry')
  if (filter) testCmd.push('--filter', filter)

  log(`running: ${testCmd.map((arg) => JSON.stringify(arg)).join(' ')}`)
  log(
    `PORT_OFFSET=${portOffset}${singleDb ? ' SINGLE_DB=1' : ''} DO_BACKEND_URL=${doBackendUrl}`
  )

  // prepend node_modules/.bin to PATH so local orez is used instead of global
  const localPath = `${binDir}:${process.env.PATH}`

  try {
    execFileSync(testCmd[0], testCmd.slice(1), {
      cwd: TEST_DIR,
      stdio: 'inherit',
      timeout: 1_200_000,
      env: {
        ...process.env,
        PATH: localPath,
        NODE: nodeBinary,
        OREZ_NODE: nodeBinary,
        PORT_OFFSET: portOffset,
        DO_BACKEND_URL: doBackendUrl,
        NODE_ENV: 'development',
        ALLOW_MISSING_ENV: '1',
      },
    })
    log('TESTS PASSED')
  } catch (err: any) {
    log('TESTS FAILED')
    process.exit(err.status || 1)
  }
}

main()
