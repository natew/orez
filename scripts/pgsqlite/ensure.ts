#!/usr/bin/env bun
/**
 * Ensure a pgsqlite binary is available for oracle-based testing.
 *
 * Strategy (in order):
 *   1. If $PGSQLITE_BIN is set and points to an executable file, use it.
 *   2. If `pgsqlite` is on PATH, use that.
 *   3. If a vendored binary exists at `vendor/pgsqlite/<platform>/pgsqlite`, use it.
 *   4. If a checkout exists at `~/github/pgsqlite/target/release/pgsqlite`, use it.
 *   5. If cargo is available, build from a fresh shallow clone into
 *      `vendor/pgsqlite/build` and use that.
 *   6. Otherwise: write a marker file noting "no pgsqlite" so test runners
 *      can skip oracle-dependent tests gracefully.
 *
 * Output: writes the resolved binary path (or empty) to
 * `vendor/pgsqlite/.resolved-path` for test runners to read.
 *
 * Exit 0 either way — oracle tests are advisory, never blocking.
 */
import { execSync, spawnSync } from 'node:child_process'
import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { resolve } from 'node:path'

const REPO_ROOT = resolve(import.meta.dirname, '..', '..')
const VENDOR_DIR = resolve(REPO_ROOT, 'vendor', 'pgsqlite')
const RESOLVED_PATH_FILE = resolve(VENDOR_DIR, '.resolved-path')
const PGSQLITE_REPO = 'https://github.com/erans/pgsqlite'
const PGSQLITE_PIN = 'v0.0.22'

function log(msg: string) {
  console.log(`[pgsqlite-ensure] ${msg}`)
}

function isExecutable(path: string | undefined): path is string {
  if (!path) return false
  try {
    const stats = require('node:fs').statSync(path)
    return stats.isFile() && (stats.mode & 0o111) !== 0
  } catch {
    return false
  }
}

function whichPgsqlite(): string | undefined {
  try {
    const out = execSync('command -v pgsqlite', { encoding: 'utf8' }).trim()
    return out || undefined
  } catch {
    return undefined
  }
}

function writeResolved(path: string) {
  mkdirSync(VENDOR_DIR, { recursive: true })
  writeFileSync(RESOLVED_PATH_FILE, path)
  log(`resolved → ${path || '<none>'}`)
}

async function main(): Promise<void> {
  // 1. env override
  if (isExecutable(process.env.PGSQLITE_BIN)) {
    writeResolved(process.env.PGSQLITE_BIN!)
    return
  }
  // 2. PATH
  const onPath = whichPgsqlite()
  if (isExecutable(onPath)) {
    writeResolved(onPath)
    return
  }
  // 3. vendored prebuilt
  const platformBin = resolve(
    VENDOR_DIR,
    process.platform === 'darwin'
      ? process.arch === 'arm64'
        ? 'darwin-arm64'
        : 'darwin-x64'
      : process.platform === 'linux'
        ? process.arch === 'arm64'
          ? 'linux-arm64'
          : 'linux-x64'
        : 'unknown',
    'pgsqlite'
  )
  if (isExecutable(platformBin)) {
    writeResolved(platformBin)
    return
  }
  // 4. local dev checkout
  const devCheckout = resolve(
    homedir(),
    'github',
    'pgsqlite',
    'target',
    'release',
    'pgsqlite'
  )
  if (isExecutable(devCheckout)) {
    writeResolved(devCheckout)
    return
  }
  // 5. build from source if cargo is available
  const hasCargo = spawnSync('cargo', ['--version'], { stdio: 'ignore' }).status === 0
  if (hasCargo) {
    const buildDir = resolve(VENDOR_DIR, 'build')
    if (!existsSync(buildDir)) {
      log(`cloning ${PGSQLITE_REPO}@${PGSQLITE_PIN} → ${buildDir}`)
      execSync(
        `git clone --depth 1 --branch ${PGSQLITE_PIN} ${PGSQLITE_REPO} "${buildDir}"`,
        { stdio: 'inherit' }
      )
    }
    log('building pgsqlite (cargo build --release, may take 5+ min)')
    execSync('cargo build --release', { cwd: buildDir, stdio: 'inherit' })
    const built = resolve(buildDir, 'target', 'release', 'pgsqlite')
    if (isExecutable(built)) {
      writeResolved(built)
      return
    }
  }
  // 6. no oracle available
  log('no pgsqlite binary available — oracle tests will be skipped')
  writeResolved('')
}

main().catch((err) => {
  log(`failed: ${err.message}`)
  // still write empty so consumers don't hang
  try {
    writeResolved('')
  } catch {}
  process.exit(0)
})
