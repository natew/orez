#!/usr/bin/env bun

/**
 * release script: check, build, publish both orez + bedrock-sqlite, commit, tag, push.
 */

import { execSync } from 'node:child_process'
import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

const args = process.argv.slice(2)
const dryRun = args.includes('--dry-run')
const patch = args.includes('--patch')
const minor = args.includes('--minor')
const major = args.includes('--major')

if (!patch && !minor && !major) {
  console.info('usage: bun scripts/release.ts --patch|--minor|--major [--dry-run]')
  process.exit(1)
}

const root = resolve(import.meta.dirname, '..')

function run(cmd: string, opts?: { silent?: boolean; cwd?: string }) {
  const cwd = opts?.cwd ?? root
  if (!opts?.silent) console.info(`$ ${cmd}`)
  return execSync(cmd, { stdio: opts?.silent ? 'pipe' : 'inherit', cwd })
}

function runText(cmd: string, cwd?: string): string {
  return execSync(cmd, { cwd: cwd ?? root })
    .toString()
    .trim()
}

function bumpVersion(current: string): string {
  const [curMajor, curMinor, curPatch] = current.split('.').map(Number)
  return major
    ? `${curMajor + 1}.0.0`
    : minor
      ? `${curMajor}.${curMinor + 1}.0`
      : `${curMajor}.${curMinor}.${curPatch + 1}`
}

// read package versions
const orezPkgPath = resolve(root, 'package.json')
const orezPkg = JSON.parse(readFileSync(orezPkgPath, 'utf-8'))
const orezNext = bumpVersion(orezPkg.version)

const sqliteWasmDir = resolve(root, 'sqlite-wasm')
const sqlitePkgPath = resolve(sqliteWasmDir, 'package.json')
const hasSqliteWasm = existsSync(sqlitePkgPath)

let sqlitePkg: any
let sqliteNext: string | undefined
if (hasSqliteWasm) {
  sqlitePkg = JSON.parse(readFileSync(sqlitePkgPath, 'utf-8'))
  sqliteNext = bumpVersion(sqlitePkg.version)
}

console.info(`\nreleasing orez: ${orezPkg.version} -> ${orezNext}`)
if (hasSqliteWasm) {
  console.info(`releasing bedrock-sqlite: ${sqlitePkg.version} -> ${sqliteNext}`)
}

// check: lint, types, tests
console.info('\nchecking orez...')
run('bun run lint')
run('bun run check')
run('bun run test')

if (hasSqliteWasm) {
  console.info('\nchecking bedrock-sqlite...')
  run('npx vitest run', { cwd: sqliteWasmDir })
}

// build orez
console.info('\nbuilding orez...')
run('bun run build')

// bump versions (keep in sync)
orezPkg.version = orezNext
if (hasSqliteWasm && sqliteNext) {
  // keep orez's dep on bedrock-sqlite pinned to the matching version
  if (orezPkg.dependencies?.['bedrock-sqlite']) {
    orezPkg.dependencies['bedrock-sqlite'] = sqliteNext
  }
  sqlitePkg.version = sqliteNext
  writeFileSync(sqlitePkgPath, JSON.stringify(sqlitePkg, null, 2) + '\n')
}
writeFileSync(orezPkgPath, JSON.stringify(orezPkg, null, 2) + '\n')

if (dryRun) {
  console.info(`\n[dry-run] would publish orez@${orezNext}`)
  if (hasSqliteWasm) console.info(`[dry-run] would publish bedrock-sqlite@${sqliteNext}`)

  // revert versions
  orezPkg.version = orezPkg.version.split('.').map(Number).join('.') // no-op but safe
  const [m, mi, p] = orezNext.split('.').map(Number)
  orezPkg.version = major
    ? `${m - 1}.${0}.${0}`
    : minor
      ? `${m}.${mi - 1}.${0}`
      : `${m}.${mi}.${p - 1}`
  writeFileSync(orezPkgPath, JSON.stringify(orezPkg, null, 2) + '\n')

  if (hasSqliteWasm && sqlitePkg) {
    const [sm, smi, sp] = sqliteNext!.split('.').map(Number)
    sqlitePkg.version = major
      ? `${sm - 1}.${0}.${0}`
      : minor
        ? `${sm}.${smi - 1}.${0}`
        : `${sm}.${smi}.${sp - 1}`
    writeFileSync(sqlitePkgPath, JSON.stringify(sqlitePkg, null, 2) + '\n')
  }
  process.exit(0)
}

// publish
console.info('\npublishing orez...')
run('npm publish')

if (hasSqliteWasm) {
  console.info('\npublishing bedrock-sqlite...')
  run('npm publish', { cwd: sqliteWasmDir })
}

// git commit + tag + push
const tag = `v${orezNext}`
run('git add -A')
run(`git commit -m "${tag}"`)
run(`git tag ${tag}`)
run('git push origin HEAD')
run(`git push origin ${tag}`)

console.info(`\nreleased orez@${orezNext}`)
if (hasSqliteWasm) console.info(`released bedrock-sqlite@${sqliteNext}`)
