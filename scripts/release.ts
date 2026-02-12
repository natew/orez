#!/usr/bin/env bun

/**
 * release script: check, build, publish both orez + bedrock-sqlite, commit, tag, push.
 * uses workspace:* protocol — at publish time we copy to tmp and replace with real versions.
 */

import { execSync } from 'node:child_process'
import { cpSync, existsSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve, join } from 'node:path'

const args = process.argv.slice(2)
const dryRun = args.includes('--dry-run')
const patch = args.includes('--patch')
const minor = args.includes('--minor')
const major = args.includes('--major')
const skipTest = args.includes('--skip-test')

if (!patch && !minor && !major) {
  console.info(
    'usage: bun scripts/release.ts --patch|--minor|--major [--dry-run] [--skip-test]'
  )
  process.exit(1)
}

const root = resolve(import.meta.dirname, '..')

function run(cmd: string, opts?: { silent?: boolean; cwd?: string }) {
  const cwd = opts?.cwd ?? root
  if (!opts?.silent) console.info(`$ ${cmd}`)
  return execSync(cmd, { stdio: opts?.silent ? 'pipe' : 'inherit', cwd })
}

function bumpVersion(current: string): string {
  const [curMajor, curMinor, curPatch] = current.split('.').map(Number)
  return major
    ? `${curMajor + 1}.0.0`
    : minor
      ? `${curMajor}.${curMinor + 1}.0`
      : `${curMajor}.${curMinor}.${curPatch + 1}`
}

// workspace packages: [dir, pkgPath, pkg, nextVersion]
interface WorkspacePkg {
  dir: string
  pkgPath: string
  pkg: any
  next: string
}

const packages: WorkspacePkg[] = []

// orez (root)
const orezPkgPath = resolve(root, 'package.json')
const orezPkg = JSON.parse(readFileSync(orezPkgPath, 'utf-8'))
const orezNext = bumpVersion(orezPkg.version)
packages.push({ dir: root, pkgPath: orezPkgPath, pkg: orezPkg, next: orezNext })

// bedrock-sqlite (workspace)
const sqliteWasmDir = resolve(root, 'sqlite-wasm')
const sqlitePkgPath = resolve(sqliteWasmDir, 'package.json')
if (existsSync(sqlitePkgPath)) {
  const sqlitePkg = JSON.parse(readFileSync(sqlitePkgPath, 'utf-8'))
  const sqliteNext = bumpVersion(sqlitePkg.version)
  packages.push({
    dir: sqliteWasmDir,
    pkgPath: sqlitePkgPath,
    pkg: sqlitePkg,
    next: sqliteNext,
  })
}

// version map for resolving workspace:* at publish time
const versionMap = new Map(packages.map((p) => [p.pkg.name, p.next]))

for (const p of packages) {
  console.info(`  ${p.pkg.name}: ${p.pkg.version} -> ${p.next}`)
}

// check: format, lint, types, tests
console.info('\nchecking...')
run('bun run format')
run('bun run format:check')
run('bun run lint')
run('bun run check')
if (!skipTest) {
  run('bun run test')
  if (packages.length > 1) {
    run('bun install', { cwd: sqliteWasmDir })
    run('bun run test', { cwd: sqliteWasmDir })
  }
}

// build orez
console.info('\nbuilding...')
run('bun run build')

// bump versions in source
for (const p of packages) {
  p.pkg.version = p.next
  writeFileSync(p.pkgPath, JSON.stringify(p.pkg, null, 2) + '\n')
}

// regenerate lockfile (workspace:* resolves locally, no npm needed)
run('bun install')

if (dryRun) {
  console.info(`\n[dry-run] would publish:`)
  for (const p of packages) {
    console.info(`  ${p.pkg.name}@${p.next}`)
  }
  // revert versions
  for (const p of packages) {
    const original = JSON.parse(readFileSync(p.pkgPath, 'utf-8'))
    const [m, mi, pa] = p.next.split('.').map(Number)
    original.version = major
      ? `${m - 1}.0.0`
      : minor
        ? `${m}.${mi - 1}.0`
        : `${m}.${mi}.${pa - 1}`
    writeFileSync(p.pkgPath, JSON.stringify(original, null, 2) + '\n')
  }
  run('bun install')
  process.exit(0)
}

// publish each package from a tmp copy with workspace:* resolved
const tmpBase = mkdtempSync(join(tmpdir(), 'orez-publish-'))
console.info(`\npublishing from ${tmpBase}`)

for (const p of packages) {
  const name = p.pkg.name
  const tmpDir = join(tmpBase, name)

  // copy package files to tmp
  const files: string[] = p.pkg.files || []
  const filesToCopy = [...files, 'package.json']
  if (existsSync(resolve(p.dir, 'README.md'))) filesToCopy.push('README.md')
  if (existsSync(resolve(p.dir, 'LICENSE'))) filesToCopy.push('LICENSE')

  for (const f of filesToCopy) {
    const src = resolve(p.dir, f)
    const dest = join(tmpDir, f)
    if (existsSync(src)) {
      cpSync(src, dest, { recursive: true })
    }
  }

  // resolve workspace:* references in the tmp package.json
  const tmpPkgPath = join(tmpDir, 'package.json')
  const tmpPkg = JSON.parse(readFileSync(tmpPkgPath, 'utf-8'))
  for (const depField of ['dependencies', 'devDependencies', 'peerDependencies']) {
    const deps = tmpPkg[depField]
    if (!deps) continue
    for (const dep of Object.keys(deps)) {
      if (deps[dep].startsWith('workspace:')) {
        const resolved = versionMap.get(dep)
        if (resolved) {
          deps[dep] = resolved
        }
      }
    }
  }
  // remove workspace-only fields
  delete tmpPkg.workspaces
  writeFileSync(tmpPkgPath, JSON.stringify(tmpPkg, null, 2) + '\n')

  console.info(`\npublishing ${name}@${p.next}...`)
  run('npm publish --access public', { cwd: tmpDir })
}

// git commit + tag + push
const tag = `v${orezNext}`
run('git add -A')
run(`git commit -m "${tag}"`)
run(`git tag ${tag}`)
run('git push origin HEAD')
run(`git push origin ${tag}`)

console.info(`\nreleased:`)
for (const p of packages) {
  console.info(`  ${p.pkg.name}@${p.next}`)
}
