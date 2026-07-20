#!/usr/bin/env bun

/**
 * release script: check, build, publish both orez + bedrock-sqlite, commit, tag, push.
 * uses workspace:* protocol — at publish time we copy to tmp and replace with real versions.
 */

import { execSync } from 'node:child_process'
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve, join, relative } from 'node:path'

import {
  orderReleasePackages,
  selectLocalReleasePackages,
} from './release-package-order.js'

const args = process.argv.slice(2)
const dryRun = args.includes('--dry-run')
const patch = args.includes('--patch')
const minor = args.includes('--minor')
const major = args.includes('--major')
const canary = args.includes('--canary')
const ci = args.includes('--ci')
const rePublish = args.includes('--republish')
const skipTest = args.includes('--skip-test') || args.includes('--skip-all')
const packOnly = args.includes('--pack-only')
const intoIdx = args.indexOf('--into')
const into = intoIdx !== -1 ? args[intoIdx + 1] : null
const trustedPublishing =
  process.env.GITHUB_ACTIONS === 'true' &&
  Boolean(process.env.ACTIONS_ID_TOKEN_REQUEST_URL) &&
  Boolean(process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN)

if (!patch && !minor && !major && !canary && !rePublish && !packOnly && !into) {
  console.info(
    'usage: bun scripts/release.ts --patch|--minor|--major|--canary|--republish [--dry-run] [--skip-test] [--pack-only] [--into <dir>]\n       bun scripts/release.ts --pack-only [--patch|--minor|--major|--canary]'
  )
  process.exit(1)
}

const root = resolve(import.meta.dirname, '..')

function run(
  cmd: string,
  opts?: {
    cwd?: string
    env?: Record<string, string>
    silent?: boolean
  }
) {
  const cwd = opts?.cwd ?? root
  if (!opts?.silent) console.info(`$ ${cmd}`)
  return execSync(cmd, {
    stdio: opts?.silent ? 'pipe' : 'inherit',
    cwd,
    env: { ...process.env, ...opts?.env },
  })
}

function cleanRootDist() {
  rmSync(resolve(root, 'dist'), { recursive: true, force: true })
}

function preparePgToSqliteDist() {
  const packageDir = resolve(root, 'pg-to-sqlite')
  const dest = resolve(packageDir, 'dist')
  rmSync(dest, { recursive: true, force: true })
  mkdirSync(dest, { recursive: true })
  cpSync(resolve(root, 'dist', 'pg-sqlite-compiler'), join(dest, 'pg-sqlite-compiler'), {
    recursive: true,
  })
  rmSync(join(dest, 'pg-sqlite-compiler', 'test'), { recursive: true, force: true })
  for (const file of [
    'sqlite-keyword-identifiers.js',
    'sqlite-keyword-identifiers.js.map',
    'sqlite-keyword-identifiers.d.ts',
    'sqlite-keyword-identifiers.d.ts.map',
  ]) {
    const src = resolve(root, 'dist', file)
    if (existsSync(src)) cpSync(src, join(dest, file))
  }
}

function bumpVersion(current: string): string {
  if (rePublish) {
    return current
  }

  // strip any existing prerelease tag (e.g. -canary.123)
  const base = current.split('-')[0]
  const [curMajor, curMinor, curPatch] = base.split('.').map(Number)

  if (canary) {
    // canary: use current version + timestamp suffix, no version bump
    const timestamp = Date.now()
    return `${curMajor}.${curMinor}.${curPatch}-canary.${timestamp}`
  }

  return major
    ? `${curMajor + 1}.0.0`
    : minor
      ? `${curMajor}.${curMinor + 1}.0`
      : `${curMajor}.${curMinor}.${curPatch + 1}`
}

// --into <dir>: quick local release, packs each package and unpacks into target node_modules
if (into) {
  if (!into || into.startsWith('--')) {
    console.error('missing directory argument for --into')
    process.exit(1)
  }
  const targetDir = resolve(into.replace(/^~/, process.env.HOME!))

  console.info('building...')
  cleanRootDist()
  run('bun run build')
  run('bun run build:dist', { cwd: resolve(root, 'packages', 'sync-cf-host') })
  preparePgToSqliteDist()

  const tmpDir = mkdtempSync(join(tmpdir(), 'orez-release-into-'))

  // gather packages the same way the normal flow does
  const pkgDirs: { name: string; dir: string; pkg: any }[] = []
  const rootPkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf-8'))
  pkgDirs.push({ name: rootPkg.name, dir: root, pkg: rootPkg })
  const compilerDir = resolve(root, 'pg-to-sqlite')
  const compilerPkgPath = resolve(compilerDir, 'package.json')
  if (existsSync(compilerPkgPath)) {
    const compilerPkg = JSON.parse(readFileSync(compilerPkgPath, 'utf-8'))
    pkgDirs.push({ name: compilerPkg.name, dir: compilerDir, pkg: compilerPkg })
  }

  const sqlDir = resolve(root, 'sqlite-wasm')
  const sqlPkgPath = resolve(sqlDir, 'package.json')
  if (existsSync(sqlPkgPath)) {
    const sqlPkg = JSON.parse(readFileSync(sqlPkgPath, 'utf-8'))
    pkgDirs.push({ name: sqlPkg.name, dir: sqlDir, pkg: sqlPkg })
  }

  const syncHostDir = resolve(root, 'packages', 'sync-cf-host')
  const syncHostPkgPath = resolve(syncHostDir, 'package.json')
  if (existsSync(syncHostPkgPath)) {
    const syncHostPkg = JSON.parse(readFileSync(syncHostPkgPath, 'utf-8'))
    pkgDirs.push({ name: syncHostPkg.name, dir: syncHostDir, pkg: syncHostPkg })
  }

  const syncExecutorDir = resolve(root, 'packages', 'sync-executor')
  const syncExecutorPkgPath = resolve(syncExecutorDir, 'package.json')
  if (existsSync(syncExecutorPkgPath)) {
    const syncExecutorPkg = JSON.parse(readFileSync(syncExecutorPkgPath, 'utf-8'))
    pkgDirs.push({
      name: syncExecutorPkg.name,
      dir: syncExecutorDir,
      pkg: syncExecutorPkg,
    })
  }

  const drizzleZeroSqliteDir = resolve(root, 'packages', 'drizzle-zero-sqlite')
  const drizzleZeroSqlitePkgPath = resolve(drizzleZeroSqliteDir, 'package.json')
  if (existsSync(drizzleZeroSqlitePkgPath)) {
    const drizzleZeroSqlitePkg = JSON.parse(
      readFileSync(drizzleZeroSqlitePkgPath, 'utf-8')
    )
    pkgDirs.push({
      name: drizzleZeroSqlitePkg.name,
      dir: drizzleZeroSqliteDir,
      pkg: drizzleZeroSqlitePkg,
    })
  }

  const installed = new Set(
    pkgDirs
      .filter(({ name }) => existsSync(join(targetDir, 'node_modules', name)))
      .map(({ name }) => name)
  )
  const selectedPkgDirs = selectLocalReleasePackages(pkgDirs, installed)

  let released = 0
  try {
    for (const { name, dir } of selectedPkgDirs) {
      const destDir = join(targetDir, 'node_modules', name)
      mkdirSync(destDir, { recursive: true })

      run(`npm pack --pack-destination ${tmpDir}`, { cwd: dir, silent: true })

      const files = readdirSync(tmpDir)
      const prefix = name.replace('@', '').replace('/', '-')
      const packed = files.find((f) => f.startsWith(prefix) && f.endsWith('.tgz'))

      if (!packed) throw new Error(`${name}: pack produced no tgz`)

      const tgzPath = join(tmpDir, packed)
      rmSync(join(destDir, 'dist'), { recursive: true, force: true })
      run(`tar -xzf ${tgzPath} -C ${destDir} --strip-components=1`, { silent: true })
      rmSync(tgzPath)
      released++
      console.info(`  ✓ ${name}`)
    }
  } finally {
    rmSync(tmpDir, { recursive: true, force: true })
  }

  console.info(`\nreleased ${released} package(s) into ${targetDir}`)
  process.exit(0)
}

// workspace packages: [dir, pkgPath, pkg, nextVersion]
interface WorkspacePkg {
  dir: string
  originalVersion: string
  pkgPath: string
  pkg: any
  next: string
}

const packages: WorkspacePkg[] = []

// orez (root)
const orezPkgPath = resolve(root, 'package.json')
const orezPkg = JSON.parse(readFileSync(orezPkgPath, 'utf-8'))
const orezNext = bumpVersion(orezPkg.version)
packages.push({
  dir: root,
  originalVersion: orezPkg.version,
  pkgPath: orezPkgPath,
  pkg: orezPkg,
  next: orezNext,
})

// bedrock-sqlite (workspace) — skip if wasm dist not built
const sqliteWasmDir = resolve(root, 'sqlite-wasm')
const sqlitePkgPath = resolve(sqliteWasmDir, 'package.json')
const sqliteDistExists = existsSync(resolve(sqliteWasmDir, 'dist', 'sqlite3.wasm'))
if (existsSync(sqlitePkgPath) && sqliteDistExists) {
  const sqlitePkg = JSON.parse(readFileSync(sqlitePkgPath, 'utf-8'))
  packages.push({
    dir: sqliteWasmDir,
    originalVersion: sqlitePkg.version,
    pkgPath: sqlitePkgPath,
    pkg: sqlitePkg,
    next: orezNext,
  })
} else if (existsSync(sqlitePkgPath) && !sqliteDistExists) {
  console.info('skipping bedrock-sqlite (no wasm dist built)')
}

// pg-to-sqlite — standalone compiler package sourced from src/pg-sqlite-compiler.
const compilerDir = resolve(root, 'pg-to-sqlite')
const compilerPkgPath = resolve(compilerDir, 'package.json')
if (existsSync(compilerPkgPath)) {
  const compilerPkg = JSON.parse(readFileSync(compilerPkgPath, 'utf-8'))
  packages.push({
    dir: compilerDir,
    originalVersion: compilerPkg.version,
    pkgPath: compilerPkgPath,
    pkg: compilerPkg,
    next: orezNext,
  })
}

// orez-sync-cf-host — built CF DO host and standalone query runtime plus
// generated wasm. skip if wasm isn't built — the release build step builds it.
const cfHostDir = resolve(root, 'packages', 'sync-cf-host')
const cfHostPkgPath = resolve(cfHostDir, 'package.json')
if (existsSync(cfHostPkgPath)) {
  const cfHostPkg = JSON.parse(readFileSync(cfHostPkgPath, 'utf-8'))
  packages.push({
    dir: cfHostDir,
    originalVersion: cfHostPkg.version,
    pkgPath: cfHostPkgPath,
    pkg: cfHostPkg,
    next: orezNext,
  })
}

// orez-sync-executor — host-neutral mutation execution and application adapters.
const syncExecutorDir = resolve(root, 'packages', 'sync-executor')
const syncExecutorPkgPath = resolve(syncExecutorDir, 'package.json')
if (existsSync(syncExecutorPkgPath)) {
  const syncExecutorPkg = JSON.parse(readFileSync(syncExecutorPkgPath, 'utf-8'))
  packages.push({
    dir: syncExecutorDir,
    originalVersion: syncExecutorPkg.version,
    pkgPath: syncExecutorPkgPath,
    pkg: syncExecutorPkg,
    next: orezNext,
  })
}

// drizzle-zero-sqlite package
const drizzleZeroSqliteDir = resolve(root, 'packages', 'drizzle-zero-sqlite')
const drizzleZeroSqlitePkgPath = resolve(drizzleZeroSqliteDir, 'package.json')
if (existsSync(drizzleZeroSqlitePkgPath)) {
  const drizzleZeroSqlitePkg = JSON.parse(readFileSync(drizzleZeroSqlitePkgPath, 'utf-8'))
  packages.push({
    dir: drizzleZeroSqliteDir,
    originalVersion: drizzleZeroSqlitePkg.version,
    pkgPath: drizzleZeroSqlitePkgPath,
    pkg: drizzleZeroSqlitePkg,
    next: orezNext,
  })
}

packages.splice(0, packages.length, ...orderReleasePackages(packages))

// plain --pack-only preserves current versions; an explicit release kind packs
// the next unpublished version without mutating source manifests.
if (packOnly && !patch && !minor && !major && !canary) {
  for (const p of packages) {
    p.next = p.pkg.version
  }
}

// version map for resolving workspace:* at publish time
const versionMap = new Map(packages.map((p) => [p.pkg.name, p.next]))

for (const p of packages) {
  if (packOnly) {
    console.info(`  ${p.pkg.name}: ${p.next}`)
  } else {
    console.info(`  ${p.pkg.name}: ${p.pkg.version} -> ${p.next}`)
  }
}

if (!packOnly && !dryRun && !trustedPublishing) {
  try {
    run('npm whoami', { silent: true })
  } catch {
    console.info(
      '\nnpm authentication is required before publishing. Opening npm login...'
    )
    run('npm login')

    try {
      run('npm whoami', { silent: true })
    } catch (error) {
      throw new Error('npm login completed, but npm whoami is still unauthenticated.', {
        cause: error,
      })
    }
  }
}

// check: format, lint, types, tests
if (!packOnly) {
  console.info('\nchecking...')
  run('bun run format')
  run('bun run format:check')
  run('bun run lint')
  run('bun run check')
  if (!skipTest) {
    run('bun run test')
    run('bun run test:sync-browser-host')
    run('bun run test:sync-cf-host')
    if (packages.length > 1) {
      run('bun install', { cwd: sqliteWasmDir })
      run('bun run test', { cwd: sqliteWasmDir })
    }
  }
}

// build orez
console.info('\nbuilding...')
cleanRootDist()
run('bun run build')
run('bun run build:dist', { cwd: resolve(root, 'packages', 'sync-cf-host') })
preparePgToSqliteDist()

// bump versions in source (skip for --pack-only and --canary)
if (!packOnly && !canary) {
  for (const p of packages) {
    p.pkg.version = p.next
    writeFileSync(p.pkgPath, JSON.stringify(p.pkg, null, 2) + '\n')
  }

  // regenerate lockfile (workspace:* resolves locally, no npm needed)
  run('bun install')
}

if (dryRun) {
  console.info(`\n[dry-run] would publish:`)
  for (const p of packages) {
    console.info(`  ${p.pkg.name}@${p.next}`)
  }
  // revert versions
  for (const p of packages) {
    const original = JSON.parse(readFileSync(p.pkgPath, 'utf-8'))
    original.version = p.originalVersion
    writeFileSync(p.pkgPath, JSON.stringify(original, null, 2) + '\n')
  }
  run('bun install')
  process.exit(0)
}

// publish each package from a tmp copy with workspace:* resolved
const tmpBase = mkdtempSync(join(tmpdir(), 'orez-publish-'))
console.info(`\n${packOnly ? 'packing to' : 'publishing from'} ${tmpBase}`)

const preparedPackages: Array<{ name: string; version: string; cwd: string }> = []

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

  // resolve workspace:* references and set version in the tmp package.json
  const tmpPkgPath = join(tmpDir, 'package.json')
  const tmpPkg = JSON.parse(readFileSync(tmpPkgPath, 'utf-8'))
  tmpPkg.version = p.next
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
  // remove workspace-only fields. prepare builds workspace packages from the
  // repo tree; this tmp package ships prebuilt dist and has no workspace tree,
  // so npm must not run it here or for anyone installing the published git ref
  delete tmpPkg.workspaces
  if (tmpPkg.scripts) delete tmpPkg.scripts.prepare
  writeFileSync(tmpPkgPath, JSON.stringify(tmpPkg, null, 2) + '\n')

  if (packOnly) {
    console.info(`\npacking ${name}@${p.next}...`)
    run('npm pack', { cwd: tmpDir })
  } else {
    preparedPackages.push({ name, version: p.next, cwd: tmpDir })
  }
}

if (packOnly) {
  console.info(`\npacked to ${tmpBase}`)
  process.exit(0)
}

function isPublished({ name, version }: (typeof preparedPackages)[number]) {
  try {
    const output = run(`npm view ${name}@${version} version --json`, {
      cwd: tmpBase,
      silent: true,
    }).toString()
    const found = JSON.parse(output.trim())
    return found === version || (Array.isArray(found) && found.includes(version))
  } catch (error) {
    const details = error as { stdout?: Buffer; stderr?: Buffer }
    const message = `${String(error)}\n${details.stdout || ''}\n${details.stderr || ''}`
    if (/E404|404 Not Found|is not in this registry/i.test(message)) {
      return false
    }
    throw new Error(`Could not verify ${name}@${version} on npm:\n${message}`)
  }
}

console.info(`Checking ${preparedPackages.length} package versions on npm...`)
const pendingPackages = preparedPackages.filter((pkg) => {
  if (isPublished(pkg)) {
    console.info(`Skipping ${pkg.name}: this version is already published`)
    return false
  }
  return true
})

if (pendingPackages.length > 0) {
  if (!ci && process.stdin.isTTY && process.stdout.isTTY) {
    console.info(
      'npm will open the browser for 2FA once. Select “do not challenge for the next 5 minutes” so the same short-lived approval can publish the remaining packages.'
    )
  }

  writeFileSync(
    join(tmpBase, 'package.json'),
    JSON.stringify(
      {
        name: 'orez-release',
        private: true,
        workspaces: pendingPackages.map((pkg) => relative(tmpBase, pkg.cwd)),
      },
      null,
      2
    ) + '\n'
  )

  const webAuthCache = join(root, 'scripts', 'cache-npm-webauth.cjs')
  const nodeOptions = [process.env.NODE_OPTIONS, `--require=${webAuthCache}`]
    .filter(Boolean)
    .join(' ')
  const tag = canary ? '--tag canary' : ''

  try {
    // trusted publishing exchanges a package-scoped OIDC token for each
    // workspace; the local passkey cache would replay the first package's token.
    run(`npm publish --workspaces --ignore-scripts --access public ${tag}`.trim(), {
      cwd: tmpBase,
      env: trustedPublishing ? {} : { NODE_OPTIONS: nodeOptions },
    })
  } catch (error) {
    const postflight = pendingPackages.map((pkg) => ({
      pkg,
      published: isPublished(pkg),
    }))
    const completed = postflight.filter(({ published }) => published)
    const missing = postflight.filter(({ published }) => !published)
    throw new Error(
      `Publish stopped after ${completed.length} packages. Still missing:\n${missing.map(({ pkg }) => pkg.name).join('\n')}\n\nRe-run with --republish to retry only these packages.`,
      { cause: error }
    )
  }
}

// git commit + tag + push (skip for canary releases)
if (!canary) {
  const gitTag = `v${orezNext}`
  // stage ONLY the files this release legitimately changed: the bumped
  // package.json of each workspace package plus the regenerated lockfile.
  // never `git add -A` — this checkout hosts concurrent agent sessions, and a
  // blanket add sweeps a co-tenant's uncommitted WIP into the version commit
  // (and any dirty source compiled into the just-published dist). real
  // incident: v0.4.31 swept an in-flight src/config.ts edit.
  const versionPaths = [
    ...packages.map((p) => p.pkgPath),
    resolve(root, 'bun.lock'),
  ].filter((p) => existsSync(p))
  const pathspec = versionPaths.map((p) => `'${p}'`).join(' ')
  run(`git add ${pathspec}`)
  run(`git commit -m "${gitTag}" -- ${pathspec}`)
  run(`git tag ${gitTag}`)
  run('git push origin HEAD')
  run(`git push origin ${gitTag}`)
}

console.info(`\nreleased${canary ? ' (canary)' : ''}:`)
for (const p of packages) {
  console.info(`  ${p.pkg.name}@${p.next}`)
}
