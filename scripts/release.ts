#!/usr/bin/env bun

/**
 * release script: check, build, publish the Orez package family, commit, tag, push.
 * uses workspace:* protocol — at publish time we copy to tmp and replace with real versions.
 */

import { execSync } from 'node:child_process'
import {
  chmodSync,
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
  assertLocalReleaseVersions,
  orderReleasePackages,
  selectLocalReleasePackages,
} from './release-package-order.js'
import {
  currentSyncNativePlatform,
  prepareLauncherPackage,
  preparePlatformPackage,
} from './sync-native-package.js'

const args = process.argv.slice(2)
const knownArgs = new Set([
  '--canary',
  '--ci',
  '--dry-run',
  '--into',
  '--major',
  '--minor',
  '--pack-only',
  '--patch',
  '--republish',
  '--skip-all',
  '--skip-build',
  '--skip-test',
])
for (let index = 0; index < args.length; index++) {
  const arg = args[index]
  if (!knownArgs.has(arg)) {
    throw new Error(`Unknown release argument: ${arg}`)
  }
  if (arg === '--into') index++
}
const dryRun = args.includes('--dry-run')
const patch = args.includes('--patch')
const minor = args.includes('--minor')
const major = args.includes('--major')
const canary = args.includes('--canary')
const ci = args.includes('--ci')
const rePublish = args.includes('--republish')
const skipAll = args.includes('--skip-all')
const skipTest = args.includes('--skip-test') || skipAll || rePublish
const skipBuild = args.includes('--skip-build') || skipAll || rePublish
const packOnly = args.includes('--pack-only')
const intoIdx = args.indexOf('--into')
const into = intoIdx !== -1 ? args[intoIdx + 1] : null
const trustedPublishing =
  process.env.GITHUB_ACTIONS === 'true' &&
  Boolean(process.env.ACTIONS_ID_TOKEN_REQUEST_URL) &&
  Boolean(process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN)

if (!patch && !minor && !major && !canary && !rePublish && !packOnly && !into) {
  console.info(
    'usage: bun scripts/release.ts --patch|--minor|--major|--canary|--republish [--dry-run] [--skip-build] [--skip-test] [--pack-only] [--into <dir>]\n       bun scripts/release.ts --pack-only [--patch|--minor|--major|--canary] [--skip-build]'
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

// Stage a package into `destDir` ready to pack: its shipped files, plus a
// package.json with workspace:* resolved to real versions and the
// workspace-only fields dropped.
//
// Everything that packs goes through here. A tarball packed straight from the
// repo tree carries `workspace:*` specifiers, which resolve to nothing once
// installed: orez-lite's `./realtime` re-export of orez-sync-executor is one
// import that then fails outright in the consumer.
function stageForPack(
  dir: string,
  pkg: { name?: string; files?: string[]; scripts?: Record<string, string> },
  version: string,
  versionMap: Map<string, string>,
  destDir: string
): void {
  for (const file of pkg.files ?? []) {
    if (!existsSync(resolve(dir, file))) {
      throw new Error(
        `${pkg.name ?? dir}: package file '${file}' is missing. Build before publishing or remove --skip-build.`
      )
    }
  }

  const filesToCopy = [...(pkg.files ?? []), 'package.json']
  if (existsSync(resolve(dir, 'README.md'))) filesToCopy.push('README.md')
  if (existsSync(resolve(dir, 'LICENSE'))) filesToCopy.push('LICENSE')

  for (const file of filesToCopy) {
    const src = resolve(dir, file)
    if (existsSync(src)) cpSync(src, join(destDir, file), { recursive: true })
  }

  const stagedPath = join(destDir, 'package.json')
  const staged = JSON.parse(readFileSync(stagedPath, 'utf-8'))
  staged.version = version
  for (const depField of [
    'dependencies',
    'devDependencies',
    'peerDependencies',
    'optionalDependencies',
  ]) {
    const deps = staged[depField]
    if (!deps) continue
    for (const dep of Object.keys(deps)) {
      if (deps[dep].startsWith('workspace:')) {
        const resolved = versionMap.get(dep)
        if (resolved) deps[dep] = resolved
      }
    }
  }
  // remove workspace-only fields. prepare builds workspace packages from the
  // repo tree; this copy ships prebuilt dist and has no workspace tree, so npm
  // must not run it here or for anyone installing the published git ref
  delete staged.workspaces
  if (staged.scripts) delete staged.scripts.prepare
  writeFileSync(stagedPath, JSON.stringify(staged, null, 2) + '\n')
}

// Every installed copy of `name` under the target, not only the hoisted one.
//
// A consumer nests a second copy of a package whenever two of its dependencies
// want different versions, and the nested one wins for anything resolving from
// inside that subtree. Refreshing only the hoisted copy leaves the consumer
// running two versions of the family at once with nothing to say so: team-machine
// resolved orez-lite's realtime re-export into a stale nested sync-executor
// that had no such export, and the import simply failed.
function installedCopies(targetDir: string, name: string): string[] {
  const found: string[] = []
  const visit = (modulesDir: string) => {
    if (!existsSync(modulesDir)) return
    if (existsSync(join(modulesDir, name, 'package.json'))) {
      found.push(join(modulesDir, name))
    }
    for (const entry of readdirSync(modulesDir, { withFileTypes: true })) {
      // symlinks are skipped rather than followed: a linked package belongs to
      // whatever tree it really lives in, and following them can cycle
      if (!entry.isDirectory() || entry.name.startsWith('.')) continue
      const dir = join(modulesDir, entry.name)
      if (entry.name.startsWith('@')) {
        for (const scoped of readdirSync(dir, { withFileTypes: true })) {
          if (scoped.isDirectory()) visit(join(dir, scoped.name, 'node_modules'))
        }
        continue
      }
      visit(join(dir, 'node_modules'))
    }
  }
  visit(join(targetDir, 'node_modules'))
  return found
}

function installedCopyVersions(
  targetDir: string,
  name: string
): { dir: string; version: string }[] {
  return installedCopies(targetDir, name).map((dir) => {
    const installedPkg = JSON.parse(readFileSync(join(dir, 'package.json'), 'utf8'))
    if (typeof installedPkg.version !== 'string') {
      throw new Error(`${name}: installed package at ${dir} has no version`)
    }
    return { dir, version: installedPkg.version }
  })
}

// --into <dir>: quick local release, packs each package and unpacks into target node_modules
if (into) {
  if (!into || into.startsWith('--')) {
    console.error('missing directory argument for --into')
    process.exit(1)
  }
  const targetDir = resolve(into.replace(/^~/, process.env.HOME!))

  // gather packages the same way the normal flow does
  const pkgDirs: { name: string; dir: string; pkg: any }[] = []
  const rootPkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf-8'))
  pkgDirs.push({ name: rootPkg.name, dir: root, pkg: rootPkg })
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

  const orezLiteDir = resolve(root, 'packages', 'orez-lite')
  const orezLitePkgPath = resolve(orezLiteDir, 'package.json')
  if (existsSync(orezLitePkgPath)) {
    const orezLitePkg = JSON.parse(readFileSync(orezLitePkgPath, 'utf-8'))
    pkgDirs.push({
      name: orezLitePkg.name,
      dir: orezLiteDir,
      pkg: orezLitePkg,
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

  const onZeroDir = resolve(root, 'packages', 'on-zero')
  const onZeroPkgPath = resolve(onZeroDir, 'package.json')
  if (existsSync(onZeroPkgPath)) {
    const onZeroPkg = JSON.parse(readFileSync(onZeroPkgPath, 'utf-8'))
    pkgDirs.push({ name: onZeroPkg.name, dir: onZeroDir, pkg: onZeroPkg })
  }

  const sourcePackageCopies = pkgDirs.map(({ name, pkg }) => ({
    pkg,
    copies: installedCopyVersions(targetDir, name),
  }))
  assertLocalReleaseVersions(sourcePackageCopies)
  const sourceCopies = new Map(
    sourcePackageCopies.map(({ pkg, copies }) => [pkg.name, copies])
  )

  console.info('building...')
  cleanRootDist()
  run('bun run build')
  run('bun run build:dist', { cwd: resolve(root, 'packages', 'sync-cf-host') })
  const tmpDir = mkdtempSync(join(tmpdir(), 'orez-release-into-'))

  const nativePlatform = currentSyncNativePlatform()
  if (!nativePlatform) {
    throw new Error(`sync-native does not support ${process.platform} ${process.arch}`)
  }
  run('cargo build --release -p sync-native --bin sync-native')
  const nativePlatformDir = resolve(tmpDir, 'native-platform')
  const nativeBinary = resolve(root, 'target', 'release', nativePlatform.executable)
  preparePlatformPackage(nativePlatform.id, nativeBinary, nativePlatformDir)
  const nativePlatformPkg = JSON.parse(
    readFileSync(resolve(nativePlatformDir, 'package.json'), 'utf8')
  )
  pkgDirs.push({
    name: nativePlatformPkg.name,
    dir: nativePlatformDir,
    pkg: nativePlatformPkg,
  })

  const nativeLauncherDir = resolve(tmpDir, 'native-launcher')
  prepareLauncherPackage(nativeLauncherDir)
  const nativeLauncherPkg = JSON.parse(
    readFileSync(resolve(nativeLauncherDir, 'package.json'), 'utf8')
  )
  pkgDirs.push({
    name: nativeLauncherPkg.name,
    dir: nativeLauncherDir,
    pkg: nativeLauncherPkg,
  })

  const packageCopies = pkgDirs.map(({ name, pkg }) => ({
    pkg,
    copies: sourceCopies.get(name) ?? installedCopyVersions(targetDir, name),
  }))
  assertLocalReleaseVersions(packageCopies)
  const copies = new Map(
    packageCopies.map(({ pkg, copies }) => [pkg.name, copies.map(({ dir }) => dir)])
  )
  const installed = new Set(
    pkgDirs.filter(({ name }) => copies.get(name)!.length > 0).map(({ name }) => name)
  )
  const selectedPkgDirs = selectLocalReleasePackages(pkgDirs, installed)

  // --into ships the versions already in the tree; nothing is bumped here
  const versionMap = new Map(
    pkgDirs.map(({ name, pkg }) => [name, pkg.version as string])
  )

  let released = 0
  try {
    for (const { name, dir, pkg } of selectedPkgDirs) {
      // a dependency the consumer does not have yet lands hoisted
      const destDirs = copies.get(name)!
      if (destDirs.length === 0) destDirs.push(join(targetDir, 'node_modules', name))

      const stageDir = join(tmpDir, 'stage', name)
      mkdirSync(stageDir, { recursive: true })
      stageForPack(dir, pkg, pkg.version, versionMap, stageDir)
      run(`npm pack --pack-destination ${tmpDir}`, { cwd: stageDir, silent: true })

      const files = readdirSync(tmpDir)
      const prefix = name.replace('@', '').replace('/', '-')
      const packed = files.find((f) => f.startsWith(prefix) && f.endsWith('.tgz'))

      if (!packed) throw new Error(`${name}: pack produced no tgz`)

      const tgzPath = join(tmpDir, packed)
      for (const destDir of destDirs) {
        mkdirSync(destDir, { recursive: true })
        rmSync(join(destDir, 'dist'), { recursive: true, force: true })
        run(`tar -xzf ${tgzPath} -C ${destDir} --strip-components=1`, { silent: true })
      }
      rmSync(tgzPath)
      released++
      const nested = destDirs.length > 1 ? ` (${destDirs.length} copies)` : ''
      console.info(`  ✓ ${name}${nested}`)
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

// orez-lite — public SQLite and Rust sync engine.
const orezLiteDir = resolve(root, 'packages', 'orez-lite')
const orezLitePkgPath = resolve(orezLiteDir, 'package.json')
if (existsSync(orezLitePkgPath)) {
  const orezLitePkg = JSON.parse(readFileSync(orezLitePkgPath, 'utf-8'))
  packages.push({
    dir: orezLiteDir,
    originalVersion: orezLitePkg.version,
    pkgPath: orezLitePkgPath,
    pkg: orezLitePkg,
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

// on-zero shares the orez version with the rest of the release family.
const onZeroDir = resolve(root, 'packages', 'on-zero')
const onZeroPkgPath = resolve(onZeroDir, 'package.json')
if (existsSync(onZeroPkgPath)) {
  const onZeroPkg = JSON.parse(readFileSync(onZeroPkgPath, 'utf-8'))
  packages.push({
    dir: onZeroDir,
    originalVersion: onZeroPkg.version,
    pkgPath: onZeroPkgPath,
    pkg: onZeroPkg,
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
const nativeLauncherPkg = JSON.parse(
  readFileSync(resolve(root, 'packages', 'orez-sync-native', 'package.json'), 'utf8')
)
versionMap.set(nativeLauncherPkg.name, nativeLauncherPkg.version)

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
if (!packOnly && !rePublish) {
  console.info('\nchecking...')
  run('make -B dist/package.json', { cwd: sqliteWasmDir })
  run('bun run format')
  run('bun run format:check')
  run('bun run lint')
  if (skipBuild) console.info('skipping build-dependent checks')
  else run('bun run check')
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

// the npm sync-native host must read the same durable contract as this tree.
// a version string cannot prove that: npm 0.1.2 shipped without the packed
// ledger while a source build calling itself 0.1.2 shipped with it, and the
// skew served lastMutationID 0 to every local client with nothing failing
// (soot factory defect #49, 2026-08-06). compare the schema revision the two
// binaries actually report, and hold the stable release until the dispatched
// sync-native release catches npm up.
if (!packOnly && !canary && !rePublish) {
  console.info('\nchecking npm sync-native contract...')
  const nativePlatform = currentSyncNativePlatform()
  if (!nativePlatform) {
    throw new Error(`sync-native does not support ${process.platform} ${process.arch}`)
  }
  const localBinary = resolve(root, 'target', 'release', nativePlatform.executable)
  if (!skipBuild) run('cargo build --release -p sync-native --bin sync-native')
  if (!existsSync(localBinary)) {
    throw new Error(
      `sync-native release binary is missing at ${localBinary}. Build before publishing or remove --skip-build.`
    )
  }
  // "sync-native <pkg version> <schema revision>"; an old binary prints no
  // revision at all, which compares as a mismatch, which is correct.
  const revisionOf = (versionOutput: string) =>
    versionOutput.trim().split(/\s+/).slice(2).join(' ')
  const localRevision = revisionOf(
    execSync(`${localBinary} --version`, { encoding: 'utf8' })
  )
  const guardDir = mkdtempSync(join(tmpdir(), 'orez-sync-native-guard-'))
  run(`npm pack ${nativePlatform.npmPackage}@latest --silent`, { cwd: guardDir })
  run('tar -xzf *.tgz', { cwd: guardDir })
  const publishedBinary = resolve(guardDir, 'package', 'bin', nativePlatform.executable)
  chmodSync(publishedBinary, 0o755)
  const publishedRevision = revisionOf(
    execSync(`${publishedBinary} --version`, { encoding: 'utf8' })
  )
  rmSync(guardDir, { recursive: true, force: true })
  if (publishedRevision !== localRevision) {
    throw new Error(
      `npm ${nativePlatform.npmPackage} reads contract '${publishedRevision || 'none'}' but this tree needs '${localRevision}'. ` +
        'Dispatch the release-sync-native workflow from this commit, wait for it to publish, then re-run the release.'
    )
  }
}

if (rePublish) {
  console.info('\nrepublishing existing package artifacts')
} else if (skipBuild) {
  console.info('\nskipping build; using existing package artifacts')
} else {
  console.info('\nbuilding...')
  cleanRootDist()
  run('bun run build')
  run('bun run build:dist', { cwd: resolve(root, 'packages', 'sync-cf-host') })
}

// bump versions in source (skip for --pack-only and --canary)
if (!packOnly && !canary && !rePublish) {
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
  if (!rePublish) {
    // revert versions
    for (const p of packages) {
      const original = JSON.parse(readFileSync(p.pkgPath, 'utf-8'))
      original.version = p.originalVersion
      writeFileSync(p.pkgPath, JSON.stringify(original, null, 2) + '\n')
    }
    run('bun install')
  }
  process.exit(0)
}

// publish each package from a tmp copy with workspace:* resolved
const tmpBase = mkdtempSync(join(tmpdir(), 'orez-publish-'))
console.info(`\n${packOnly ? 'packing to' : 'publishing from'} ${tmpBase}`)

const preparedPackages: Array<{
  name: string
  version: string
  cwd: string
  source: WorkspacePkg
}> = []

for (const p of packages) {
  const name = p.pkg.name
  const tmpDir = join(tmpBase, name)

  if (packOnly) {
    stageForPack(p.dir, p.pkg, p.next, versionMap, tmpDir)
    console.info(`\npacking ${name}@${p.next}...`)
    run('npm pack', { cwd: tmpDir })
  } else {
    preparedPackages.push({ name, version: p.next, cwd: tmpDir, source: p })
  }
}

if (packOnly) {
  console.info(`\npacked to ${tmpBase}`)
  process.exit(0)
}

function isPublished({ name, version }: (typeof preparedPackages)[number]) {
  try {
    const output = run(`npm view ${name}@${version} version --json --prefer-online`, {
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
  const tag = canary ? '--tag canary' : ''

  for (const { cwd, source } of pendingPackages) {
    stageForPack(source.dir, source.pkg, source.next, versionMap, cwd)
  }

  try {
    if (trustedPublishing) {
      // each npm process exchanges one package-scoped OIDC token. npm's
      // workspace publisher reuses its first token and package two rejects it.
      for (const pkg of pendingPackages) {
        run(`npm publish --ignore-scripts --access public ${tag}`.trim(), {
          cwd: pkg.cwd,
        })
      }
    } else {
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
      run(`npm publish --workspaces --ignore-scripts --access public ${tag}`.trim(), {
        cwd: tmpBase,
        env: { NODE_OPTIONS: nodeOptions },
      })
    }
  } catch (error) {
    let postflight = pendingPackages.map((pkg) => ({
      pkg,
      published: isPublished(pkg),
    }))
    if (postflight.some(({ published }) => !published)) {
      // npm can return before its package metadata is visible to `npm view`.
      // give successful workspaces one propagation window before reporting them.
      await Bun.sleep(10_000)
      postflight = postflight.map(({ pkg, published }) => ({
        pkg,
        published: published || isPublished(pkg),
      }))
    }
    const completed = postflight.filter(({ published }) => published)
    const missing = postflight.filter(({ published }) => !published)
    throw new Error(
      `Publish stopped after ${completed.length} packages${completed.length > 0 ? ` (${completed.map(({ pkg }) => pkg.name).join(', ')})` : ''}. Still missing:\n${missing.map(({ pkg }) => pkg.name).join('\n')}\n\nRe-run with --republish to retry only these packages with the existing build.`,
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
