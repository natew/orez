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
import { resolve, join } from 'node:path'
import { stdin as input, stdout as output } from 'node:process'
import { createInterface } from 'node:readline/promises'

const args = process.argv.slice(2)
const dryRun = args.includes('--dry-run')
const patch = args.includes('--patch')
const minor = args.includes('--minor')
const major = args.includes('--major')
const canary = args.includes('--canary')
const skipTest = args.includes('--skip-test') || args.includes('--skip-all')
const packOnly = args.includes('--pack-only')
const ci = args.includes('--ci')
const intoIdx = args.indexOf('--into')
const into = intoIdx !== -1 ? args[intoIdx + 1] : null
const canPromptForNpmOtp = Boolean(input.isTTY && output.isTTY && !process.env.CI && !ci)

if (!patch && !minor && !major && !canary && !packOnly && !into) {
  console.info(
    'usage: bun scripts/release.ts --patch|--minor|--major|--canary [--dry-run] [--skip-test] [--pack-only] [--into <dir>]'
  )
  process.exit(1)
}

const root = resolve(import.meta.dirname, '..')

function run(
  cmd: string,
  opts?: {
    captureOnError?: boolean
    cwd?: string
    env?: NodeJS.ProcessEnv
    silent?: boolean
  }
) {
  const cwd = opts?.cwd ?? root
  if (!opts?.silent) console.info(`$ ${cmd}`)
  const env = opts?.env ? { ...process.env, ...opts.env } : process.env

  try {
    return execSync(cmd, {
      stdio: opts?.silent || opts?.captureOnError ? 'pipe' : 'inherit',
      cwd,
      env,
    })
  } catch (err) {
    if (opts?.captureOnError && err && typeof err === 'object') {
      const error = err as Error & { stderr?: Buffer; stdout?: Buffer }
      const stdout = error.stdout?.toString() ?? ''
      const stderr = error.stderr?.toString() ?? ''
      if (stdout) process.stdout.write(stdout)
      if (stderr) process.stderr.write(stderr)
      throw new Error([error.message, stdout, stderr].filter(Boolean).join('\n'))
    }
    throw err
  }
}

function isPublishAuthOrOtpError(message: string) {
  return (
    /EOTP|one-time password|two-factor authentication|\botp\b/i.test(message) ||
    /code E404[\s\S]*PUT https:\/\/registry\.npmjs\.org\/@[^/\s]+%2f[^/\s]+/i.test(
      message
    )
  )
}

function redactNpmOtp(command: string) {
  return command.replace(/--otp(?:=|\s+)\S+/g, '--otp=******')
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

let cachedNpmOtp = process.env.npm_config_otp || process.env.NPM_CONFIG_OTP
let otpPromptInFlight: Promise<string> | undefined

function getNpmOtp(reason: string, optional = false): Promise<string> {
  if (otpPromptInFlight) return otpPromptInFlight

  otpPromptInFlight = (async () => {
    console.info(`\n${reason}`)

    const rl = createInterface({ input, output })
    try {
      while (true) {
        const code = (
          await rl.question(
            optional
              ? 'npm 2FA code (6 digits, empty to skip): '
              : 'npm 2FA code (6 digits): '
          )
        ).trim()

        if (!code) {
          if (optional) return ''
          throw new Error('No OTP provided, aborting publish')
        }

        if (/^\d{6}$/.test(code)) {
          cachedNpmOtp = code
          return code
        }

        console.info('Enter a 6-digit code')
      }
    } finally {
      rl.close()
    }
  })().finally(() => {
    otpPromptInFlight = undefined
  })

  return otpPromptInFlight
}

function bumpVersion(current: string): string {
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
  preparePgToSqliteDist()

  const tmpDir = mkdtempSync(join(tmpdir(), 'orez-release-into-'))

  // gather packages the same way the normal flow does
  const pkgDirs: { name: string; dir: string }[] = []
  const rootPkg = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf-8'))
  pkgDirs.push({ name: rootPkg.name, dir: root })
  const compilerDir = resolve(root, 'pg-to-sqlite')
  const compilerPkgPath = resolve(compilerDir, 'package.json')
  if (existsSync(compilerPkgPath)) {
    const compilerPkg = JSON.parse(readFileSync(compilerPkgPath, 'utf-8'))
    pkgDirs.push({ name: compilerPkg.name, dir: compilerDir })
  }

  const sqlDir = resolve(root, 'sqlite-wasm')
  const sqlPkgPath = resolve(sqlDir, 'package.json')
  if (existsSync(sqlPkgPath)) {
    const sqlPkg = JSON.parse(readFileSync(sqlPkgPath, 'utf-8'))
    pkgDirs.push({ name: sqlPkg.name, dir: sqlDir })
  }

  const syncHostDir = resolve(root, 'packages', 'sync-cf-host')
  const syncHostPkgPath = resolve(syncHostDir, 'package.json')
  if (existsSync(syncHostPkgPath)) {
    const syncHostPkg = JSON.parse(readFileSync(syncHostPkgPath, 'utf-8'))
    pkgDirs.push({ name: syncHostPkg.name, dir: syncHostDir })
  }

  let released = 0
  try {
    for (const { name, dir } of pkgDirs) {
      const destDir = join(targetDir, 'node_modules', name)
      if (!existsSync(destDir)) {
        console.info(`  skip ${name} (not in target node_modules)`)
        continue
      }

      try {
        if (name === '@orez/sync-cf-host') {
          run('bun run build:wasm', { cwd: dir })
        }
        run(`npm pack --pack-destination ${tmpDir}`, { cwd: dir, silent: true })

        const files = readdirSync(tmpDir)
        const prefix = name.replace('@', '').replace('/', '-')
        const packed = files.find((f) => f.startsWith(prefix) && f.endsWith('.tgz'))

        if (!packed) {
          console.warn(`  skip ${name}: pack produced no tgz`)
          continue
        }

        const tgzPath = join(tmpDir, packed)
        rmSync(join(destDir, 'dist'), { recursive: true, force: true })
        run(`tar -xzf ${tgzPath} -C ${destDir} --strip-components=1`, { silent: true })
        rmSync(tgzPath)
        released++
        console.info(`  ✓ ${name}`)
      } catch (err) {
        console.warn(`  ✗ ${name}: ${err}`)
      }
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

// @orez/sync-cf-host — CF DO host for the rust sync engine, published as TS
// source + generated wasm (consumers bundle with wrangler). skip if the wasm
// engine isn't built.
const cfHostDir = resolve(root, 'packages', 'sync-cf-host')
const cfHostPkgPath = resolve(cfHostDir, 'package.json')
const cfHostWasmExists = existsSync(
  resolve(cfHostDir, 'src', 'generated', 'sync_wasm_bg.wasm')
)
if (existsSync(cfHostPkgPath) && cfHostWasmExists) {
  const cfHostPkg = JSON.parse(readFileSync(cfHostPkgPath, 'utf-8'))
  packages.push({
    dir: cfHostDir,
    originalVersion: cfHostPkg.version,
    pkgPath: cfHostPkgPath,
    pkg: cfHostPkg,
    next: orezNext,
  })
} else if (existsSync(cfHostPkgPath)) {
  console.info(
    'skipping @orez/sync-cf-host (no wasm built — run bun run build:wasm there)'
  )
}

// for --pack-only, use current versions instead of bumping
if (packOnly) {
  for (const p of packages) {
    p.next = p.pkg.version
  }
}

// version map for resolving workspace:* at publish time
const versionMap = new Map(packages.map((p) => [p.pkg.name, p.next]))

for (const p of packages) {
  if (packOnly) {
    console.info(`  ${p.pkg.name}: ${p.pkg.version}`)
  } else {
    console.info(`  ${p.pkg.name}: ${p.pkg.version} -> ${p.next}`)
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

if (!packOnly) {
  try {
    run('npm whoami', { cwd: tmpBase, silent: true })
  } catch (err) {
    throw new Error(
      `npm is not authenticated for publishing. Run \`npm login\` and then re-run the release.\n\n${err}`
    )
  }

  if (!cachedNpmOtp && canPromptForNpmOtp) {
    await getNpmOtp(
      'Most orez npm publishes require 2FA. Provide the current code now so every package publish uses it.',
      true
    )
  }
}

async function publishWithOtp(name: string, version: string, cwd: string) {
  const tag = canary ? '--tag canary' : ''
  const publishCommand = `npm publish --access public ${tag}`.trim()

  console.info(`\npublishing ${name}@${version}...`)

  let attempt = 0
  let otp = cachedNpmOtp

  while (true) {
    attempt++

    try {
      console.info(
        `$ ${redactNpmOtp(
          [publishCommand, otp ? '--otp=******' : ''].filter(Boolean).join(' ')
        )}`
      )
      const publishOutput = run(publishCommand, {
        cwd,
        env: otp ? { npm_config_otp: otp } : undefined,
        silent: true,
        captureOnError: true,
      })
      if (publishOutput.length) {
        process.stdout.write(publishOutput)
      }
      return
    } catch (err) {
      const message = String(err)
      const needsOtp = isPublishAuthOrOtpError(message)

      if (needsOtp && attempt < 3) {
        if (!canPromptForNpmOtp) {
          throw new Error(
            `npm requires a 2FA code to publish ${name}. Re-run with NPM_CONFIG_OTP set.\n\n${message}`
          )
        }

        if (otp && cachedNpmOtp === otp) {
          cachedNpmOtp = undefined
        }

        otp = await getNpmOtp(
          attempt === 1
            ? `npm requires a 2FA code to publish ${name}`
            : `npm 2FA code expired, need a fresh one for ${name}`
        )
        continue
      }

      throw err
    }
  }
}

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
  // remove workspace-only fields
  delete tmpPkg.workspaces
  writeFileSync(tmpPkgPath, JSON.stringify(tmpPkg, null, 2) + '\n')

  if (packOnly) {
    console.info(`\npacking ${name}@${p.next}...`)
    run('npm pack', { cwd: tmpDir })
  } else {
    await publishWithOtp(name, p.next, tmpDir)
  }
}

if (packOnly) {
  console.info(`\npacked to ${tmpBase}`)
  process.exit(0)
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
