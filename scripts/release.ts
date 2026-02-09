#!/usr/bin/env bun

/**
 * minimal release script: check, build, publish, commit, tag, push.
 */

import { execSync } from 'node:child_process'
import { readFileSync, writeFileSync } from 'node:fs'
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

function run(cmd: string, opts?: { silent?: boolean }) {
  if (!opts?.silent) console.info(`$ ${cmd}`)
  return execSync(cmd, { stdio: opts?.silent ? 'pipe' : 'inherit', cwd: resolve('.') })
}

function runText(cmd: string): string {
  return execSync(cmd, { cwd: resolve('.') })
    .toString()
    .trim()
}

// read current version
const pkgPath = resolve('package.json')
const pkg = JSON.parse(readFileSync(pkgPath, 'utf-8'))
const [curMajor, curMinor, curPatch] = pkg.version.split('.').map(Number)

const nextVersion = major
  ? `${curMajor + 1}.0.0`
  : minor
    ? `${curMajor}.${curMinor + 1}.0`
    : `${curMajor}.${curMinor}.${curPatch + 1}`

console.info(`releasing: ${pkg.version} -> ${nextVersion}`)

// check: lint, types, tests
console.info('\nchecking...')
run('bun run lint')
run('bun run check')
run('bun run test')

// build
console.info('\nbuilding...')
run('bun run build')

// bump version
pkg.version = nextVersion
writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n')

if (dryRun) {
  console.info(`\n[dry-run] would publish v${nextVersion}`)
  // revert version
  pkg.version = `${curMajor}.${curMinor}.${curPatch}`
  writeFileSync(pkgPath, JSON.stringify(pkg, null, 2) + '\n')
  process.exit(0)
}

// publish
console.info('\npublishing...')
run('npm publish')

// git commit + tag + push
const tag = `v${nextVersion}`
run('git add -A')
run(`git commit -m "${tag}"`)
run(`git tag ${tag}`)
run('git push origin HEAD')
run(`git push origin ${tag}`)

console.info(`\nreleased ${tag}`)
