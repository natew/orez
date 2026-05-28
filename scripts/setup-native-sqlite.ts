#!/usr/bin/env bun

import { spawnSync } from 'node:child_process'

import {
  formatNativeBootstrapInstructions,
  inspectNativeSqliteBinary,
} from '../src/sqlite-mode/native-binary.ts'

function runReinstall(): void {
  const result = spawnSync('bun', ['i', '@rocicorp/zero-sqlite3'], {
    stdio: 'inherit',
    env: process.env,
  })

  if (result.status !== 0) {
    throw new Error('failed to reinstall @rocicorp/zero-sqlite3 with bun')
  }
}

function runPackageInstall(): void {
  const check = inspectNativeSqliteBinary()
  if (!check.packageRoot) {
    throw new Error(
      `[native:bootstrap] cannot locate @rocicorp/zero-sqlite3 package root\n${formatNativeBootstrapInstructions(check)}`
    )
  }

  const result = spawnSync('npm', ['run', 'install'], {
    cwd: check.packageRoot,
    stdio: 'inherit',
    env: process.env,
  })

  if (result.status !== 0) {
    throw new Error('failed to build @rocicorp/zero-sqlite3 native binding')
  }
}

function verifyOrThrow(context: string): void {
  const check = inspectNativeSqliteBinary()
  if (check.found) {
    console.log(`[native:bootstrap] ok (${context})`)
    for (const filePath of check.existingPaths) {
      console.log(`[native:bootstrap] found ${filePath}`)
    }
    return
  }

  throw new Error(
    `[native:bootstrap] ${context}\n${formatNativeBootstrapInstructions(check)}`
  )
}

function main(): void {
  const initial = inspectNativeSqliteBinary()
  if (initial.found) {
    verifyOrThrow('already present')
    return
  }

  console.warn(
    '[native:bootstrap] native binary missing, reinstalling @rocicorp/zero-sqlite3'
  )
  runReinstall()
  runPackageInstall()
  verifyOrThrow('post-reinstall check')
}

main()
