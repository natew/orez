import { expect, test } from 'bun:test'
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('a zero-exit queued npm publish cannot report a completed release', async () => {
  const root = mkdtempSync(join(tmpdir(), 'orez-queued-publish-test-'))
  try {
    mkdirSync(join(root, 'scripts'))
    mkdirSync(join(root, 'packages/orez-sync-native'), { recursive: true })
    for (const name of [
      'release',
      'release-package-order',
      'sync-native-package',
      'sync-native-platforms',
      'sync-native-release-plan',
      'verify-npm-release',
    ]) {
      const source = join(import.meta.dirname, `${name}.ts`)
      if (existsSync(source)) copyFileSync(source, join(root, 'scripts', `${name}.ts`))
    }
    writeFileSync(
      join(root, 'package.json'),
      JSON.stringify({ name: 'orez', version: '0.0.1', files: [] })
    )
    writeFileSync(
      join(root, 'packages/orez-sync-native/package.json'),
      JSON.stringify({ name: 'orez-sync-native', version: '0.1.18' })
    )
    mkdirSync(join(root, 'bin'))
    writeFileSync(
      join(root, 'bin/npm'),
      `#!/bin/sh
case "$1" in
  view) echo 'E404 Not Found' >&2; exit 1 ;;
  publish) echo 'Your package is being processed and may take a few minutes to become available.'; exit 0 ;;
  *) exit 99 ;;
esac
`
    )
    chmodSync(join(root, 'bin/npm'), 0o755)
    // advance the verification deadline without waiting or contacting npm.
    writeFileSync(
      join(root, 'queued.ts'),
      `
let now = 0
Date.now = () => (now += 60 * 60 * 1000)
globalThis.fetch = async () => new Response('not yet published', { status: 404 })
`
    )
    const child = Bun.spawn(
      [
        process.execPath,
        '--preload',
        join(root, 'queued.ts'),
        join(root, 'scripts/release.ts'),
        '--canary',
        '--ci',
        '--republish',
      ],
      {
        cwd: root,
        env: {
          ...process.env,
          PATH: `${join(root, 'bin')}:${process.env.PATH}`,
          GITHUB_ACTIONS: 'true',
          ACTIONS_ID_TOKEN_REQUEST_URL: 'fixture',
          ACTIONS_ID_TOKEN_REQUEST_TOKEN: 'fixture',
        },
        stdout: 'pipe',
        stderr: 'pipe',
      }
    )
    const [exit, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ])
    expect(stdout).toContain('Your package is being processed')
    expect(exit).not.toBe(0)
    expect(stdout).not.toContain('released (canary):')
    expect(stderr).toContain('orez@0.0.1')
    expect(stderr).toContain('not available')
  } finally {
    rmSync(root, { recursive: true, force: true })
  }
})
