#!/usr/bin/env node
import { spawnSync } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const rngpuiRoot = process.env.RNGPUI_ROOT
if (!rngpuiRoot) throw new Error('RNGPUI_ROOT is required')
const service = buildGPUIService(rngpuiRoot)

const repositoryRoot = resolve(import.meta.dirname, '..')
const conformanceModule = resolve(
  repositoryRoot,
  'dist/zero-http/encrypted-column-conformance.js'
)
const workdir = mkdtempSync(join(tmpdir(), 'orez-encryption-hermes-'))
try {
  const mainEntry = join(workdir, 'main.ts')
  const uiEntry = join(workdir, 'ui.ts')
  writeFileSync(
    mainEntry,
    `import { runEncryptionConformance } from ${JSON.stringify(conformanceModule)};
runEncryptionConformance().then((result) => {
  console.log('OREZ_ENCRYPTION_CONFORMANCE_PASS runtime=gpui-react ' + JSON.stringify(result));
  setTimeout(() => process.exit(0), 250);
}).catch((error) => {
  console.error('OREZ_ENCRYPTION_CONFORMANCE_FAIL runtime=gpui-react ' + String(error));
  process.exit(1);
});
`
  )
  writeFileSync(
    uiEntry,
    `import { runEncryptionConformance } from ${JSON.stringify(conformanceModule)};
runEncryptionConformance().then((result) => {
  console.log('OREZ_ENCRYPTION_CONFORMANCE_PASS runtime=gpui-ui ' + JSON.stringify(result));
}).catch((error) => {
  console.error('OREZ_ENCRYPTION_CONFORMANCE_FAIL runtime=gpui-ui ' + String(error));
});
`
  )
  const runner = resolve(rngpuiRoot, 'ts/scripts/run-hermes-example.mjs')
  const result = spawnSync(
    process.execPath,
    [runner, mainEntry, '--ui-entry', uiEntry, '--timeout-ms', '8000'],
    {
      cwd: resolve(rngpuiRoot, 'ts'),
      encoding: 'utf8',
      env: { ...process.env, RNGPUI_SERVICE: service },
      timeout: 60_000,
    }
  )
  const output = `${result.stdout ?? ''}${result.stderr ?? ''}`
  process.stdout.write(output)
  if (result.error) throw result.error
  if (result.status !== 0) {
    const outcome = result.signal
      ? `terminated by ${result.signal}`
      : `exited with ${result.status ?? 'an unknown status'}`
    throw new Error(`GPUI encryption conformance ${outcome}`)
  }
  for (const runtime of ['gpui-react', 'gpui-ui']) {
    if (!output.includes(`OREZ_ENCRYPTION_CONFORMANCE_PASS runtime=${runtime}`)) {
      throw new Error(`missing ${runtime} encryption conformance result`)
    }
  }
} finally {
  rmSync(workdir, { recursive: true, force: true })
}

function buildGPUIService(root) {
  const rustRoot = resolve(root, 'rust')
  const output = resolve(rustRoot, 'target/release/rngpui-service')
  rmSync(output, { force: true })
  if (existsSync(output))
    throw new Error(`failed to remove stale GPUI service: ${output}`)
  const build = spawnSync('cargo', ['build', '--release', '--bin', 'rngpui-service'], {
    cwd: rustRoot,
    env: {
      ...process.env,
      PATH: `/opt/homebrew/opt/zig@0.15/bin:${process.env.PATH ?? ''}`,
    },
    stdio: 'inherit',
    timeout: 600_000,
  })
  if (build.error) throw build.error
  if (build.status !== 0) {
    throw new Error(`failed to build GPUI service from ${root}`)
  }
  if (!existsSync(output) || !statSync(output).isFile()) {
    throw new Error(`cargo did not produce a GPUI service: ${output}`)
  }
  console.log('OREZ_ENCRYPTION_GPUI_NATIVE_BUILD_FRESH')
  return output
}
