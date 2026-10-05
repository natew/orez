import { afterEach, describe, expect, test } from 'bun:test'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { createEnv, expected } from './index.js'

const original = {
  allowMissing: process.env.ALLOW_MISSING_ENV,
  mode: process.env.TAKEOUT_ENV_MODE,
  offset: process.env.PORT_OFFSET,
  required: process.env.OREZ_ENV_TEST_REQUIRED,
  secret: process.env.OREZ_ENV_TEST_SECRET,
  rotated: process.env.OREZ_ENV_TEST_ROTATED,
  override: process.env.OREZ_ENV_TEST_OVERRIDE,
  cwd: process.cwd(),
}

afterEach(() => {
  for (const [key, value] of Object.entries({
    ALLOW_MISSING_ENV: original.allowMissing,
    TAKEOUT_ENV_MODE: original.mode,
    PORT_OFFSET: original.offset,
    OREZ_ENV_TEST_REQUIRED: original.required,
    OREZ_ENV_TEST_SECRET: original.secret,
    OREZ_ENV_TEST_ROTATED: original.rotated,
    OREZ_ENV_TEST_OVERRIDE: original.override,
  })) {
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
  process.chdir(original.cwd)
})

describe('@o/env', () => {
  test('resolves offsets, modes, and expected environment values', () => {
    process.env.TAKEOUT_ENV_MODE = 'development'
    process.env.PORT_OFFSET = '7'
    process.env.OREZ_ENV_TEST_REQUIRED = 'present'

    const result = createEnv({
      ports: { web: 3000 },
      base: { OREZ_ENV_TEST_REQUIRED: expected },
      development: ({ ports }) => ({ OREZ_ENV_TEST_MODE: `dev-${ports.web}` }),
      production: { OREZ_ENV_TEST_MODE: 'production' },
    })

    expect(result.ports.web).toBe(3007)
    expect(result.env.OREZ_ENV_TEST_REQUIRED).toBe('present')
    expect(result.env.OREZ_ENV_TEST_MODE).toBe('dev-3007')
  })

  test('a value the managed .env.development echoes back never shadows .env', () => {
    const dir = mkdtempSync(join(tmpdir(), 'orez-env-'))
    writeFileSync(
      join(dir, '.env'),
      'OREZ_ENV_TEST_SECRET=real-secret\nexport OREZ_ENV_TEST_ROTATED="new-key"\nOREZ_ENV_TEST_OVERRIDE=from-file\n'
    )
    writeFileSync(
      join(dir, '.env.development'),
      '# managed by src/env.ts!\nOREZ_ENV_TEST_SECRET=\nOREZ_ENV_TEST_ROTATED=old-key\nOREZ_ENV_TEST_OVERRIDE=managed\nOREZ_ENV_TEST_MODE=dev-3000'
    )
    process.chdir(dir)
    process.env.TAKEOUT_ENV_MODE = 'development'
    delete process.env.PORT_OFFSET
    // what bun's dotenv leaves in process.env: .env.development over .env
    process.env.OREZ_ENV_TEST_SECRET = ''
    process.env.OREZ_ENV_TEST_ROTATED = 'old-key'
    // an explicit parent export that differs from the managed file
    process.env.OREZ_ENV_TEST_OVERRIDE = 'parent'

    const result = createEnv({
      ports: { web: 3000 },
      freshDev: true,
      base: {
        OREZ_ENV_TEST_SECRET: '',
        OREZ_ENV_TEST_ROTATED: '',
        OREZ_ENV_TEST_OVERRIDE: '',
      },
      development: ({ ports }) => ({ OREZ_ENV_TEST_MODE: `dev-${ports.web}` }),
      production: { OREZ_ENV_TEST_MODE: 'production' },
    })

    expect(result.env.OREZ_ENV_TEST_SECRET).toBe('real-secret')
    expect(result.env.OREZ_ENV_TEST_ROTATED).toBe('new-key')
    expect(result.env.OREZ_ENV_TEST_OVERRIDE).toBe('parent')
    expect(result.env.OREZ_ENV_TEST_MODE).toBe('dev-3000')
  })
})
