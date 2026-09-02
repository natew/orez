import { afterEach, describe, expect, test } from 'bun:test'

import { createEnv, expected } from './index.js'

const original = {
  allowMissing: process.env.ALLOW_MISSING_ENV,
  mode: process.env.TAKEOUT_ENV_MODE,
  offset: process.env.PORT_OFFSET,
  required: process.env.OREZ_ENV_TEST_REQUIRED,
}

afterEach(() => {
  for (const [key, value] of Object.entries({
    ALLOW_MISSING_ENV: original.allowMissing,
    TAKEOUT_ENV_MODE: original.mode,
    PORT_OFFSET: original.offset,
    OREZ_ENV_TEST_REQUIRED: original.required,
  })) {
    if (value === undefined) delete process.env[key]
    else process.env[key] = value
  }
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
})
