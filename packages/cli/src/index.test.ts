import { describe, expect, test } from 'bun:test'

import { cmd, run } from './index.js'

describe('@o/cli', () => {
  test('parses typed command arguments', async () => {
    const originalArgv = process.argv
    process.argv = ['bun', 'command.ts', '--count', '7', '--enabled', 'value']

    try {
      let parsed: unknown = null
      await cmd('test command')
        .args('--count number --enabled boolean')
        .run(({ args }) => {
          parsed = args
        })

      expect(parsed).toEqual({ count: 7, enabled: true, rest: ['value'] })
    } finally {
      process.argv = originalArgv
    }
  })

  test('runs a subprocess and captures output', async () => {
    const result = await run('printf orez', { captureOutput: true })
    expect(result).toEqual({ stdout: 'orez', stderr: '', exitCode: 0 })
  })
})
