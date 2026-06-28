import { spawn } from 'node:child_process'
import { resolve } from 'node:path'

import { describe, it, expect } from 'vitest'

function runCli(
  args: string[]
): Promise<{ stdout: string; stderr: string; code: number }> {
  return new Promise((res) => {
    // cli-entry.js is the actual entry (cli.js only exports `main`; runMain
    // is invoked from cli-entry).
    const child = spawn('bun', [resolve('dist/cli-entry.js'), ...args], {
      timeout: 10_000,
      env: { ...process.env, NODE_ENV: 'test' },
    })
    let stdout = ''
    let stderr = ''
    child.stdout?.on('data', (d: Buffer) => (stdout += d.toString()))
    child.stderr?.on('data', (d: Buffer) => (stderr += d.toString()))
    child.on('close', (code) => res({ stdout, stderr, code: code ?? 1 }))
  })
}

describe('cli', () => {
  it('shows help with --help', async () => {
    const { stdout, stderr } = await runCli(['--help'])
    const output = stdout + stderr
    expect(output).toContain('orez')
    expect(output).toContain('--pg-port')
    expect(output).toContain('--zero-port')
    expect(output).toContain('--skip-zero-cache')
    expect(output).toContain('--ephemeral')
  })

  it('s3 subcommand shows help', async () => {
    const { stdout, stderr } = await runCli(['s3', '--help'])
    const output = stdout + stderr
    expect(output).toContain('--port')
    expect(output).toContain('--data-dir')
  })
})
