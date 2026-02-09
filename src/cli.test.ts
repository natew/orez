import { spawn } from 'node:child_process'
import { resolve } from 'node:path'

import { describe, it, expect } from 'vitest'

function runCli(
  args: string[]
): Promise<{ stdout: string; stderr: string; code: number }> {
  return new Promise((res) => {
    const child = spawn('bun', [resolve('dist/cli.js'), ...args], {
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
    const { stdout } = await runCli(['--help'])
    expect(stdout).toContain('orez')
    expect(stdout).toContain('--pg-port')
    expect(stdout).toContain('--zero-port')
    expect(stdout).toContain('--skip-zero-cache')
  })

  it('s3 subcommand shows help', async () => {
    const { stdout } = await runCli(['s3', '--help'])
    expect(stdout).toContain('--port')
    expect(stdout).toContain('--data-dir')
  })
})
