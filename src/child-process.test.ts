import { spawn, type ChildProcess } from 'node:child_process'

import { afterEach, describe, expect, it } from 'vitest'

import {
  isChildProcessRunning,
  isPidRunning,
  killProcessTree,
  waitForChildProcessExit,
} from './child-process.js'

const spawned = new Set<ChildProcess>()

function track(child: ChildProcess): ChildProcess {
  spawned.add(child)
  child.once('close', () => spawned.delete(child))
  return child
}

function pidExists(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (err: any) {
    if (err?.code === 'ESRCH') return false
    throw err
  }
}

async function waitForPidExit(pid: number, timeoutMs: number): Promise<boolean> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (!pidExists(pid)) return true
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  return !pidExists(pid)
}

afterEach(async () => {
  for (const child of spawned) {
    if (!isChildProcessRunning(child)) continue
    if (child.pid) killProcessTree(child.pid, 'SIGKILL')
    else child.kill('SIGKILL')
    await waitForChildProcessExit(child, 1000)
  }
  spawned.clear()
})

describe('child process helpers', () => {
  it('tracks whether a pid is still alive', async () => {
    const child = track(
      spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {
        stdio: 'ignore',
      })
    )

    expect(isPidRunning(child.pid)).toBe(true)

    child.kill('SIGKILL')
    await expect(waitForChildProcessExit(child, 2000)).resolves.toBe(true)
    expect(isPidRunning(child.pid)).toBe(false)
  })

  it('does not confuse "signal sent" with "process exited"', async () => {
    const child = track(
      spawn(
        process.execPath,
        [
          '-e',
          "process.on('SIGTERM', () => {}); console.log('ready'); setInterval(() => {}, 1000)",
        ],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      )
    )

    await new Promise<void>((resolve, reject) => {
      let output = ''

      const onData = (chunk: Buffer) => {
        output += chunk.toString()
        if (!output.includes('ready')) return
        child.stdout?.off('data', onData)
        resolve()
      }

      child.stdout?.on('data', onData)
      child.once('error', reject)
      child.once('close', () =>
        reject(new Error('child exited before signaling readiness'))
      )
    })

    child.kill('SIGTERM')

    expect(child.killed).toBe(true)
    expect(isChildProcessRunning(child)).toBe(true)
    await expect(waitForChildProcessExit(child, 100)).resolves.toBe(false)

    child.kill('SIGKILL')
    await expect(waitForChildProcessExit(child, 2000)).resolves.toBe(true)
    expect(isChildProcessRunning(child)).toBe(false)
  })

  it('kills a parent process and its descendants', async () => {
    const parent = track(
      spawn(
        process.execPath,
        [
          '-e',
          [
            "const { spawn } = require('node:child_process')",
            "const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' })",
            'console.log(child.pid)',
            'setInterval(() => {}, 1000)',
          ].join('; '),
        ],
        { stdio: ['ignore', 'pipe', 'ignore'] }
      )
    )

    const childPid = await new Promise<number>((resolve, reject) => {
      let output = ''

      const onData = (chunk: Buffer) => {
        output += chunk.toString()
        if (!output.includes('\n')) return

        const value = Number.parseInt(output.split(/\r?\n/, 1)[0]!.trim(), 10)
        if (Number.isInteger(value) && value > 0) {
          parent.stdout?.off('data', onData)
          resolve(value)
        }
      }

      parent.stdout?.on('data', onData)
      parent.once('error', reject)
      parent.once('close', () =>
        reject(new Error('parent exited before reporting child pid'))
      )
    })

    killProcessTree(parent.pid!, 'SIGKILL')

    await expect(waitForChildProcessExit(parent, 2000)).resolves.toBe(true)
    await expect(waitForPidExit(childPid, 2000)).resolves.toBe(true)
  })
})
