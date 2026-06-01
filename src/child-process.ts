import { spawnSync, type ChildProcess } from 'node:child_process'

export function isPidRunning(pid: number | null | undefined): pid is number {
  if (!Number.isInteger(pid) || (pid as number) <= 0) return false

  try {
    process.kill(pid as number, 0)
    return true
  } catch (err: any) {
    if (err?.code === 'ESRCH') return false
    if (err?.code === 'EPERM') return true
    throw err
  }
}

export function isChildProcessRunning(
  child: ChildProcess | null | undefined
): child is ChildProcess {
  if (!child) return false
  return child.exitCode === null && child.signalCode === null
}

export async function waitForChildProcessExit(
  child: ChildProcess,
  timeoutMs: number
): Promise<boolean> {
  if (!isChildProcessRunning(child)) return true

  return await new Promise<boolean>((resolve) => {
    let settled = false

    const finish = (exited: boolean) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      child.off('exit', onExit)
      child.off('close', onClose)
      resolve(exited || !isChildProcessRunning(child))
    }

    const onExit = () => finish(true)
    const onClose = () => finish(true)

    const timer = setTimeout(() => finish(false), timeoutMs)

    child.once('exit', onExit)
    child.once('close', onClose)
  })
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

function listChildPids(pid: number): number[] {
  if (process.platform === 'win32') return []

  const result = spawnSync('pgrep', ['-P', String(pid)], {
    encoding: 'utf8',
    env: process.env,
  })

  if (result.status !== 0 || !result.stdout) return []

  return result.stdout
    .split(/\s+/)
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0)
}

export function listProcessTreePids(pid: number): number[] {
  if (!isPidRunning(pid)) return []

  const seen = new Set<number>()
  const stack = [pid]
  const order: number[] = []

  while (stack.length > 0) {
    const current = stack.pop()!
    if (seen.has(current)) continue
    seen.add(current)
    order.push(current)

    for (const childPid of listChildPids(current)) {
      if (!seen.has(childPid)) stack.push(childPid)
    }
  }

  return order
}

export function killProcessTree(pid: number, signal: NodeJS.Signals | number): void {
  const order = listProcessTreePids(pid)

  for (const current of order.reverse()) {
    try {
      process.kill(current, signal)
    } catch (err: any) {
      if (err?.code !== 'ESRCH') throw err
    }
  }
}

export async function waitForPidsExit(
  pids: Iterable<number>,
  timeoutMs: number
): Promise<boolean> {
  const uniquePids = [
    ...new Set([...pids].filter((pid) => Number.isInteger(pid) && pid > 0)),
  ]
  const deadline = Date.now() + timeoutMs

  while (Date.now() < deadline) {
    if (uniquePids.every((pid) => !isPidRunning(pid))) return true
    await sleep(25)
  }

  return uniquePids.every((pid) => !isPidRunning(pid))
}

export interface TerminateProcessTreeOptions {
  gracefulSignal?: NodeJS.Signals
  forceSignal?: NodeJS.Signals
  graceMs?: number
  forceGraceMs?: number
}

export async function terminateProcessTree(
  pid: number,
  options: TerminateProcessTreeOptions = {}
): Promise<boolean> {
  const {
    gracefulSignal = 'SIGTERM',
    forceSignal = 'SIGKILL',
    graceMs = 5000,
    forceGraceMs = 1000,
  } = options
  const knownPids = new Set(listProcessTreePids(pid))

  if (knownPids.size === 0) return true

  try {
    process.kill(pid, gracefulSignal)
  } catch (err: any) {
    if (err?.code !== 'ESRCH') throw err
  }

  const deadline = Date.now() + graceMs
  while (Date.now() < deadline) {
    for (const knownPid of [...knownPids]) {
      for (const childPid of listChildPids(knownPid)) {
        knownPids.add(childPid)
      }
    }

    if ([...knownPids].every((knownPid) => !isPidRunning(knownPid))) {
      return true
    }

    await sleep(50)
  }

  const remaining = [...knownPids].filter((knownPid) => isPidRunning(knownPid))
  for (const remainingPid of remaining.reverse()) {
    try {
      process.kill(remainingPid, forceSignal)
    } catch (err: any) {
      if (err?.code !== 'ESRCH') throw err
    }
  }

  return waitForPidsExit(remaining, forceGraceMs)
}

export async function terminateChildProcessTree(
  child: ChildProcess,
  options: TerminateProcessTreeOptions = {}
): Promise<boolean> {
  if (!isChildProcessRunning(child)) return true
  if (child.pid) return terminateProcessTree(child.pid, options)

  try {
    child.kill(options.gracefulSignal ?? 'SIGTERM')
  } catch (err: any) {
    if (err?.code !== 'ESRCH') throw err
  }

  const exited = await waitForChildProcessExit(child, options.graceMs ?? 5000)
  if (exited) return true

  child.kill(options.forceSignal ?? 'SIGKILL')
  return waitForChildProcessExit(child, options.forceGraceMs ?? 1000)
}
