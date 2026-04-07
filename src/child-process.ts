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

export function killProcessTree(pid: number, signal: NodeJS.Signals | number): void {
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

  for (const current of order.reverse()) {
    try {
      process.kill(current, signal)
    } catch (err: any) {
      if (err?.code !== 'ESRCH') throw err
    }
  }
}
