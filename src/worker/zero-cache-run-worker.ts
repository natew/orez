// @ts-expect-error — internal zero-cache module, no type declarations
import { runWorker as runZeroWorker } from '@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js'
// @ts-expect-error — added by the CF overlay, no type declarations
import * as zeroProcesses from '@rocicorp/zero/out/zero-cache/src/types/processes.js'

const abandonOrezZeroWorkers = zeroProcesses.abandonOrezZeroWorkers as (
  taskId: string
) => void
const waitForOrezZeroWorkersStopped = zeroProcesses.waitForOrezZeroWorkersStopped as (
  taskId: string
) => Promise<void>

export function abandonWorkerTree(taskId: string): void {
  abandonOrezZeroWorkers(taskId)
}

export async function runWorker(
  parent: unknown,
  env: Record<string, string>
): Promise<void> {
  try {
    await runZeroWorker(parent, env)
  } finally {
    await waitForOrezZeroWorkersStopped(env.ZERO_TASK_ID)
  }
}
