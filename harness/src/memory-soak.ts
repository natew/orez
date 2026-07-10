// Cloudflare wasm linear-memory soak. The authenticated status endpoint emits
// only the current byte length; no heap contents or request payloads are read.
import { parseArgs } from 'node:util'

import { startRustCf } from './targets/rust-cf.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-cf' },
    blocks: { type: 'string', default: '3' },
    ops: { type: 'string', default: '1000' },
  },
})

if (args.target !== 'rust-cf') {
  throw new Error('wasm linear-memory soak target must be rust-cf')
}
const blocks = Number(args.blocks)
const ops = Number(args.ops)
if (!Number.isSafeInteger(blocks) || blocks < 3) throw new Error('blocks must be >= 3')
if (!Number.isSafeInteger(ops) || ops < 1) throw new Error('ops must be positive')

const target = await startRustCf({ queryAware: true, pullIntervalMs: 0 })
const origin = target.origin
const clientID = `memory-${crypto.randomUUID()}`
const clientGroupID = `memory-group-${crypto.randomUUID()}`
let cookie: unknown = null
let queryVersion = 0

async function queryPatch(patch: Array<Record<string, unknown>>) {
  queryVersion++
  const response = await fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-memory-soak',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID,
      clientGroupID,
      cookie,
      queries: { version: queryVersion, patch },
    }),
    signal: AbortSignal.timeout(5_000),
  })
  const body = (await response.json()) as { cookie?: unknown; error?: string }
  if (!response.ok)
    throw new Error(`query churn failed ${response.status}: ${body.error}`)
  cookie = body.cookie ?? cookie
}

async function churnWake(index: number) {
  const url = `${origin.replace('https:', 'wss:').replace('http:', 'ws:')}/wake?clientID=memory-${index}`
  const socket = new WebSocket(url)
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('wake churn open timed out')), 5_000)
    socket.addEventListener('open', () => {
      clearTimeout(timer)
      socket.close()
      resolve()
    })
    socket.addEventListener('error', () => {
      clearTimeout(timer)
      reject(new Error('wake churn socket failed'))
    })
  })
}

try {
  // The measured instance must stay alive for the whole run: a restart
  // re-instantiates the wasm module and resets linear memory, which turns
  // every sample into a boot-footprint reading and hides any leak. Restart
  // and eviction churn are covered by the eviction lane, never here.
  async function runBlock(label: string, block: number) {
    for (let batchStart = 0; batchStart < ops; batchStart += 100) {
      const patch: Array<Record<string, unknown>> = []
      const batchEnd = Math.min(batchStart + 100, ops)
      for (let index = batchStart; index < batchEnd; index++) {
        const hash = `memory-${label}-${block}-${index}`
        patch.push({ op: 'put', hash, name: 'tasksDone', args: [] })
        patch.push({ op: 'del', hash })
      }
      await queryPatch(patch)
      await churnWake(block * ops + batchStart)
    }
  }

  // Warm the compiler and allocator with one full representative block. The
  // budget applies to the three subsequent blocks, not cold-start ramp-up.
  await runBlock('warm', 0)
  const samples = [(await target.hibernationStatus()).wasmMemoryBytes]

  for (let block = 0; block < blocks; block++) {
    await runBlock('measure', block)
    samples.push((await target.hibernationStatus()).wasmMemoryBytes)
  }

  const growth = samples.slice(1).map((value, index) => value - samples[index]!)
  if (growth.some((bytes) => bytes > 65_536)) {
    throw new Error(
      `wasm memory grew by more than one page in a block: ${growth.join(',')}`
    )
  }
  if (growth.length >= 3 && growth.slice(-3).every((bytes) => bytes > 0)) {
    throw new Error(
      `wasm memory increased across three consecutive blocks: ${growth.join(',')}`
    )
  }

  console.log(
    JSON.stringify({
      lane: 'wasm-memory-soak',
      result: 'PASS',
      blocks,
      opsPerBlock: ops,
      samples,
      growth,
      pageBudgetBytes: 65_536,
    })
  )
} finally {
  await target.close()
}
