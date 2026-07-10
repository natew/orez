import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-cf' },
    blocks: { type: 'string', default: '3' },
    ops: { type: 'string', default: '3000' },
    writers: { type: 'string', default: '12' },
    worker: {
      type: 'string',
      default:
        process.env.ZHARNESS_RUST_CF_WORKER ?? 'https://orez-rust-sync.lslcf.workers.dev',
    },
    'admin-key': {
      type: 'string',
      default: process.env.ZHARNESS_CF_ADMIN_KEY,
    },
  },
})

if (args.target !== 'rust-cf') throw new Error('push memory soak target must be rust-cf')
const blocks = Number(args.blocks)
const ops = Number(args.ops)
const writers = Number(args.writers)
if (!Number.isSafeInteger(blocks) || blocks < 3) throw new Error('blocks must be >= 3')
if (!Number.isSafeInteger(ops) || ops < 1) throw new Error('ops must be positive')
if (!Number.isSafeInteger(writers) || writers < 1)
  throw new Error('writers must be positive')

type Status = {
  bootID: string
  databaseSizeBytes: number
  wasmMemoryBytes: number
  heapUsedBytes: number | null
  heapTotalBytes: number | null
  heapLimitBytes: number | null
  counters: Record<string, number>
}

const namespace = `push-memory-${crypto.randomUUID()}`
const origin = `${args.worker!.replace(/\/$/, '')}/${namespace}`
const adminKey =
  args['admin-key'] ??
  readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
const runID = crypto.randomUUID()
const mutationIDs = Array.from({ length: writers }, () => 0)
let operation = 0

async function status(): Promise<Status> {
  const response = await fetch(`${origin}/admin/status`, {
    headers: { 'x-admin-key': adminKey },
    signal: AbortSignal.timeout(5_000),
  })
  if (!response.ok)
    throw new Error(`status failed ${response.status}: ${await response.text()}`)
  return response.json() as Promise<Status>
}

async function push(writer: number, label: string, block: number, index: number) {
  const mutationID = ++mutationIDs[writer]!
  const id = `${runID}-${label}-${block}-${index}`
  const operationID = operation++
  const response = await fetch(`${origin}/push`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-u0',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientGroupID: `push-memory-group-${runID}`,
      pushVersion: 1,
      mutations: [
        {
          type: 'custom',
          clientID: `push-memory-client-${runID}-${writer}`,
          id: mutationID,
          name: 'message.send',
          args: [
            {
              id,
              serverId: 'p0',
              channelId: `channel-${operationID % 12}`,
              creatorId: 'u0',
              content: `message ${operationID} keeps chat-shaped text, emoji 🚀, and metadata moving through the push boundary`,
              type: 'person',
              createdAt: 1_783_684_000_000 + operationID,
              order: String(operationID).padStart(12, '0'),
              meta: { attachments: [], reactions: {}, edited: false },
            },
          ],
        },
      ],
    }),
    signal: AbortSignal.timeout(10_000),
  })
  const body = (await response.json()) as {
    pushResponse?: {
      mutations?: Array<{ result?: { error?: string; message?: string } }>
    }
    error?: string
  }
  const result = body.pushResponse?.mutations?.[0]?.result
  if (!response.ok || result?.error) {
    throw new Error(
      `push failed ${response.status}: ${result?.message ?? result?.error ?? body.error ?? 'unknown error'}`
    )
  }
}

async function runBlock(label: string, block: number) {
  for (let batchStart = 0; batchStart < ops; batchStart += writers) {
    const batchEnd = Math.min(batchStart + writers, ops)
    await Promise.all(
      Array.from({ length: batchEnd - batchStart }, (_, offset) =>
        push(offset, label, block, batchStart + offset)
      )
    )
  }
}

function growth(samples: number[]) {
  return samples.slice(1).map((value, index) => value - samples[index]!)
}

function assertGrowth(label: string, values: number[]) {
  const deltas = growth(values)
  if (deltas.some((bytes) => bytes > 65_536)) {
    throw new Error(
      `${label} grew by more than 65536 bytes in a block: ${deltas.join(',')}`
    )
  }
  if (deltas.slice(-3).every((bytes) => bytes > 0)) {
    throw new Error(
      `${label} increased across three consecutive blocks: ${deltas.join(',')}`
    )
  }
  return deltas
}

await runBlock('warm', 0)
const samples = [await status()]
const bootID = samples[0]!.bootID
const applicationErrors = samples[0]!.counters.applicationErrors ?? 0

for (let block = 0; block < blocks; block++) {
  await runBlock('measure', block)
  const sample = await status()
  if (sample.bootID !== bootID)
    throw new Error('durable object restarted during push soak')
  samples.push(sample)
}

const wasmSamples = samples.map((sample) => sample.wasmMemoryBytes)
const wasmGrowth = assertGrowth('wasm memory', wasmSamples)
const heapSamples = samples.map((sample) => sample.heapUsedBytes)
const measuredHeap = heapSamples.every((value): value is number => value !== null)
  ? heapSamples
  : null
const heapGrowth = measuredHeap ? assertGrowth('js heap', measuredHeap) : null
const final = samples.at(-1)!
if ((final.counters.applicationErrors ?? 0) !== applicationErrors) {
  throw new Error(
    `application errors increased during push soak: ${applicationErrors} -> ${final.counters.applicationErrors}`
  )
}

console.log(
  JSON.stringify({
    lane: 'push-memory-soak',
    result: 'PASS',
    blocks,
    opsPerBlock: ops,
    writers,
    totalPushes: ops * (blocks + 1),
    wasmSamples,
    wasmGrowth,
    heapSamples,
    heapGrowth,
    heapTotalBytes: final.heapTotalBytes,
    heapLimitBytes: final.heapLimitBytes,
    databaseSizeBytes: final.databaseSizeBytes,
    pageBudgetBytes: 65_536,
  })
)
