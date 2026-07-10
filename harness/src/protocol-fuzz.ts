// Deterministic malformed-protocol lane for the Rust native and CF hosts.
// It checks bounded responses and post-corpus health; it never sends fixture
// row contents or logs request bodies.
import { parseArgs } from 'node:util'

import { startRustCf } from './targets/rust-cf.js'
import { startRustLocal } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'rust-local' },
    cases: { type: 'string', default: '10000' },
    seed: { type: 'string', default: '1' },
    concurrency: { type: 'string', default: '20' },
  },
})

const total = Number(args.cases)
const seed = Number(args.seed)
const concurrency = Number(args.concurrency)
if (!Number.isSafeInteger(total) || total < 1) throw new Error('cases must be positive')
if (!Number.isSafeInteger(seed)) throw new Error('seed must be an integer')
if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 100) {
  throw new Error('concurrency must be 1..100')
}

const target =
  args.target === 'rust-cf'
    ? await startRustCf({ queryAware: false, pullIntervalMs: 0 })
    : args.target === 'rust-local'
      ? await startRustLocal({ queryAware: false, pullIntervalMs: 0 })
      : (() => {
          throw new Error('target must be rust-local or rust-cf')
        })()

const origin =
  'origin' in target
    ? target.origin
    : `${target.baseUrl}/${target.namespace}`

function mulberry32(initial: number) {
  let value = initial
  return () => {
    value |= 0
    value = (value + 0x6d2b79f5) | 0
    let mixed = Math.imul(value ^ (value >>> 15), 1 | value)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return (mixed ^ (mixed >>> 14)) >>> 0
  }
}

const random = mulberry32(seed)
const malformed = [
  '',
  '{',
  '[',
  'null',
  '[]',
  '{}',
  '{"clientID":1}',
  '{"clientID":"x","clientGroupID":null,"cookie":null}',
  '{"clientID":"x","clientGroupID":"g","cookie":-1}',
  '{"clientID":"x","clientGroupID":"g","cookie":"not-a-counter"}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":null}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":[{}]}',
  '{"pushVersion":1,"clientGroupID":"g","mutations":[{"type":"custom","clientID":"c","id":0,"name":1,"args":[]}]}',
] as const

let next = 0
let completed = 0
const statuses = new Map<number, number>()
const started = performance.now()

async function worker() {
  while (true) {
    const index = next++
    if (index >= total) return
    const route = random() % 2 === 0 ? 'pull' : 'push'
    const body = malformed[random() % malformed.length]!
    const requestStarted = performance.now()
    const response = await fetch(`${origin}/${route}`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-fuzz-user',
        'content-type': 'application/json',
      },
      body,
      signal: AbortSignal.timeout(2_000),
    })
    await response.arrayBuffer()
    const elapsed = performance.now() - requestStarted
    if (elapsed > 2_000) throw new Error(`case ${index} exceeded 2s (${elapsed}ms)`)
    if (response.status >= 500) {
      throw new Error(`case ${index} produced server error ${response.status}`)
    }
    statuses.set(response.status, (statuses.get(response.status) ?? 0) + 1)
    completed++
  }
}

try {
  await Promise.all(Array.from({ length: concurrency }, () => worker()))

  const validPull = await fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-fuzz-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'fuzz-valid',
      clientGroupID: 'fuzz-valid-group',
      cookie: null,
    }),
  })
  await validPull.arrayBuffer()
  if (validPull.status !== 200) {
    throw new Error(`valid post-fuzz pull failed ${validPull.status}`)
  }

  console.log(
    JSON.stringify({
      lane: 'protocol-fuzz',
      result: 'PASS',
      target: args.target,
      seed,
      cases: completed,
      concurrency,
      elapsedMs: Math.round(performance.now() - started),
      statuses: Object.fromEntries([...statuses].sort(([left], [right]) => left - right)),
    }),
  )
} finally {
  await target.close()
}
